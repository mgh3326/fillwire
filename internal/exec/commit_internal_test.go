package exec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCommitRefusesBeyondE1Caps: decide sizes every send to E1's room, so
// only a bug reaches this backstop. A first order whose outcome stayed
// unknown holds all ten shares in E1's count; a second submit of even one
// share is refused before it is journaled or sent.
func TestCommitRefusesBeyondE1Caps(t *testing.T) {
	data, err := os.ReadFile("testdata/e1_intent.json")
	if err != nil {
		t.Fatal(err)
	}
	in, err := ParseIntent(data)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "exec.journal")
	j, st, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	cfg := DefaultConfig()
	cfg.SendEnabled = true
	e := New(cfg, j, st, nil, &KillSwitch{})
	key := func(n string) string { return in.IntentID + ":" + n }
	c1 := Child{Action: ActSubmit, IntentID: in.IntentID, Key: key("1"), Qty: 10, Price: 70_000}
	for _, r := range []Record{
		{Kind: RecStart, Intent: &in},
		{Kind: RecSend, Child: &c1},
		{Kind: RecOutcome, Outcome: &Outcome{Key: c1.Key, Status: StatusUncertain}},
		{Kind: RecReconcile, View: &View{}},
	} {
		if err := e.commit(r); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c2 := Child{Action: ActSubmit, IntentID: in.IntentID, Key: key("2"), Qty: 1, Price: 70_000}
	if err := e.commit(Record{Kind: RecSend, Child: &c2}); err == nil || !strings.Contains(err.Error(), "quantity_cap_exceeded") {
		t.Fatalf("second submit: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) || len(e.state.Sent) != 1 {
		t.Fatalf("refused submit was journaled: %d children sent", len(e.state.Sent))
	}
	// Replay is not checked: Apply takes the same record, so a journal
	// written before the mirror still opens.
	if _, err := Apply(e.state, Record{Kind: RecSend, Child: &c2}); err != nil {
		t.Fatalf("replay of the same record: %v", err)
	}
}
