package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
)

const evidence = "../../internal/exec/sim/testdata/sim.evidence.jsonl"

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "e1.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// e1Server counts every request; the tests require zero.
func e1Server(t *testing.T) (*httptest.Server, *atomic.Int64) {
	var hits atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Write([]byte(`{"status":"accepted","broker_order_id":"1"}`))
	}))
	t.Cleanup(s.Close)
	return s, &hits
}

func execmock(args ...string) (code int, stdout, stderr string) {
	var out, errb bytes.Buffer
	code = run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestDryRunPrintsRequests(t *testing.T) {
	code, out, errs := execmock("-config", "testdata/dry-run.toml", "-intent", "testdata/intent.json", "-in", evidence, "-dry-run")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errs)
	}
	want, err := os.ReadFile("testdata/dry-run.golden.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if out != string(want) {
		t.Fatalf("dry run output differs from testdata/dry-run.golden.jsonl:\n%s", out)
	}
	// The dry run goes through the E1 client and lands where execsim's fake
	// gateway lands for the same intent and rule.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var last struct{ Result json.RawMessage }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("../execsim/testdata/sim.golden.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if first := strings.SplitN(string(golden), "\n", 2)[0]; string(last.Result) != first {
		t.Fatalf("result %s\nexecsim %s", last.Result, first)
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	srv, hits := e1Server(t)
	cfg := writeConfig(t, "[e1]\naccount = \"kis_mock\"\nsend_enabled = true\nurl = \""+srv.URL+"\"\n")
	code, out, errs := execmock("-config", cfg, "-intent", "testdata/intent.json", "-in", evidence, "-dry-run")
	if code != 0 || hits.Load() != 0 || strings.Count(out, `"dry_run":true`) != 5 {
		t.Fatalf("exit %d hits %d stderr %s\n%s", code, hits.Load(), errs, out)
	}
}

func TestConfigThatWouldSendSendsNothing(t *testing.T) {
	srv, hits := e1Server(t)
	cases := []struct {
		name, toml string
		code       int
	}{
		{"empty config", "", 2},
		{"empty e1 table", "[e1]\n", 2},
		{"send_enabled without account", "[e1]\nsend_enabled = true\nurl = \"" + srv.URL + "\"\n", 2},
		{"account without send_enabled", "[e1]\naccount = \"kis_mock\"\nurl = \"" + srv.URL + "\"\n", 0},
		{"kis_live", "[e1]\naccount = \"kis_live\"\nsend_enabled = true\nurl = \"" + srv.URL + "\"\n", 2},
		{"unknown account", "[e1]\naccount = \"paper\"\nsend_enabled = true\nurl = \"" + srv.URL + "\"\n", 2},
		{"both set: not wired in this build", "[e1]\naccount = \"kis_mock\"\nsend_enabled = true\nurl = \"" + srv.URL + "\"\n", 2},
		{"unknown key", "[e1]\naccount = \"kis_mock\"\nlive = true\n", 2},
	}
	for _, c := range cases {
		code, out, errs := execmock("-config", writeConfig(t, c.toml), "-intent", "testdata/intent.json", "-in", evidence)
		if code != c.code || out != "" {
			t.Errorf("%s: exit %d stdout %q stderr %q, want exit %d", c.name, code, out, errs, c.code)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("%d requests sent", hits.Load())
	}
}

func TestRecordedDryRunBodiesStayWithinIntentBounds(t *testing.T) {
	data, err := os.ReadFile("testdata/intent.json")
	if err != nil {
		t.Fatal(err)
	}
	in, err := exec.ParseIntent(data)
	if err != nil {
		t.Fatal(err)
	}
	code, out, errs := execmock("-config", "testdata/dry-run.toml", "-intent", "testdata/intent.json", "-in", evidence, "-dry-run")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errs)
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var envelope struct {
			DryRun bool       `json:"dry_run"`
			Body   exec.Child `json:"body"`
		}
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			t.Fatal(err)
		}
		if !envelope.DryRun {
			continue
		}
		count++
		c := envelope.Body
		if c.IntentID != in.IntentID {
			t.Fatalf("wrong intent: %+v", c)
		}
		if c.Action == exec.ActCancel {
			continue
		}
		if c.Qty < 1 || c.Qty > in.TargetQty() || c.Price < 1 || c.Price > in.Cap() || c.Qty*c.Price > in.MaxNotionalKRW {
			t.Fatalf("outside intent bounds: %+v", c)
		}
	}
	if count == 0 {
		t.Fatal("no bodies checked")
	}
}
