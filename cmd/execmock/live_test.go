package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
	"github.com/redis/go-redis/v9"
)

func liveIntent(t *testing.T) exec.Intent {
	t.Helper()
	data, err := os.ReadFile("testdata/intent.json")
	if err != nil {
		t.Fatal(err)
	}
	in, err := exec.ParseIntent(data)
	if err != nil {
		t.Fatal(err)
	}
	return in
}
func liveExecutor(t *testing.T, in exec.Intent, g exec.Gateway, require bool) (*exec.Executor, *exec.KillSwitch) {
	t.Helper()
	j, st, err := exec.OpenJournal(filepath.Join(t.TempDir(), "exec.journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	cfg := exec.DefaultConfig()
	cfg.SendEnabled = true
	cfg.RequireLedgerComplete = require
	k := &exec.KillSwitch{}
	e := exec.New(cfg, j, st, g, k)
	if err := e.Start(in, in.ExpiresAt.Add(-35*time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	return e, k
}

type numericGateway struct{ exec.Gateway }

func (g numericGateway) Send(c exec.Child) (exec.Outcome, error) {
	o, err := g.Gateway.Send(c)
	o.BrokerOrderID = "000123"
	return o, err
}

func TestLiveCompletionRequiresTerminalMatchingBrokerEvidence(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonterminal", true: "terminal"}[terminal], func(t *testing.T) {
			in := liveIntent(t)
			start := in.ExpiresAt.Add(-35 * time.Second)
			g := exectest.New(in)
			e, k := liveExecutor(t, in, numericGateway{g}, true)
			e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
			if err := e.Tick(start.UnixNano()); err != nil {
				t.Fatal(err)
			}
			k.Engage()
			if err := e.Tick(start.Add(time.Second).UnixNano()); err != nil {
				t.Fatal(err)
			}
			if e.State().Done != "" || e.State().Order != nil {
				t.Fatal("cancel acceptance incorrectly finished intent")
			}
			read := func(context.Context) (e1.FillsSnapshot, error) {
				return e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{{BrokerOrderID: "123", Fills: []e1.LedgerFill{}}}}, nil
			}
			if err := refreshLedger(context.Background(), e, read, func(context.Context, exec.BrokerOrder) (brokerView, error) {
				return brokerView{Terminal: terminal}, nil
			}); err != nil {
				t.Fatal(err)
			}
			err := e.Tick(start.Add(2 * time.Second).UnixNano())
			if terminal {
				if err != nil || e.State().Done != exec.DoneKilled {
					t.Fatalf("state %+v %v", e.State(), err)
				}
			} else if !errors.Is(err, exec.ErrIncomplete) || e.State().Done != "" {
				t.Fatalf("clean finish with incomplete evidence: %+v %v", e.State(), err)
			}
		})
	}
}

func TestLedgerSnapshotErrorsBothDirections(t *testing.T) {
	for _, kind := range []string{"unknown_order", "missing_order", "broker_has_unknown_fill", "superseded_fill", "changed_fill"} {
		t.Run(kind, func(t *testing.T) {
			in := liveIntent(t)
			start := in.ExpiresAt.Add(-35 * time.Second)
			e, _ := liveExecutor(t, in, numericGateway{exectest.New(in)}, true)
			e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
			if err := e.Tick(start.UnixNano()); err != nil {
				t.Fatal(err)
			}
			f := e1.LedgerFill{FillID: "42", Side: in.Side, Qty: 1, Price: "70000.00000000", FilledAt: start.Add(time.Second), CommittedAt: start.Add(2 * time.Second)}
			order := e1.LedgerOrder{BrokerOrderID: "123", CumulativeQty: 1, Fills: []e1.LedgerFill{f}}
			snapshot := e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{order}}
			brokerQty := int64(1)
			if kind == "superseded_fill" || kind == "changed_fill" {
				fill, _ := f.Execution()
				if err := e.OnLedgerFill("123", fill); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "unknown_order":
				snapshot.Orders[0].BrokerOrderID = "999"
			case "missing_order":
				snapshot.Orders = nil
			case "broker_has_unknown_fill":
				brokerQty = 2
			case "superseded_fill":
				snapshot.Orders[0].Fills[0].FillID = "43"
			case "changed_fill":
				snapshot.Orders[0].Fills[0].Price = "70100"
			}
			err := refreshLedger(context.Background(), e, func(context.Context) (e1.FillsSnapshot, error) { return snapshot, nil }, func(context.Context, exec.BrokerOrder) (brokerView, error) {
				return brokerView{Filled: brokerQty, Terminal: true}, nil
			})
			if err == nil {
				t.Fatal("accepted inconsistent ledger/broker evidence")
			}
			if e.State().Done != "" {
				t.Fatal("finished despite inconsistent evidence")
			}
		})
	}
}

func TestLiveBookDryRunRecordedStreamBodiesWithinBounds(t *testing.T) {
	file, err := os.Open("../../internal/bookfeed/testdata/stream.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var rows []struct {
		ID     string         `json:"id"`
		Values map[string]any `json:"values"`
	}
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		var row struct {
			ID     string         `json:"id"`
			Values map[string]any `json:"values"`
		}
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	in := liveIntent(t)
	g := exectest.New(in)
	var out bytes.Buffer
	server, hits := e1Server(t)
	c, err := e1.New(e1.Config{Account: e1.MockAccount, DryRun: true, URL: server.URL + "/execution-intents/children"}, in, e1.Options{Log: &out, Answer: fakeAnswer(g), Token: func() (string, error) { t.Fatal("dry run read a token"); return "", nil }})
	if err != nil {
		t.Fatal(err)
	}
	e, k := liveExecutor(t, in, c, false)
	i := -1
	ticks := make(chan time.Time, len(rows))
	for range rows {
		ticks <- time.Time{}
	}
	current := in.ExpiresAt.Add(-35 * time.Second)
	input := liveInputs{Ticks: ticks, Now: func() time.Time { return current }, Book: func(ctx context.Context) (bookfeed.Snapshot, bool, error) {
		i++
		if i >= len(rows) {
			t.Fatal("fixture exhausted")
		}
		snapshot, err := bookfeed.Decode(rows[i].Values)
		if err != nil {
			t.Fatal(err)
		}
		current = snapshot.Received
		if err := r.XAdd(ctx, &redis.XAddArgs{Stream: "book:kis:" + in.Symbol, ID: rows[i].ID, Values: rows[i].Values}).Err(); err != nil {
			t.Fatal(err)
		}
		return bookfeed.Latest(ctx, r, in.Symbol)
	}}
	if err := driveLive(context.Background(), e, k, true, in.ExpiresAt, input); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var item struct {
			Body exec.Child `json:"body"`
		}
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			t.Fatal(err)
		}
		c := item.Body
		count++
		if c.IntentID != in.IntentID {
			t.Fatal("wrong intent")
		}
		if c.Action == exec.ActCancel {
			continue
		}
		if c.Qty <= 0 || c.Qty > in.TargetQty() || c.Price <= 0 || c.Price > in.Cap() || c.Qty*c.Price > in.MaxNotionalKRW {
			t.Fatalf("outside bounds: %+v", c)
		}
	}
	if count != 5 || e.State().Done != exec.DoneCap || len(g.Violations) > 0 || hits.Load() != 0 {
		t.Fatalf("bodies %d state %+v violations %v", count, e.State(), g.Violations)
	}
}

func TestSendRequiresMockConfigAndExplicitFlag(t *testing.T) {
	for _, tc := range []struct {
		name, account      string
		enabled, flag, dry bool
		want               int
		called             bool
	}{
		{"default_off", "kis_mock", false, false, false, 0, false}, {"flag_without_config", "kis_mock", false, true, false, 2, false}, {"config_without_flag", "kis_mock", true, false, false, 2, false}, {"live_account", "kis_live", true, true, false, 2, false}, {"mock_explicit", "kis_mock", true, true, false, 0, true}, {"dry_run_wins", "kis_mock", true, true, true, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "[e1]\naccount = " + strconvQuote(tc.account) + "\nsend_enabled = " + map[bool]string{true: "true", false: "false"}[tc.enabled] + "\n"
			args := []string{"-config", writeConfig(t, body), "-intent", "testdata/intent.json", "-live-book"}
			if tc.flag {
				args = append(args, "-send")
			}
			if tc.dry {
				args = append(args, "-dry-run")
			}
			called := false
			code := runWithLive(args, io.Discard, io.Discard, func(_ string, cfg e1.Config, _ exec.Config, _, _ string, _ time.Duration, _ io.Writer) error {
				called = true
				if cfg.Account != e1.MockAccount || cfg.DryRun != tc.dry {
					t.Fatal("wrong live config")
				}
				return nil
			})
			if code != tc.want || called != tc.called {
				t.Fatalf("exit %d called %v", code, called)
			}
		})
	}
}
func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestLiveExecutorUsesE1HTTPAndCommittedReads(t *testing.T) {
	in := liveIntent(t)
	start := in.ExpiresAt.Add(-35 * time.Second)
	var e *exec.Executor
	posts, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(e1.DefaultTokenHeader) != "fixture" {
			t.Error("missing auth")
		}
		if r.Method == "POST" {
			posts++
			var c exec.Child
			if json.NewDecoder(r.Body).Decode(&c) != nil {
				t.Error("invalid child")
			}
			if r.Header.Get(e1.ConfirmHeader) != c.Key {
				t.Error("missing confirm")
			}
			io.WriteString(w, `{"status":"accepted","broker_order_id":"000123"}`)
			return
		}
		gets++
		snapshot := e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{}}
		if len(e.State().Sent) > 0 {
			snapshot.Orders = []e1.LedgerOrder{{BrokerOrderID: "000123", Fills: []e1.LedgerFill{}}}
		}
		json.NewEncoder(w).Encode(snapshot)
	}))
	defer server.Close()
	c, err := e1.New(e1.Config{Account: e1.MockAccount, SendEnabled: true, URL: server.URL + "/execution-intents/children"}, in, e1.Options{Token: func() (string, error) { return "fixture", nil }})
	if err != nil {
		t.Fatal(err)
	}
	var k *exec.KillSwitch
	e, k = liveExecutor(t, in, c, true)
	n := -1
	ticks := make(chan time.Time, 3)
	for range 3 {
		ticks <- time.Time{}
	}
	snapshot, _ := bookfeed.Decode(func() map[string]any {
		f, err := os.Open("../../internal/bookfeed/testdata/stream.jsonl")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		scan := bufio.NewScanner(f)
		scan.Scan()
		var row struct{ Values map[string]any }
		json.Unmarshal(scan.Bytes(), &row)
		return row.Values
	}())
	current := start
	err = driveLive(context.Background(), e, k, false, time.Time{}, liveInputs{Ticks: ticks, Now: func() time.Time { return current }, Book: func(context.Context) (bookfeed.Snapshot, bool, error) {
		n++
		if n == 1 {
			k.Engage()
		}
		if n > 2 {
			t.Fatal("did not finish")
		}
		current = start.Add(time.Duration(n) * time.Second)
		return snapshot, true, nil
	}, Ledger: c.Fills, Broker: func(_ context.Context, o exec.BrokerOrder) (brokerView, error) {
		if o.BrokerOrderID != "123" || o.TradingDay != "2026-11-02" {
			t.Error("wrong broker identity")
		}
		return brokerView{Terminal: e.State().Order == nil}, nil
	}})
	if err != nil || posts != 2 || gets != 3 || e.State().Done != exec.DoneKilled {
		t.Fatalf("posts %d gets %d done %s err %v", posts, gets, e.State().Done, err)
	}
}

func TestLiveBookSilenceCancelsAndStops(t *testing.T) {
	in := liveIntent(t)
	start := in.ExpiresAt.Add(-35 * time.Second)
	g := exectest.New(in)
	e, k := liveExecutor(t, in, g, false)
	current := start
	reads := 0
	ticks := make(chan time.Time, 2)
	ticks <- start
	var snapshot bookfeed.Snapshot
	f, err := os.Open("../../internal/bookfeed/testdata/stream.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	var row struct{ Values map[string]any }
	if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = bookfeed.Decode(row.Values)
	if err != nil {
		t.Fatal(err)
	}
	err = driveLive(context.Background(), e, k, true, in.ExpiresAt, liveInputs{Now: func() time.Time { return current }, Ticks: ticks, Book: func(context.Context) (bookfeed.Snapshot, bool, error) {
		if reads > 0 {
			current = start.Add(31 * time.Second)
		}
		reads++
		return snapshot, true, nil
	}})
	if err != nil || e.State().Done != exec.DoneStaleBook || len(g.Sends) != 2 || g.Sends[1].Action != exec.ActCancel {
		t.Fatalf("done %s sends %+v err %v", e.State().Done, g.Sends, err)
	}
}

func TestCommandLiveDryRunUsesRedisWithoutTokens(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	file, err := os.Open("../../internal/bookfeed/testdata/stream.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	var row struct{ Values map[string]any }
	if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	row.Values["ts_exchange"] = now.Format(time.RFC3339Nano)
	row.Values["ts_received"] = now.Format(time.RFC3339Nano)
	if err := r.XAdd(context.Background(), &redis.XAddArgs{Stream: "book:kis:005930", Values: row.Values}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXECMOCK_TEST_REDIS_URL", "redis://"+m.Addr())
	in := liveIntent(t)
	in.ExpiresAt = now.Add(time.Minute)
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(t.TempDir(), "intent.json")
	if err := os.WriteFile(intentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	server, hits := e1Server(t)
	cfg := writeConfig(t, "[e1]\naccount = \"kis_mock\"\nurl = "+strconvQuote(server.URL+"/execution-intents/children")+"\ntoken_env = \"UNSET_GATEWAY_TOKEN\"\n[redis]\nurl_env = \"EXECMOCK_TEST_REDIS_URL\"\n")
	code, out, errs := execmock("-config", cfg, "-intent", intentPath, "-live-book", "-dry-run", "-duration", "10ms")
	if code != 0 || hits.Load() != 0 || strings.Count(out, `"dry_run":true`) != 2 {
		t.Fatalf("code %d hits %d error %s output %s", code, hits.Load(), errs, out)
	}
}

func TestInterruptDuringReadCancelsWorkingOrder(t *testing.T) {
	for _, stage := range []string{"ledger", "broker", "book"} {
		for _, interrupted := range []bool{true, false} {
			name := stage + map[bool]string{true: "/interrupt", false: "/ordinary_error"}[interrupted]
			t.Run(name, func(t *testing.T) {
				in := liveIntent(t)
				start := in.ExpiresAt.Add(-35 * time.Second)
				gateway := exectest.New(in)
				e, kill := liveExecutor(t, in, numericGateway{gateway}, true)
				e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
				if err := e.Tick(start.UnixNano()); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				readError := errors.New("read interrupted") // HTTP readers may hide context.Canceled.
				failRead := func() error {
					if interrupted {
						cancel()
					}
					return readError
				}
				inputs := liveInputs{
					Now: func() time.Time { return start.Add(time.Second) },
					Ledger: func(context.Context) (e1.FillsSnapshot, error) {
						if stage == "ledger" {
							return e1.FillsSnapshot{}, failRead()
						}
						return e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{{BrokerOrderID: "123", Fills: []e1.LedgerFill{}}}}, nil
					},
					Broker: func(context.Context, exec.BrokerOrder) (brokerView, error) {
						if stage == "broker" {
							return brokerView{}, failRead()
						}
						return brokerView{}, nil
					},
					Book: func(context.Context) (bookfeed.Snapshot, bool, error) { return bookfeed.Snapshot{}, false, failRead() },
				}
				err := driveLive(ctx, e, kill, false, time.Time{}, inputs)
				if !interrupted {
					if !errors.Is(err, readError) || kill.Engaged() || len(gateway.Sends) != 1 {
						t.Fatalf("ordinary read error changed shutdown behavior: kill=%v sends=%+v err=%v", kill.Engaged(), gateway.Sends, err)
					}
					return
				}
				if !kill.Engaged() || len(gateway.Sends) != 2 || gateway.Sends[1].Action != exec.ActCancel {
					t.Fatalf("interrupt during %s: kill=%v sends=%+v; want kill and submit followed by cancel", stage, kill.Engaged(), gateway.Sends)
				}
				if err == nil || !strings.Contains(err.Error(), "interrupted;") || e.State().Done != "" {
					t.Fatalf("interrupt claimed clean completion: state=%+v err=%v", e.State(), err)
				}
			})
		}
	}
}
