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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
	"github.com/mgh3326/fillwire/internal/kisread"
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
		{"default_off", "kis_mock", false, false, false, 0, false}, {"flag_without_config", "kis_mock", false, true, false, 2, false}, {"config_without_flag", "kis_mock", true, false, false, 2, false}, {"live_account", "kis_live", true, true, false, 78, false}, {"mock_explicit", "kis_mock", true, true, false, 0, true}, {"dry_run_wins", "kis_mock", true, true, true, 0, true},
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
			code := runWithLive(args, io.Discard, io.Discard, func(_ fileConfig, cfg e1.Config, _ exec.Config, _, _ string, _ time.Duration, _ io.Writer) error {
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
	template, err := bookfeed.Decode(row.Values)
	if err != nil {
		t.Fatal(err)
	}
	var bookReads atomic.Int64
	// Deliver the recorded book with current timestamps when the command reads
	// it, so journal startup time cannot make the fixture stale.
	m.Server().SetPreHook(func(peer *redisserver.Peer, command string, args ...string) bool {
		if command != "XREVRANGE" || len(args) == 0 || args[0] != "book:kis:"+template.Symbol {
			return false
		}
		bookReads.Add(1)
		snapshot := template
		snapshot.Exchange = time.Now()
		snapshot.Received = snapshot.Exchange
		var values []string
		for key, value := range snapshot.Values() {
			values = append(values, key, value.(string))
		}
		if _, err := m.XAdd(args[0], "*", values); err != nil {
			peer.WriteError("fixture snapshot unavailable")
			return true
		}
		return false
	})
	t.Setenv("EXECMOCK_TEST_REDIS_URL", "redis://"+m.Addr())
	t.Setenv("UNSET_GATEWAY_TOKEN", "")
	in := liveIntent(t)
	if in.Symbol != template.Symbol {
		t.Fatal("intent and book fixture symbols differ")
	}
	// End through the price cap, not a 10ms wall-clock window that can expire
	// before the journal fsync completes. Keep safety limits past the test deadline.
	runFor := time.Hour
	if deadline, ok := t.Deadline(); ok {
		runFor = time.Until(deadline) + time.Minute
	}
	in.ExpiresAt = time.Now().Add(runFor)
	in.LimitPriceKRW = template.Bids[0].Price
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
	code, out, errs := execmock("-config", cfg, "-intent", intentPath, "-live-book", "-dry-run", "-duration", runFor.String(), "-step", "1ns")
	if code != 0 || hits.Load() != 0 || bookReads.Load() == 0 || strings.Count(out, `"dry_run":true`) != 2 {
		t.Fatalf("code %d HTTP hits %d book reads %d error %s output %s", code, hits.Load(), bookReads.Load(), errs, out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d output lines, want exactly 2 (submit and cancel):\n%s", len(lines), out)
	}
	for i, line := range lines {
		var envelope struct {
			Body exec.Child `json:"body"`
		}
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			t.Fatal(err)
		}
		if want := []string{exec.ActSubmit, exec.ActCancel}[i]; envelope.Body.Action != want {
			t.Fatalf("body %d action=%q, want %q", i, envelope.Body.Action, want)
		}
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

func TestLiveLadderDoesNotRereadConfirmedTerminalOrders(t *testing.T) {
	in := liveIntent(t)
	in.LimitPriceKRW = 71000
	start := in.ExpiresAt.Add(-35 * time.Second)
	gateway := exectest.New(in)
	e, kill := liveExecutor(t, in, gateway, true)
	seconds := []int{0, 5, 10, 15, 20, 25, 30, 31}
	ticks := make(chan time.Time, len(seconds))
	for range seconds {
		ticks <- time.Time{}
	}
	current := start
	index := 0
	calls := map[string]int{}
	err := driveLive(context.Background(), e, kill, false, time.Time{}, liveInputs{
		Ticks: ticks, Now: func() time.Time { return current },
		Ledger: func(context.Context) (e1.FillsSnapshot, error) {
			orders, err := e.BrokerOrders()
			if err != nil {
				return e1.FillsSnapshot{}, err
			}
			snapshot := e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{}}
			for _, o := range orders {
				snapshot.Orders = append(snapshot.Orders, e1.LedgerOrder{BrokerOrderID: o.BrokerOrderID, Fills: []e1.LedgerFill{}})
			}
			return snapshot, nil
		},
		Broker: func(_ context.Context, o exec.BrokerOrder) (brokerView, error) {
			calls[o.Key]++
			working := e.State().Order
			return brokerView{Terminal: working == nil || o.Key != working.Key}, nil
		},
		Book: func(context.Context) (bookfeed.Snapshot, bool, error) {
			if index >= len(seconds) {
				t.Fatal("ladder did not finish")
			}
			current = start.Add(time.Duration(seconds[index]) * time.Second)
			index++
			if current.Equal(start.Add(30 * time.Second)) {
				kill.Engage()
			}
			snapshot := bookfeed.Snapshot{Symbol: in.Symbol, Exchange: current, Received: current, Source: "ws"}
			snapshot.Bids[0].Price = 70000
			snapshot.Asks[0].Price = 70200
			return snapshot, true, nil
		},
	})
	if err != nil || e.State().Done != exec.DoneKilled {
		t.Fatalf("ladder did not finish: done=%q err=%v", e.State().Done, err)
	}
	if len(calls) != 6 {
		t.Fatalf("checked %d orders, want 6", len(calls))
	}
	for key, count := range calls {
		if count != 2 {
			t.Fatalf("broker readbacks for %s=%d, want 2 (working then terminal)", key, count)
		}
	}
}

func TestPollDeadlineCancelsWorkingOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := liveIntent(t)
		start := in.ExpiresAt.Add(-35 * time.Second)
		gateway := exectest.New(in)
		e, kill := liveExecutor(t, in, numericGateway{gateway}, true)
		e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
		if err := e.Tick(start.UnixNano()); err != nil {
			t.Fatal(err)
		}
		parent := context.Background()
		err := driveLive(parent, e, kill, false, time.Time{}, liveInputs{
			Now: func() time.Time { return start.Add(time.Second) },
			Ledger: func(context.Context) (e1.FillsSnapshot, error) {
				return e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{{BrokerOrderID: "123", Fills: []e1.LedgerFill{}}}}, nil
			},
			Broker: func(ctx context.Context, _ exec.BrokerOrder) (brokerView, error) {
				<-ctx.Done()
				return brokerView{}, errors.New("readback unavailable")
			},
			Book: func(context.Context) (bookfeed.Snapshot, bool, error) {
				t.Fatal("book read after failed broker poll")
				return bookfeed.Snapshot{}, false, nil
			},
		})
		if parent.Err() != nil {
			t.Fatal("parent context unexpectedly canceled")
		}
		if !kill.Engaged() || len(gateway.Sends) != 2 || gateway.Sends[1].Action != exec.ActCancel {
			t.Fatalf("poll deadline: kill=%v sends=%+v; want submit followed by cancel", kill.Engaged(), gateway.Sends)
		}
		if !errors.Is(err, context.DeadlineExceeded) || e.State().Done != "" {
			t.Fatalf("deadline lost or clean finish claimed: done=%q err=%v", e.State().Done, err)
		}
	})
}

func TestCachedTerminalReadbackStillChecksLedgerCumulativeQuantity(t *testing.T) {
	in := liveIntent(t)
	start := in.ExpiresAt.Add(-35 * time.Second)
	e, _ := liveExecutor(t, in, numericGateway{exectest.New(in)}, true)
	e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
	if err := e.Tick(start.UnixNano()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	broker := cacheTerminalBrokerViews(func(context.Context, exec.BrokerOrder) (brokerView, error) {
		calls++
		return brokerView{Terminal: true}, nil
	})
	snapshot := e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{{BrokerOrderID: "123", Fills: []e1.LedgerFill{}}}}
	read := func(context.Context) (e1.FillsSnapshot, error) { return snapshot, nil }
	if err := refreshLedger(context.Background(), e, read, broker); err != nil {
		t.Fatal(err)
	}
	snapshot.Orders[0].CumulativeQty = 1
	snapshot.Orders[0].Fills = []e1.LedgerFill{{FillID: "late-ledger-row", Side: in.Side, Qty: 1, Price: "70000", FilledAt: start.Add(time.Second), CommittedAt: start.Add(2 * time.Second)}}
	if err := refreshLedger(context.Background(), e, read, broker); !errors.Is(err, exec.ErrIncomplete) {
		t.Fatalf("cached terminal evidence hid a cumulative mismatch: %v", err)
	}
	if calls != 1 {
		t.Fatalf("terminal readback calls=%d, want 1", calls)
	}
}

func TestLivePollHandlesSevenUnverifiedOrdersAtMockReadRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := liveIntent(t)
		base := time.Now()
		in.ExpiresAt = base.Add(2 * time.Minute)
		in.LimitPriceKRW = 71000
		gateway := exectest.New(in)
		journal, state, err := exec.OpenJournal(filepath.Join(t.TempDir(), "exec.journal"))
		if err != nil {
			t.Fatal(err)
		}
		defer journal.Close()
		cfg := exec.DefaultConfig()
		cfg.SendEnabled = true
		cfg.RequireLedgerComplete = true
		kill := &exec.KillSwitch{}
		e := exec.New(cfg, journal, state, gateway, kill)
		if err := e.Start(in, base.UnixNano()); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 7; i++ {
			if i > 0 {
				time.Sleep(5 * time.Second)
			}
			e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: time.Now().UnixNano(), Synced: true})
			if err := e.Tick(time.Now().UnixNano()); err != nil {
				t.Fatal(err)
			}
		}
		orders, err := e.BrokerOrders()
		if err != nil {
			t.Fatal(err)
		}
		if len(orders) != 7 {
			t.Fatalf("journal orders=%d, want 7", len(orders))
		}
		var readTimes []time.Time
		client := &kisread.Client{Mock: true, AppKey: "fixture", AppSecret: "fixture", Token: func() string { return "fixture" }, HTTP: &http.Client{Transport: brokerTransport(func(r *http.Request) (*http.Response, error) {
			readTimes = append(readTimes, time.Now())
			canceled := "N"
			if kill.Engaged() {
				canceled = "Y"
			}
			body, err := json.Marshal(map[string]any{"rt_cd": "0", "output1": []map[string]string{{"odno": r.URL.Query().Get("ODNO"), "ord_dt": r.URL.Query().Get("INQR_STRT_DT"), "pdno": in.Symbol, "sll_buy_dvsn_cd": "02", "ord_qty": "10", "tot_ccld_qty": "0", "cncl_yn": canceled, "rjct_qty": "0"}}})
			if err != nil {
				t.Fatal(err)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
		})}}
		broker := historyRead{Client: client, Account: "fixture", Product: "fixture", Symbol: in.Symbol, Side: in.Side}
		ticks := make(chan time.Time, 2)
		ticks <- time.Time{}
		ticks <- time.Time{}
		bookReads := 0
		err = driveLive(context.Background(), e, kill, false, time.Time{}, liveInputs{
			Now: time.Now, Ticks: ticks, Broker: broker.read,
			Ledger: func(context.Context) (e1.FillsSnapshot, error) {
				snapshot := e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{}}
				for _, o := range orders {
					snapshot.Orders = append(snapshot.Orders, e1.LedgerOrder{BrokerOrderID: o.BrokerOrderID, Fills: []e1.LedgerFill{}})
				}
				return snapshot, nil
			},
			Book: func(context.Context) (bookfeed.Snapshot, bool, error) {
				bookReads++
				// Ending the test run happens only after all seven healthy readbacks.
				// A pacing-induced deadline must never engage this switch first.
				if bookReads == 1 && kill.Engaged() {
					t.Fatal("healthy broker polling engaged kill")
				}
				kill.Engage()
				snapshot := bookfeed.Snapshot{Symbol: in.Symbol, Exchange: time.Now(), Received: time.Now(), Source: "ws"}
				snapshot.Bids[0].Price = 70000
				snapshot.Asks[0].Price = 70200
				return snapshot, true, nil
			},
		})
		if err != nil {
			t.Fatalf("healthy seven-order poll tripped deadline: reads=%d err=%v", len(readTimes), err)
		}
		if bookReads != 2 || e.State().Done != exec.DoneKilled || len(readTimes) != 14 {
			t.Fatalf("book reads=%d broker reads=%d done=%q", bookReads, len(readTimes), e.State().Done)
		}
		for i := 1; i < len(readTimes); i++ {
			if readTimes[i].Sub(readTimes[i-1]) < time.Second {
				t.Fatal("mock 1/s throttle was bypassed")
			}
		}
	})
}

func TestInterruptRetainsReasonWhenExecutorTickFails(t *testing.T) {
	in := liveIntent(t)
	start := in.ExpiresAt.Add(-35 * time.Second)
	e, kill := liveExecutor(t, in, numericGateway{exectest.New(in)}, true)
	e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
	if err := e.Tick(start.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := e.OnLedgerFill("unknown", exec.Fill{FillID: "unknown-fill", Qty: 1, Price: 70000, AtNS: start.Add(time.Second).UnixNano()}); !errors.Is(err, exec.ErrAttribution) {
		t.Fatalf("failed to reproduce executor halt: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := driveLive(ctx, e, kill, false, time.Time{}, liveInputs{
		Now: func() time.Time { return start.Add(time.Second) },
		Ledger: func(context.Context) (e1.FillsSnapshot, error) {
			cancel()
			return e1.FillsSnapshot{}, errors.New("read interrupted")
		},
	})
	if !errors.Is(err, exec.ErrAttribution) || !strings.Contains(err.Error(), "interrupted; verify terminal broker/ledger evidence") || !strings.Contains(err.Error(), "ledger fill attribution failed") {
		t.Fatalf("lost interrupt reason or cancel-tick error: %v", err)
	}
}

func TestPollDeadlineRetainsReasonWhenExecutorTickFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := liveIntent(t)
		start := in.ExpiresAt.Add(-35 * time.Second)
		e, kill := liveExecutor(t, in, numericGateway{exectest.New(in)}, true)
		e.OnBook(exec.Top{Bid: 70000, Ask: 70200, AtNS: start.UnixNano(), Synced: true})
		if err := e.Tick(start.UnixNano()); err != nil {
			t.Fatal(err)
		}
		if err := e.OnLedgerFill("unknown", exec.Fill{FillID: "unknown-fill", Qty: 1, Price: 70000, AtNS: start.Add(time.Second).UnixNano()}); !errors.Is(err, exec.ErrAttribution) {
			t.Fatalf("failed to reproduce executor halt: %v", err)
		}
		err := driveLive(context.Background(), e, kill, false, time.Time{}, liveInputs{
			Now: func() time.Time { return start.Add(time.Second) },
			Ledger: func(context.Context) (e1.FillsSnapshot, error) {
				return e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{{BrokerOrderID: "123", Fills: []e1.LedgerFill{}}}}, nil
			},
			Broker: func(ctx context.Context, _ exec.BrokerOrder) (brokerView, error) {
				<-ctx.Done()
				return brokerView{}, errors.New("readback unavailable")
			},
		})
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, exec.ErrAttribution) || !strings.Contains(err.Error(), "operator reconcile required") || !strings.Contains(err.Error(), "ledger fill attribution failed") {
			t.Fatalf("lost deadline reason or cancel-tick error: %v", err)
		}
	})
}
