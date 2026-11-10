package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// withReconcile appends a [reconcile] table to the shipped example, which
// already carries one; the shipped table is removed first.
func withReconcile(t *testing.T, body string) string {
	t.Helper()
	shipped := shippedConfig(t)
	start := strings.Index(shipped, "[reconcile]\n")
	if start < 0 {
		t.Fatal("shipped fillwire.toml has no [reconcile] table")
	}
	end := strings.Index(shipped[start:], "\n\n")
	if end < 0 {
		t.Fatal("cannot find the end of the shipped [reconcile] table")
	}
	return shipped[:start] + body + shipped[start+end:]
}

const reconcileURL = "https://auto-trader.example.invalid/trading/api/execution-ledger/reconcile/trigger"

func TestReconcileDefaultOff(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	if !strings.Contains(shipped, "[reconcile]\nenabled = false\n") {
		t.Fatal("shipped fillwire.toml must show the reconcile trigger disabled")
	}
	for name, text := range map[string]string{
		"shipped":                               shipped,
		"no table":                              withReconcile(t, ""),
		"enabled omitted":                       withReconcile(t, "[reconcile]\nurl = \""+reconcileURL+"\"\n"),
		"disabled, unused fields not validated": withReconcile(t, "[reconcile]\nenabled = false\nurl = \"ftp://x\"\nmarkets = [\"zz\"]\ninterval = \"1s\"\n"),
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", text))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.reconcile.enabled {
				t.Fatalf("reconcile = %+v, want off", cfg.reconcile)
			}
		})
	}
}

func TestReconcileDefaultsWhenEnabled(t *testing.T) {
	setFillsEnv(t)
	cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withReconcile(t, "[reconcile]\nenabled = true\nurl = \""+reconcileURL+"\"\n")))
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.reconcile
	if !got.enabled || got.url != reconcileURL || len(got.markets) != 1 || got.markets[0] != "kr" || !got.dryRun || got.interval != 0 || got.timeout != 10*time.Second {
		t.Fatalf("defaults = %+v", got)
	}
}

func TestReconcileExplicitValues(t *testing.T) {
	setFillsEnv(t)
	cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withReconcile(t, `[reconcile]
enabled = true
url = "http://127.0.0.1:8000/trading/api/execution-ledger/reconcile/trigger"
markets = ["kr", "us"]
dry_run = false
interval = "30m"
timeout = "3s"
`)))
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.reconcile
	if got.dryRun || got.interval != 30*time.Minute || got.timeout != 3*time.Second || len(got.markets) != 2 {
		t.Fatalf("explicit = %+v", got)
	}
}

func TestReconcileRejectsBadSettings(t *testing.T) {
	setFillsEnv(t)
	for name, body := range map[string]string{
		"unknown key, enabled":   "enabled = true\nurl = \"" + reconcileURL + "\"\ntoken = \"x\"\n",
		"unknown key, disabled":  "enabled = false\ntoken = \"x\"\n",
		"wrong type":             "enabled = \"yes\"\n",
		"missing url":            "enabled = true\n",
		"plain http, not local":  "enabled = true\nurl = \"http://auto-trader.example.invalid/trigger\"\n",
		"other scheme":           "enabled = true\nurl = \"ftp://127.0.0.1/trigger\"\n",
		"no path":                "enabled = true\nurl = \"https://auto-trader.example.invalid\"\n",
		"root path":              "enabled = true\nurl = \"https://auto-trader.example.invalid/\"\n",
		"query":                  "enabled = true\nurl = \"" + reconcileURL + "?x=1\"\n",
		"fragment":               "enabled = true\nurl = \"" + reconcileURL + "#x\"\n",
		"userinfo":               "enabled = true\nurl = \"https://user:pw@auto-trader.example.invalid/trigger\"\n",
		"unknown market":         "enabled = true\nurl = \"" + reconcileURL + "\"\nmarkets = [\"jp\"]\n",
		"repeated market":        "enabled = true\nurl = \"" + reconcileURL + "\"\nmarkets = [\"kr\", \"kr\"]\n",
		"empty markets":          "enabled = true\nurl = \"" + reconcileURL + "\"\nmarkets = []\n",
		"interval too short":     "enabled = true\nurl = \"" + reconcileURL + "\"\ninterval = \"10s\"\n",
		"interval negative":      "enabled = true\nurl = \"" + reconcileURL + "\"\ninterval = \"-1m\"\n",
		"interval not duration":  "enabled = true\nurl = \"" + reconcileURL + "\"\ninterval = \"often\"\n",
		"timeout zero":           "enabled = true\nurl = \"" + reconcileURL + "\"\ntimeout = \"0s\"\n",
		"timeout not a duration": "enabled = true\nurl = \"" + reconcileURL + "\"\ntimeout = \"soon\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadConfig(writeTemp(t, "fillwire.toml", withReconcile(t, "[reconcile]\n"+body)))
			if err == nil {
				t.Fatal("config accepted")
			}
			for _, secret := range []string{"fixture-ingest", "user:pw", "auto-trader.example.invalid"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error echoes %q: %v", secret, err)
				}
			}
		})
	}
}

func TestReconcileDisabledStartsNothing(t *testing.T) {
	scheduler, err := startReconcile(t.Context(), runtimeConfig{}, nil)
	if err != nil || scheduler != nil {
		t.Fatalf("scheduler=%v err=%v, want nil, nil", scheduler, err)
	}
}

type reconcileProbe struct {
	mu       sync.Mutex
	bodies   []string
	tokens   []string
	inFlight int
	maxIn    int
	hang     bool
}

func (p *reconcileProbe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	p.bodies = append(p.bodies, string(raw))
	p.tokens = append(p.tokens, r.Header.Get(sinkTokenHeader))
	p.inFlight++
	if p.inFlight > p.maxIn {
		p.maxIn = p.inFlight
	}
	hang := p.hang
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.inFlight--; p.mu.Unlock() }()
	if hang {
		select {
		case <-r.Context().Done():
		case <-time.After(20 * time.Second):
		}
		return
	}
	_, _ = io.WriteString(w, `{"status":"executed","deduped":false,"backfilled":0}`)
}

const sinkTokenHeader = "X-Execution-Ledger-Ingest-Token"

func (p *reconcileProbe) snapshot() (bodies []string, maxIn int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.bodies...), p.maxIn
}

func enableReconcile(url string) func(*runtimeConfig) {
	return func(cfg *runtimeConfig) {
		cfg.alertRateLimit = time.Minute
		cfg.reconcile = reconcileSettings{enabled: true, url: url, markets: []string{"kr"}, dryRun: true, timeout: 10 * time.Second}
	}
}

// After the initial subscription the real run sends exactly one trigger with
// the default body and the ingest token header, and the fills still arrive.
func TestRunSendsStartupTrigger(t *testing.T) {
	probe := &reconcileProbe{}
	srv := httptest.NewServer(probe)
	defer srv.Close()
	result := runFillsWith(t, nil, enableReconcile(srv.URL+"/trigger"))
	if result.err != nil || len(result.entries) != isoFills {
		t.Fatalf("err=%v fills=%d", result.err, len(result.entries))
	}
	bodies, _ := probe.snapshot()
	t.Logf("trigger bodies seen by the fake server: %q", bodies)
	if len(bodies) != 1 || bodies[0] != `{"market":"kr","dry_run":true,"reason":"reconnect"}` {
		t.Fatalf("bodies = %q", bodies)
	}
	if probe.tokens[0] != "synthetic-ingest-token" {
		t.Fatal("trigger did not carry the ingest token in the ingest header")
	}
}

// A trigger server that never answers must not delay decode, XADD or
// shutdown, and never has more than one request in flight.
func TestHungReconcileServerDoesNotBlockFills(t *testing.T) {
	probe := &reconcileProbe{hang: true}
	srv := httptest.NewServer(probe)
	defer srv.CloseClientConnections()
	defer srv.Close()
	start := time.Now()
	result := runFillsWith(t, nil, enableReconcile(srv.URL+"/trigger"))
	elapsed := time.Since(start)
	if result.err != nil || len(result.entries) != isoFills {
		t.Fatalf("err=%v fills=%d, want %d fills and a clean stop", result.err, len(result.entries), isoFills)
	}
	bodies, maxIn := probe.snapshot()
	t.Logf("elapsed=%s requests=%d max in flight=%d fills=%d", elapsed, len(bodies), maxIn, len(result.entries))
	if len(bodies) != 1 || maxIn != 1 {
		t.Fatalf("requests=%d maxInFlight=%d, want 1 and 1", len(bodies), maxIn)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("run took %s with a hung trigger server", elapsed)
	}
}

// With the table off, the trigger server sees no request at all.
func TestRunDisabledReconcileMakesNoCalls(t *testing.T) {
	probe := &reconcileProbe{}
	srv := httptest.NewServer(probe)
	defer srv.Close()
	result := runFillsWith(t, nil, func(cfg *runtimeConfig) {
		cfg.reconcile = reconcileSettings{url: srv.URL + "/trigger", markets: []string{"kr"}, dryRun: true, timeout: time.Second} // enabled left false
	})
	if result.err != nil || len(result.entries) != isoFills {
		t.Fatalf("err=%v fills=%d", result.err, len(result.entries))
	}
	if bodies, _ := probe.snapshot(); len(bodies) != 0 {
		t.Fatalf("disabled reconcile made %d calls", len(bodies))
	}
}
