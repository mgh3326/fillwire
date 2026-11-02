package e1_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
)

var t0 = time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

func at(sec float64) int64 { return t0.Add(time.Duration(sec * float64(time.Second))).UnixNano() }

func fixtureIntent(t *testing.T) exec.Intent {
	t.Helper()
	data, err := os.ReadFile("../testdata/e1_intent.json")
	if err != nil {
		t.Fatal(err)
	}
	in, err := exec.ParseIntent(data)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

// server is a stand-in E1 endpoint that counts every request it receives.
type server struct {
	*httptest.Server
	mu     sync.Mutex
	hits   int
	bodies [][]byte
	heads  []http.Header
	answer func(n int, body []byte) (int, string)
}

func newServer(t *testing.T, answer func(n int, body []byte) (int, string)) *server {
	s := &server{answer: answer}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.hits++
		n := s.hits
		s.bodies = append(s.bodies, body)
		s.heads = append(s.heads, r.Header.Clone())
		s.mu.Unlock()
		code, resp := http.StatusOK, `{"status":"accepted","reason":null,"child_id":1,"broker_order_id":"1001","result":{"success":true},"replayed":false,"outcome_not_recorded":false}`
		if s.answer != nil {
			code, resp = s.answer(n, body)
		}
		w.WriteHeader(code)
		io.WriteString(w, resp)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits
}

func sendConfig(url string) e1.Config {
	return e1.Config{Account: e1.MockAccount, SendEnabled: true, URL: url}
}

// tokenSource fails the test if a token is read where none may be.
func tokenSource(t *testing.T, allowed bool) func() (string, error) {
	return func() (string, error) {
		if !allowed {
			t.Error("a token was read")
		}
		return "test-token", nil
	}
}

func submit(in exec.Intent) exec.Child {
	return exec.Child{Action: exec.ActSubmit, IntentID: in.IntentID, Key: in.IntentID + ":1", Qty: 10, Price: 70_000}
}

func TestAccountGuardAcceptsOnlyKISMock(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	c, err := e1.New(sendConfig(srv.URL), in, e1.Options{Token: tokenSource(t, true)})
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Send(submit(in))
	if err != nil || out.Status != exec.StatusAccepted || out.BrokerOrderID != "1001" {
		t.Fatalf("send: %+v %v", out, err)
	}
	if srv.count() != 1 {
		t.Fatalf("hits %d", srv.count())
	}
	h := srv.heads[0]
	if h.Get(e1.DefaultTokenHeader) != "test-token" || h.Get(e1.ConfirmHeader) != in.IntentID+":1" || h.Get("Content-Type") != "application/json" {
		t.Fatalf("headers %v", h)
	}
	want := `{"action":"submit","intent_id":"` + in.IntentID + `","idempotency_key":"` + in.IntentID + `:1","quantity":10,"limit_price_krw":70000}`
	if string(srv.bodies[0]) != want {
		t.Fatalf("body %s\nwant %s", srv.bodies[0], want)
	}
}

func TestAccountGuardRefusesBeforeAnyRequest(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	for _, account := range []string{"kis_live", "", "unknown", "KIS_MOCK", " kis_mock", "kis_mock ", "kis_mock\n", "toss_live", "kiwoom_mock"} {
		for _, cfg := range []e1.Config{
			{Account: account, SendEnabled: true, URL: srv.URL},
			{Account: account, DryRun: true, URL: srv.URL},
			{Account: account, URL: srv.URL},
		} {
			c, err := e1.New(cfg, in, e1.Options{Token: tokenSource(t, false), Log: io.Discard})
			if !errors.Is(err, e1.ErrAccountNotMock) || c != nil {
				t.Fatalf("account %q cfg %+v: client %v err %v", account, cfg, c, err)
			}
			if cfg.ExecutorSends() {
				t.Fatalf("account %q cfg %+v lets the executor send", account, cfg)
			}
		}
	}
	// An intent for another account is refused too, though the config is right.
	live := in
	live.Account = "kis_live"
	if _, err := e1.New(sendConfig(srv.URL), live, e1.Options{Token: tokenSource(t, false)}); !errors.Is(err, e1.ErrAccountNotMock) {
		t.Fatalf("live intent: %v", err)
	}
	if srv.count() != 0 {
		t.Fatalf("%d requests left the process", srv.count())
	}
}

func TestSendRefusesAnotherIntent(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	c, err := e1.New(sendConfig(srv.URL), in, e1.Options{Token: tokenSource(t, true)})
	if err != nil {
		t.Fatal(err)
	}
	ch := submit(in)
	ch.IntentID = "00000000-0000-4000-8000-000000000000"
	out, err := c.Send(ch)
	if err != nil || out.Status != exec.StatusRefused || srv.count() != 0 {
		t.Fatalf("out %+v err %v hits %d", out, err, srv.count())
	}
}

func TestE1ModeNeedsBothFlags(t *testing.T) {
	cases := []struct {
		cfg  e1.Config
		mode e1.Mode
		err  error
	}{
		{e1.Config{}, e1.Off, e1.ErrAccountNotMock},
		{e1.Config{SendEnabled: true}, e1.Off, e1.ErrAccountNotMock},
		{e1.Config{Account: "kis_live", SendEnabled: true}, e1.Off, e1.ErrAccountNotMock},
		{e1.Config{Account: e1.MockAccount}, e1.Off, nil},
		{e1.Config{Account: e1.MockAccount, SendEnabled: true}, e1.Send, nil},
		{e1.Config{Account: e1.MockAccount, DryRun: true}, e1.DryRun, nil},
		{e1.Config{Account: e1.MockAccount, SendEnabled: true, DryRun: true}, e1.DryRun, nil},
	}
	for _, c := range cases {
		m, err := c.cfg.Mode()
		if m != c.mode || !errors.Is(err, c.err) || (c.err == nil) != (err == nil) {
			t.Errorf("%+v: mode %v err %v, want %v %v", c.cfg, m, err, c.mode, c.err)
		}
		if want := c.err == nil && c.mode != e1.Off; c.cfg.ExecutorSends() != want {
			t.Errorf("%+v: ExecutorSends %v", c.cfg, !want)
		}
	}
}

// runner drives one executor with an E1 client for 30 s of fresh books.
type runner struct {
	e    *exec.Executor
	j    *exec.Journal
	kill *exec.KillSwitch
}

func newRunner(t *testing.T, cfg e1.Config, in exec.Intent, opt e1.Options) *runner {
	t.Helper()
	c, err := e1.New(cfg, in, opt)
	if err != nil {
		t.Fatal(err)
	}
	j, st, err := exec.OpenJournal(filepath.Join(t.TempDir(), "exec.journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	ec := exec.DefaultConfig()
	ec.SendEnabled = cfg.ExecutorSends()
	r := &runner{j: j, kill: &exec.KillSwitch{}}
	r.e = exec.New(ec, j, st, c, r.kill)
	if err := r.e.Start(in, at(0)); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *runner) tick(sec float64) error {
	r.e.OnBook(exec.Top{Bid: 70_000, Ask: 70_100, AtNS: at(sec), Synced: true})
	return r.e.Tick(at(sec))
}

func TestDefaultConfigSendsNothing(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	// The zero config names no account: no client, so nothing to send with.
	if _, err := e1.New(e1.Config{URL: srv.URL}, in, e1.Options{Token: tokenSource(t, false)}); !errors.Is(err, e1.ErrAccountNotMock) {
		t.Fatalf("zero config: %v", err)
	}
	if (e1.Config{}).ExecutorSends() {
		t.Fatal("zero config lets the executor send")
	}
	if srv.count() != 0 {
		t.Fatalf("hits %d", srv.count())
	}
}

func TestE1OffSendsNothing(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	cfg := e1.Config{Account: e1.MockAccount, URL: srv.URL} // send_enabled unset
	r := newRunner(t, cfg, in, e1.Options{Token: tokenSource(t, false)})
	for s := 0.0; s < 30; s++ {
		if err := r.tick(s); err != nil {
			t.Fatal(err)
		}
	}
	if st := r.e.State(); len(st.Sent) != 0 || srv.count() != 0 {
		t.Fatalf("sent %v hits %d", st.Sent, srv.count())
	}
	// Even an executor wrongly enabled sends nothing through an Off client.
	c, err := e1.New(cfg, in, e1.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.Send(submit(in)); err != nil || out.Status != exec.StatusRefused || srv.count() != 0 {
		t.Fatalf("off send: %+v %v hits %d", out, err, srv.count())
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	var log bytes.Buffer
	cfg := e1.Config{Account: e1.MockAccount, SendEnabled: true, DryRun: true, URL: srv.URL}
	r := newRunner(t, cfg, in, e1.Options{Token: tokenSource(t, false), Log: &log})
	for s := 0.0; s < 30; s++ {
		r.tick(s) // the reconcile after the unanswered request fails each tick
	}
	if srv.count() != 0 {
		t.Fatalf("dry run sent %d requests", srv.count())
	}
	var line struct {
		DryRun  bool              `json:"dry_run"`
		Method  string            `json:"method"`
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	}
	dec := json.NewDecoder(&log)
	if err := dec.Decode(&line); err != nil {
		t.Fatal(err)
	}
	if dec.More() {
		t.Fatal("more than one request without an answer")
	}
	want, _ := json.Marshal(submit(in))
	if !line.DryRun || line.Method != "POST" || line.URL != srv.URL || string(line.Body) != string(want) || line.Headers[e1.ConfirmHeader] != in.IntentID+":1" {
		t.Fatalf("line %+v", line)
	}
	if st := r.e.State(); st.InFlight == nil || st.InFlightStatus != exec.StatusUncertain {
		t.Fatalf("in flight %+v %q", st.InFlight, st.InFlightStatus)
	}
}

func TestE1UncertainHoldsWithoutResend(t *testing.T) {
	answers := map[string]func(int, []byte) (int, string){
		"uncertain": func(int, []byte) (int, string) {
			return 200, `{"status":"uncertain","reason":null,"child_id":1,"broker_order_id":null,"result":{},"replayed":false,"outcome_not_recorded":false}`
		},
		"sending": func(int, []byte) (int, string) {
			return 200, `{"status":"sending","reason":null,"child_id":1,"broker_order_id":null,"result":null,"replayed":true,"outcome_not_recorded":false}`
		},
		"conflict": func(int, []byte) (int, string) {
			return 200, `{"status":"conflict","reason":"idempotency_key_reused","child_id":null,"broker_order_id":null,"result":null,"replayed":false,"outcome_not_recorded":false}`
		},
		"unknown": func(int, []byte) (int, string) {
			return 200, `{"status":"maybe","reason":null,"child_id":1,"broker_order_id":"1","result":null,"replayed":false,"outcome_not_recorded":false}`
		},
		"no order id": func(int, []byte) (int, string) {
			return 200, `{"status":"accepted","reason":null,"child_id":1,"broker_order_id":null,"result":null,"replayed":false,"outcome_not_recorded":false}`
		},
		"new field": func(int, []byte) (int, string) {
			return 200, `{"status":"accepted","reason":null,"child_id":1,"broker_order_id":"1","result":null,"replayed":false,"outcome_not_recorded":false,"fill":10}`
		},
		"not json":      func(int, []byte) (int, string) { return 200, `<html>` },
		"http 500":      func(int, []byte) (int, string) { return 500, `{"status":"refused"}` },
		"http 401":      func(int, []byte) (int, string) { return 401, `` },
		"trailing data": func(int, []byte) (int, string) { return 200, `{"status":"accepted","broker_order_id":"1"}{}` },
	}
	for name, answer := range answers {
		t.Run(name, func(t *testing.T) {
			in := fixtureIntent(t)
			srv := newServer(t, answer)
			r := newRunner(t, sendConfig(srv.URL), in, e1.Options{Token: tokenSource(t, true)})
			r.tick(0)
			for s := 1.0; s < 30; s++ {
				if err := r.tick(s); !errors.Is(err, e1.ErrNoReconcile) {
					t.Fatalf("tick %v: %v", s, err)
				}
			}
			st := r.e.State()
			if srv.count() != 1 || st.InFlight == nil || st.Filled != 0 {
				t.Fatalf("hits %d in flight %+v filled %d", srv.count(), st.InFlight, st.Filled)
			}
		})
	}
}

func TestTransportErrorIsUnknown(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	url := srv.URL
	srv.Close() // connection refused
	r := newRunner(t, sendConfig(url), in, e1.Options{Token: tokenSource(t, true)})
	r.tick(0)
	if st := r.e.State(); st.InFlight == nil || st.InFlightStatus != exec.StatusUncertain {
		t.Fatalf("in flight %+v %q", st.InFlight, st.InFlightStatus)
	}
	if err := r.tick(1); !errors.Is(err, e1.ErrNoReconcile) {
		t.Fatalf("tick: %v", err)
	}
}

func TestSendModeNeedsURLAndToken(t *testing.T) {
	in := fixtureIntent(t)
	for _, c := range []struct {
		cfg e1.Config
		opt e1.Options
	}{
		{e1.Config{Account: e1.MockAccount, SendEnabled: true}, e1.Options{Token: tokenSource(t, true)}},
		{e1.Config{Account: e1.MockAccount, SendEnabled: true, URL: "ftp://x"}, e1.Options{Token: tokenSource(t, true)}},
		{sendConfig("http://127.0.0.1:1/child"), e1.Options{}},
		{sendConfig("http://127.0.0.1:1/child"), e1.Options{Token: func() (string, error) { return "", nil }}},
		{e1.Config{Account: e1.MockAccount, DryRun: true, URL: "http://127.0.0.1:1/child"}, e1.Options{}},
	} {
		if _, err := e1.New(c.cfg, in, c.opt); !errors.Is(err, e1.ErrConfig) {
			t.Errorf("%+v: %v", c.cfg, err)
		}
	}
}

// TestTokenTravelsOnlyOverTLSOrLoopback: every request carries the gateway
// token, so New refuses plain http to any host but loopback, and a URL with
// credentials in it, before it reads the token.
func TestTokenTravelsOnlyOverTLSOrLoopback(t *testing.T) {
	in := fixtureIntent(t)
	for _, u := range []string{
		"http://gateway.example/child",
		"http://10.0.0.5:8000/child",
		"http://192.168.0.10/child",
		"http://localhost.example/child",
		"http://127.0.0.1.example/child",
		"http://[::ffff:10.0.0.5]/child",
		"http://0.0.0.0:8000/child",
		"https://user:pass@gateway.example/child",
		"http://user@127.0.0.1/child",
		"https:///child",
		"https:gateway.example/child",
	} {
		for _, cfg := range []e1.Config{sendConfig(u), {Account: e1.MockAccount, DryRun: true, URL: u}} {
			if _, err := e1.New(cfg, in, e1.Options{Token: tokenSource(t, false), Log: io.Discard}); !errors.Is(err, e1.ErrConfig) {
				t.Errorf("%s (dry run %v): %v", u, cfg.DryRun, err)
			}
		}
	}
	for _, u := range []string{
		"https://gateway.example/child",
		"http://127.0.0.1:8000/child",
		"http://127.0.0.2/child",
		"http://localhost:8000/child",
		"http://[::1]:8000/child",
	} {
		if _, err := e1.New(sendConfig(u), in, e1.Options{Token: tokenSource(t, true)}); err != nil {
			t.Errorf("%s: %v", u, err)
		}
	}
}

// TestBadHeaderConfigFailsInNew: a token header or token that Go cannot
// write would fail every request as a transport error, which the executor
// holds as uncertain with no way to reconcile. New refuses it instead.
func TestBadHeaderConfigFailsInNew(t *testing.T) {
	in := fixtureIntent(t)
	srv := newServer(t, nil)
	for _, h := range []string{"X Token", "X-Token:", "X-Token\n", "X-Tökén", "(X)", "x-execution-intent-confirm", "content-type", "Host"} {
		for _, cfg := range []e1.Config{
			{Account: e1.MockAccount, SendEnabled: true, URL: srv.URL, TokenHeader: h},
			{Account: e1.MockAccount, DryRun: true, URL: srv.URL, TokenHeader: h},
			{Account: e1.MockAccount, URL: srv.URL, TokenHeader: h},
		} {
			if _, err := e1.New(cfg, in, e1.Options{Token: tokenSource(t, false), Log: io.Discard}); !errors.Is(err, e1.ErrConfig) {
				t.Errorf("header %q (send %v, dry run %v): %v", h, cfg.SendEnabled, cfg.DryRun, err)
			}
		}
	}
	for _, tok := range []string{"tok-secret\nb", "tok-secret\r\nX-Evil: 1", "tok-secret\x00", " tok-secret", "tok-secret ", "tok-secrét"} {
		_, err := e1.New(sendConfig(srv.URL), in, e1.Options{Token: func() (string, error) { return tok, nil }})
		if !errors.Is(err, e1.ErrConfig) || strings.Contains(err.Error(), "tok-secret") {
			t.Errorf("token %q: %v", tok, err)
		}
	}
	c, err := e1.New(e1.Config{Account: e1.MockAccount, SendEnabled: true, URL: srv.URL, TokenHeader: "X-Gateway_Token.v2"}, in, e1.Options{Token: tokenSource(t, true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(submit(in)); err != nil {
		t.Fatalf("valid header: %v", err)
	}
	if srv.count() != 1 {
		t.Fatalf("hits %d", srv.count())
	}
}

// TestURLErrorsHideCredentials: a refused URL is never echoed with its
// userinfo, since callers log these errors.
func TestURLErrorsHideCredentials(t *testing.T) {
	in := fixtureIntent(t)
	for _, u := range []string{
		"https://user:s3cret-pw@gateway.example/child",
		"http://user:s3cret-pw@127.0.0.1/child",
		"http://user:s3cret-pw@[::1/child",
		"ftp://user:s3cret-pw@gateway.example/child",
	} {
		_, err := e1.New(sendConfig(u), in, e1.Options{Token: tokenSource(t, false)})
		if !errors.Is(err, e1.ErrConfig) || strings.Contains(err.Error(), "s3cret-pw") {
			t.Errorf("%q: %v", u, err)
		}
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	in := fixtureIntent(t)
	elsewhere := newServer(t, nil)
	srv := newServer(t, func(int, []byte) (int, string) { return 0, "" })
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL, http.StatusTemporaryRedirect)
	})
	c, err := e1.New(sendConfig(srv.URL), in, e1.Options{Token: tokenSource(t, true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(submit(in)); err == nil {
		t.Fatal("a redirect answered as an outcome")
	}
	if elsewhere.count() != 0 {
		t.Fatalf("the redirect was followed: %d requests elsewhere", elsewhere.count())
	}
}
