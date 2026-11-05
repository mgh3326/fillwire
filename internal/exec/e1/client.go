// Package e1 is the executor's client for the E1 intent gateway (auto_trader
// ROB-1440, docs/design/1440-intent-gateway-contract.md). It implements
// exec.Gateway and sends E1's child request JSON and reads E1's outcome JSON.
//
// It reaches only the KIS mock account: New refuses every account surface but
// the exact "kis_mock", and Send checks again before it builds a request.
// Sending is off unless Config sets both SendEnabled and Account. Dry-run
// writes each request it would send and sends nothing.
//
// E1 leaves the transport to E2. This package binds it to HTTP (see
// docs/e3a-mock-runbook.md, "Transport binding"):
//
//	POST <url>
//	Content-Type: application/json
//	<token header>: <token>
//	X-Execution-Intent-Confirm: <idempotency_key>
//
//	<child request JSON>
//
// A 200 answer carries E1's outcome JSON. Any other answer, a transport
// error, or a body this package cannot read is an unknown outcome.
package e1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/netpolicy"
)

// MockAccount is the one account surface E1 marks as mock (auto_trader
// decisions.py MOCK_ACCOUNTS; the ExecutionIntentV1 account Literal).
const MockAccount = "kis_mock"

// ConfirmHeader carries the per-action confirmation: the idempotency key of
// the request it confirms. E1 must pass confirm=True only when it equals the
// body's idempotency_key.
const ConfirmHeader = "X-Execution-Intent-Confirm"

// DefaultTokenHeader is the header the gateway token travels in.
const DefaultTokenHeader = "X-Execution-Intent-Gateway-Token"

// maxAnswerBytes bounds an answer body. E1 limits result to 16 KiB.
const maxAnswerBytes = 64 << 10

var (
	// ErrAccountNotMock refuses any account surface but MockAccount.
	ErrAccountNotMock = errors.New("e1: account is not kis_mock")
	// ErrConfig refuses a config that cannot run as written.
	ErrConfig = errors.New("e1: invalid config")
	// ErrNoReconcile means the route has no usable reconciled view.
	ErrNoReconcile = errors.New("e1: no usable reconcile view; the child stays unresolved")
	// ErrNoAnswer is a dry-run send with no simulated answer: the outcome is
	// unknown and the executor holds.
	ErrNoAnswer = errors.New("e1: dry run, no answer")
)

// Mode is what a client does with a child request.
type Mode int

const (
	// Off sends nothing: Send answers refused without building a request.
	Off Mode = iota
	// DryRun writes each request and sends nothing.
	DryRun
	// Send posts each request to E1.
	Send
)

func (m Mode) String() string {
	switch m {
	case Off:
		return "off"
	case DryRun:
		return "dry-run"
	case Send:
		return "send"
	}
	return fmt.Sprintf("Mode(%d)", int(m))
}

// Config selects the account and the mode. The zero value is refused, since
// it names no account.
type Config struct {
	// Account must be exactly MockAccount, in every mode.
	Account string
	// SendEnabled must be set, with Account, before anything is sent.
	SendEnabled bool
	// DryRun writes requests instead of sending them. It wins over
	// SendEnabled.
	DryRun bool
	// URL is the gateway's child request endpoint: https, or http to a
	// loopback host only, since each request carries the gateway token.
	URL string
	// TokenHeader names the header the token travels in; empty means
	// DefaultTokenHeader.
	TokenHeader string
	// Timeout bounds one request; zero means 10 s.
	Timeout time.Duration
}

// Mode reports what this config does, or why it is refused.
func (c Config) Mode() (Mode, error) {
	if c.Account != MockAccount {
		return Off, fmt.Errorf("%w: %q", ErrAccountNotMock, c.Account)
	}
	switch {
	case c.DryRun:
		return DryRun, nil
	case c.SendEnabled:
		return Send, nil
	}
	return Off, nil
}

// ExecutorSends is the exec.Config.SendEnabled this config allows: true only
// in DryRun and Send. In Off the executor itself sends nothing either.
func (c Config) ExecutorSends() bool {
	m, err := c.Mode()
	return err == nil && m != Off
}

// Options are the parts of a client that tests and dry runs replace.
type Options struct {
	// Token returns the gateway token. It is called once, by New, and only in
	// Send mode; a dry run never reads a credential.
	Token func() (string, error)
	// HTTP is the client for Send mode; nil means one with Config.Timeout.
	HTTP *http.Client
	// Log receives one JSON line per request in DryRun mode.
	Log io.Writer
	// Answer simulates E1 in DryRun mode. Nil means every request ends with
	// ErrNoAnswer (unknown outcome).
	Answer func(exec.Child) (Outcome, error)
}

// Client is an exec.Gateway for one intent on the KIS mock account.
type Client struct {
	mode    Mode
	account string
	intent  exec.Intent
	url     string
	header  string
	token   string
	http    *http.Client
	log     io.Writer
	answer  func(exec.Child) (Outcome, error)
}

var _ exec.Gateway = (*Client)(nil)

// New returns a client for intent in. It refuses a config or intent whose
// account is not MockAccount before it reads any token or opens anything.
func New(cfg Config, in exec.Intent, opt Options) (*Client, error) {
	mode, err := cfg.Mode()
	if err != nil {
		return nil, err
	}
	if in.Account != MockAccount {
		return nil, fmt.Errorf("%w: intent account %q", ErrAccountNotMock, in.Account)
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	c := &Client{mode: mode, account: cfg.Account, intent: in, header: cfg.TokenHeader}
	if c.header == "" {
		c.header = DefaultTokenHeader
	}
	// A header Go cannot write fails each request as a transport error,
	// which the executor holds as uncertain for good: refuse it here.
	if !headerName(c.header) {
		return nil, fmt.Errorf("%w: token header %q is not a valid header name", ErrConfig, c.header)
	}
	switch http.CanonicalHeaderKey(c.header) {
	case ConfirmHeader, "Content-Type", "Content-Length", "Host", "Transfer-Encoding":
		return nil, fmt.Errorf("%w: token header %q collides with a header the client sets", ErrConfig, c.header)
	}
	if mode == Off {
		return c, nil
	}
	if c.url, err = checkURL(cfg.URL); err != nil {
		return nil, err
	}
	if mode == DryRun {
		if opt.Log == nil {
			return nil, fmt.Errorf("%w: dry run needs a log", ErrConfig)
		}
		c.log, c.answer = opt.Log, opt.Answer
		return c, nil
	}
	if opt.Token == nil {
		return nil, fmt.Errorf("%w: send mode needs a token source", ErrConfig)
	}
	if c.token, err = opt.Token(); err != nil {
		return nil, fmt.Errorf("%w: token: %v", ErrConfig, err)
	}
	if c.token == "" {
		return nil, fmt.Errorf("%w: empty token", ErrConfig)
	}
	if !headerValue(c.token) {
		return nil, fmt.Errorf("%w: token is not a valid header value", ErrConfig) // the token itself is never printed
	}
	hc := http.Client{Timeout: cfg.Timeout}
	if opt.HTTP != nil {
		hc = *opt.HTTP
	}
	if hc.Timeout <= 0 {
		hc.Timeout = 10 * time.Second
	}
	// A redirect would re-post the child, and the token header, to another
	// URL. Never follow one: the answer is then a non-200, an unknown outcome.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.http = &hc
	return c, nil
}

// checkURL accepts an https URL, or an http URL whose host is loopback
// (localhost, 127.0.0.0/8, ::1). Every request carries the gateway token, so
// plain http to any other host would hand it to whatever is on the path. A
// URL with credentials in it is refused too: the token header is the only
// credential.
//
// A query or a fragment is refused for the same reason: the endpoint needs
// neither, and a credential put there would travel in every request and in
// every error that names the URL. An accepted URL is therefore safe to log.
//
// The errors never echo the URL as given. A URL that parses is shown without
// its userinfo, query and fragment; one that does not is not shown.
func checkURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: url does not parse", ErrConfig)
	}
	bare := *u
	bare.User, bare.RawQuery, bare.ForceQuery, bare.Fragment, bare.RawFragment = nil, "", false, "", ""
	shown := bare.String()
	switch {
	case u.User != nil:
		return "", fmt.Errorf("%w: url %q: credentials in a url are refused; the token header is the only credential", ErrConfig, shown)
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "":
		return "", fmt.Errorf("%w: url %q: a query or fragment is refused; the token header is the only credential", ErrConfig, shown)
	case u.Host == "" || u.Hostname() == "" || u.Opaque != "":
		return "", fmt.Errorf("%w: url %q", ErrConfig, shown)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !netpolicy.IsLoopbackHost(u.Hostname()) {
			return "", fmt.Errorf("%w: url %q: http only to a loopback host; use https", ErrConfig, shown)
		}
	default:
		return "", fmt.Errorf("%w: url %q", ErrConfig, shown)
	}
	return u.String(), nil
}

// headerName reports whether s is an HTTP field name: one or more token
// characters (RFC 9110, section 5.6.2).
func headerName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", b) >= 0:
		default:
			return false
		}
	}
	return true
}

// headerValue reports whether s can be sent as a field value: visible ASCII
// and spaces, with no leading or trailing space.
func headerValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if b := s[i]; b < 0x20 || b > 0x7e {
			return false
		}
	}
	return strings.TrimSpace(s) == s
}

// Mode reports the client's mode.
func (c *Client) Mode() Mode { return c.mode }

// local is an answer from this client; nothing left the process.
func local(c exec.Child, reason string) exec.Outcome {
	return exec.Outcome{Key: c.Key, Status: exec.StatusRefused, Reason: "fillwire: " + reason}
}

// Send implements exec.Gateway. A refusal it makes itself is a refused
// outcome with a "fillwire:" reason, and no request is built.
func (c *Client) Send(ch exec.Child) (exec.Outcome, error) {
	if c.account != MockAccount || c.intent.Account != MockAccount {
		return local(ch, "account not kis_mock"), nil
	}
	if ch.IntentID != c.intent.IntentID {
		return local(ch, fmt.Sprintf("intent %q is not this client's", ch.IntentID)), nil
	}
	if c.mode == Off {
		return local(ch, "send disabled"), nil
	}
	body, err := json.Marshal(ch)
	if err != nil {
		return local(ch, err.Error()), nil
	}
	if c.mode == DryRun {
		return c.dryRun(ch, body)
	}
	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return local(ch, err.Error()), nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(c.header, c.token)
	req.Header.Set(ConfirmHeader, ch.Key)
	resp, err := c.http.Do(req)
	if err != nil {
		// Do wraps the cause in a *url.Error that names the URL. checkURL
		// keeps credentials out of the URL; the journal still gets only the
		// cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = fmt.Errorf("e1: post: %w", ue.Err)
		}
		return exec.Outcome{}, err // unknown: the request may have arrived
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes+1))
	if err != nil {
		return exec.Outcome{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return exec.Outcome{}, fmt.Errorf("e1: http %d", resp.StatusCode)
	}
	if len(data) > maxAnswerBytes {
		return exec.Outcome{}, errors.New("e1: answer too large")
	}
	o, err := ParseOutcome(data)
	if err != nil {
		return exec.Outcome{}, err
	}
	return o.ToExec(ch), nil
}

// dryRunLine is one request a dry run would have sent.
type dryRunLine struct {
	DryRun  bool              `json:"dry_run"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

func (c *Client) dryRun(ch exec.Child, body []byte) (exec.Outcome, error) {
	var line bytes.Buffer
	enc := json.NewEncoder(&line)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(dryRunLine{
		DryRun: true, Method: http.MethodPost, URL: c.url, Body: body,
		Headers: map[string]string{
			"Content-Type": "application/json",
			c.header:       "<not read in dry run>",
			ConfirmHeader:  ch.Key,
		},
	}); err != nil {
		return exec.Outcome{}, err
	}
	if _, err := c.log.Write(line.Bytes()); err != nil {
		return exec.Outcome{}, err
	}
	if c.answer == nil {
		return exec.Outcome{}, ErrNoAnswer
	}
	o, err := c.answer(ch)
	if err != nil {
		return exec.Outcome{}, err
	}
	return o.ToExec(ch), nil
}

// Query implements exec.Gateway using the authenticated intent read route.
// An unresolved or missing view is never converted to an empty order.
func (c *Client) Query(intentID string) (exec.View, error) { return c.query(intentID) }

// Outcome is E1's GatewayOutcome (gateway.py) as JSON.
type Outcome struct {
	Status             string          `json:"status"`
	Reason             *string         `json:"reason"`
	ChildID            *int64          `json:"child_id"`
	BrokerOrderID      *string         `json:"broker_order_id"`
	Result             json.RawMessage `json:"result"`
	Replayed           bool            `json:"replayed"`
	OutcomeNotRecorded bool            `json:"outcome_not_recorded"`
}

// ParseOutcome decodes one E1 outcome and refuses unknown fields, so a
// changed contract is an unknown outcome, not a guess.
func ParseOutcome(data []byte) (Outcome, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var o Outcome
	if err := dec.Decode(&o); err != nil {
		return Outcome{}, fmt.Errorf("e1: outcome: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Outcome{}, errors.New("e1: outcome: trailing data")
	}
	if o.Status == "" {
		return Outcome{}, errors.New("e1: outcome: no status")
	}
	return o, nil
}

// ToExec maps E1's outcome for child ch to the executor's. Only an accepted
// outcome that carries what the executor needs stays accepted; refused stays
// refused; everything else, known or not, is unknown and waits for
// reconciliation. Nothing in an outcome is a fill.
func (o Outcome) ToExec(ch exec.Child) exec.Outcome {
	out := exec.Outcome{Key: ch.Key, Status: exec.StatusUncertain}
	if o.Reason != nil {
		out.Reason = *o.Reason
	}
	if o.BrokerOrderID != nil {
		out.BrokerOrderID = strings.TrimSpace(*o.BrokerOrderID)
	}
	switch o.Status {
	case exec.StatusAccepted:
		if out.BrokerOrderID != "" || ch.Action == exec.ActCancel {
			out.Status = exec.StatusAccepted
		} else {
			out.Reason = "e1 accepted without broker_order_id"
		}
	case exec.StatusRefused:
		out.Status = exec.StatusRefused
	case exec.StatusUncertain, exec.StatusSending, exec.StatusConflict:
		out.Status = o.Status
	default:
		out.Reason = fmt.Sprintf("e1 status %q", o.Status)
	}
	return out
}
