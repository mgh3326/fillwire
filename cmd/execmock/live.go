package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
	"github.com/mgh3326/fillwire/internal/kisread"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

const liveReadTimeout = 5 * time.Second

type ledgerRead func(context.Context) (e1.FillsSnapshot, error)

func refreshLedger(ctx context.Context, e *exec.Executor, read ledgerRead, broker brokerRead) error {
	ledgerCtx, cancelLedger := context.WithTimeout(ctx, liveReadTimeout)
	s, err := read(ledgerCtx)
	if ledgerCtx.Err() != nil {
		err = ledgerCtx.Err()
	}
	cancelLedger()
	if err != nil {
		return err
	}
	st := e.State()
	if st.InFlight != nil {
		return exec.ErrIncomplete
	}
	if s.IntentID != st.Intent.IntentID {
		return exec.ErrAttribution
	}
	orders, err := e.BrokerOrders()
	if err != nil {
		return err
	}
	if len(orders) != len(s.Orders) {
		return fmt.Errorf("%w: ledger/journal order set differs", exec.ErrAttribution)
	}
	if st.Order != nil {
		matched := false
		for _, o := range orders {
			matched = matched || o.Key == st.Order.Key
		}
		if !matched {
			return fmt.Errorf("%w: working order has no journal broker identity", exec.ErrAttribution)
		}
	}
	expected := map[string]exec.BrokerOrder{}
	for _, o := range orders {
		if _, dup := expected[o.BrokerOrderID]; dup {
			return exec.ErrAttribution
		}
		expected[o.BrokerOrderID] = o
	}
	seen := map[string]bool{}
	seenOrders := map[string]bool{}
	complete := true
	for _, o := range s.Orders {
		number := strings.TrimLeft(o.BrokerOrderID, "0")
		known, ok := expected[number]
		if !ok || seenOrders[number] {
			return exec.ErrAttribution
		}
		seenOrders[number] = true
		var total int64
		for _, f := range o.Fills {
			if f.Side != st.Intent.Side || seen[f.FillID] {
				return exec.ErrAttribution
			}
			seen[f.FillID] = true
			fill, err := f.Execution()
			if err != nil {
				return err
			}
			if err := e.OnLedgerFill(number, fill); err != nil {
				return err
			}
			total += fill.Qty
		}
		if total != o.CumulativeQty {
			return exec.ErrAttribution
		}
		// Each exact order owns its wait/transport budget. Pacing earlier
		// reads must not consume the budget of later unverified orders.
		orderCtx, cancelOrder := context.WithTimeout(ctx, liveReadTimeout)
		view, err := broker(orderCtx, known)
		if orderCtx.Err() != nil {
			err = orderCtx.Err()
		}
		cancelOrder()
		if err != nil {
			return err
		}
		if view.Filled != o.CumulativeQty {
			return fmt.Errorf("%w: exact broker order has unaccounted fills", exec.ErrIncomplete)
		}
		complete = complete && view.Terminal
	}
	// PR #2273 can upsert or supersede rows. Never add a replacement to already
	// counted evidence; a changed effective snapshot requires operator reconcile.
	for id := range st.Fills {
		if !seen[id] {
			return fmt.Errorf("%w: effective ledger fill removed or superseded", exec.ErrAttribution)
		}
	}
	e.SetLedgerComplete(complete)
	return nil
}

type liveInputs struct {
	Book   func(context.Context) (bookfeed.Snapshot, bool, error)
	Ledger ledgerRead
	Broker brokerRead
	Now    func() time.Time
	Ticks  <-chan time.Time
}

func driveLive(ctx context.Context, e *exec.Executor, kill *exec.KillSwitch, dry bool, until time.Time, in liveInputs) error {
	if !dry {
		in.Broker = cacheTerminalBrokerViews(in.Broker)
	}
	interrupted := errors.New("execmock: interrupted; verify terminal broker/ledger evidence before restarting")
	stopForReconcile := func(reason error) error {
		kill.Engage()
		if err := e.Tick(in.Now().UnixNano()); err != nil {
			return errors.Join(reason, err)
		}
		return reason
	}

	step := func() error {
		if !dry {
			if err := refreshLedger(ctx, e, in.Ledger, in.Broker); err != nil {
				return err
			}
		}
		bookCtx, cancelBook := context.WithTimeout(ctx, liveReadTimeout)
		s, ok, err := in.Book(bookCtx)
		if bookCtx.Err() != nil {
			err = bookCtx.Err()
		}
		cancelBook()
		if err != nil {
			return err
		}

		now := in.Now()
		if ok {
			if s.Symbol != e.State().Intent.Symbol {
				return errors.New("execmock: wrong book symbol")
			}
			e.OnBook(exec.Top{Bid: s.Bids[0].Price, Ask: s.Asks[0].Price, AtNS: s.At().UnixNano(), Synced: !s.Exchange.After(now) && !s.Received.After(now)})
		}
		return e.Tick(now.UnixNano())
	}
	for {
		now := in.Now()
		if dry && !now.Before(until) {
			kill.Engage()
			return e.Tick(now.UnixNano())
		}
		if err := step(); err != nil {
			if ctx.Err() != nil {
				return stopForReconcile(interrupted)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return stopForReconcile(fmt.Errorf("execmock: poll deadline exceeded; operator reconcile required: %w", err))
			}
			return err
		}
		if e.State().Done != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return stopForReconcile(interrupted)
		case _, ok := <-in.Ticks:
			if !ok {
				return errors.New("execmock: clock stopped")
			}
		}
	}
}

func liveRun(path string, cfg e1.Config, ecfg exec.Config, intentPath, journal string, duration time.Duration, stdout io.Writer) error {
	data, err := os.ReadFile(intentPath)
	if err != nil {
		return err
	}
	intent, err := exec.ParseIntent(bytes.TrimSpace(data))
	if err != nil {
		return err
	}
	if intent.Account != e1.MockAccount {
		return e1.ErrAccountNotMock
	}
	var fc fileConfig
	data, err = os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return err
	}
	options, err := redis.ParseURL(os.Getenv(fc.Redis.URLEnv))
	if err != nil {
		return errors.New("execmock: invalid redis.url_env")
	}
	host, _, splitErr := net.SplitHostPort(options.Addr)
	ip := net.ParseIP(host)
	if options.Network != "unix" && options.TLSConfig == nil && (splitErr != nil || host != "localhost" && (ip == nil || !ip.IsLoopback())) {
		return errors.New("execmock: Redis requires TLS outside loopback")
	}
	options.ReadTimeout = time.Second
	options.WriteTimeout = time.Second
	options.DialTimeout = time.Second
	r := redis.NewClient(options)
	defer r.Close()
	dry := cfg.DryRun
	if !dry && (journal == "" || fc.E1.TokenEnv == "" || !strings.HasSuffix(cfg.URL, "/execution-intents/children")) {
		return errors.New("execmock: send needs -journal, e1.token_env, and the /execution-intents/children route")
	}
	if dry {
		dir, err := os.MkdirTemp("", "execmock-live-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		journal = filepath.Join(dir, "dry.journal")
	}
	j, st, err := exec.OpenJournal(journal)
	if err != nil {
		return err
	}
	defer j.Close()
	if st.Started() && !reflect.DeepEqual(st.Intent, intent) {
		return errors.New("execmock: journal intent differs from supplied intent")
	}
	fake := exectest.New(intent)
	client, err := e1.New(cfg, intent, e1.Options{Log: stdout, Answer: fakeAnswer(fake), Token: func() (string, error) { return os.Getenv(fc.E1.TokenEnv), nil }})
	if err != nil {
		return err
	}
	var broker brokerRead
	if !dry {
		for _, name := range []string{fc.BrokerRead.AppKeyEnv, fc.BrokerRead.AppSecretEnv, fc.BrokerRead.TokenEnv, fc.BrokerRead.AccountEnv, fc.BrokerRead.ProductEnv} {
			if name == "" || os.Getenv(name) == "" {
				return errors.New("execmock: broker_read credential environment is incomplete")
			}
		}
		k := &kisread.Client{Mock: true, AppKey: os.Getenv(fc.BrokerRead.AppKeyEnv), AppSecret: os.Getenv(fc.BrokerRead.AppSecretEnv), Token: func() string { return os.Getenv(fc.BrokerRead.TokenEnv) }}

		broker = historyRead{Client: k, Account: os.Getenv(fc.BrokerRead.AccountEnv), Product: os.Getenv(fc.BrokerRead.ProductEnv), Symbol: intent.Symbol, Side: intent.Side}.read
	}
	ecfg.SendEnabled = true
	ecfg.RequireLedgerComplete = !dry
	kill := &exec.KillSwitch{}
	e := exec.New(ecfg, j, st, client, kill)
	now := time.Now()
	if !st.Started() {
		if err := e.Start(intent, now.UnixNano()); err != nil {
			return err
		}
	}
	if st.Done != "" && !dry {
		if err := refreshLedger(context.Background(), e, client.Fills, broker); err != nil {
			return err
		}
		return errors.New("execmock: journal already finished; inspect it before another run")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	return driveLive(ctx, e, kill, dry, now.Add(duration), liveInputs{Book: func(ctx context.Context) (bookfeed.Snapshot, bool, error) {
		return bookfeed.Latest(ctx, r, intent.Symbol)
	}, Ledger: client.Fills, Broker: broker, Now: time.Now, Ticks: timer.C})
}
