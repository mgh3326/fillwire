// Command execmock works an approved KIS mock intent using recorded evidence
// or the live Redis book and committed E1 ledger fills. Sending requires both
// send_enabled=true and -send; -dry-run always prints and simulates requests.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
	"github.com/mgh3326/fillwire/internal/exec/sim"
	"github.com/mgh3326/fillwire/internal/exitcode"
	"github.com/pelletier/go-toml/v2"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type fileConfig struct {
	E1 struct {
		Account     string `toml:"account"`
		TokenEnv    string `toml:"token_env"`
		SendEnabled bool   `toml:"send_enabled"`
		URL         string `toml:"url"`
		TokenHeader string `toml:"token_header"`
		Timeout     string `toml:"timeout"`
	} `toml:"e1"`
	Redis struct {
		URLEnv string `toml:"url_env"`
	} `toml:"redis"`
	BrokerRead struct {
		AppKeyEnv    string `toml:"app_key_env"`
		AppSecretEnv string `toml:"app_secret_env"`
		TokenEnv     string `toml:"access_token_env"`
		AccountEnv   string `toml:"account_env"`
		ProductEnv   string `toml:"product_env"`
	} `toml:"broker_read"`
}

// configError marks a failure the operator fixes in the config file or the
// environment it names. It exits exitcode.Config, like fillwire.
type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

func configErrorf(format string, args ...any) error {
	return &configError{msg: fmt.Sprintf(format, args...)}
}

// loadConfig decodes the config file once. The [e1] table becomes the client
// config; the whole file is returned for the live-book path, which reads the
// [redis] and [broker_read] tables.
func loadConfig(path string, dryRun bool) (fileConfig, e1.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, e1.Config{}, configErrorf("%s: %v", path, err)
	}
	var fc fileConfig
	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return fileConfig{}, e1.Config{}, configErrorf("%s: %v", path, err)
	}
	cfg := e1.Config{
		Account: fc.E1.Account, SendEnabled: fc.E1.SendEnabled, DryRun: dryRun,
		URL: fc.E1.URL, TokenHeader: fc.E1.TokenHeader,
	}
	if fc.E1.Timeout != "" {
		if cfg.Timeout, err = time.ParseDuration(fc.E1.Timeout); err != nil || cfg.Timeout <= 0 {
			return fileConfig{}, e1.Config{}, configErrorf("%s: e1.timeout %q", path, fc.E1.Timeout)
		}
	}
	return fc, cfg, nil
}

type liveRunner func(fileConfig, e1.Config, exec.Config, string, string, time.Duration, io.Writer) error

func run(args []string, stdout, stderr io.Writer) int {
	return runWithLive(args, stdout, stderr, liveRun)
}

// fail prints err once, with the command prefix, and maps it to an exit code.
func fail(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, "execmock:", err)
	var ce *configError
	if errors.As(err, &ce) {
		return exitcode.Config
	}
	return exitcode.Failure
}

func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintln(stderr, "execmock:", msg)
	return exitcode.Usage
}

func runWithLive(args []string, stdout, stderr io.Writer, runLive liveRunner) int {
	fs := flag.NewFlagSet("execmock", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "config file with an [e1] table")
	intentPath := fs.String("intent", "", "one E1 ExecutionIntentV1 JSON object")
	in := fs.String("in", "", "book evidence file (bookrec record)")
	live := fs.Bool("live-book", false, "read book:kis:<intent symbol> from Redis")
	send := fs.Bool("send", false, "explicitly authorize configured mock sends")
	journal := fs.String("journal", "", "durable local journal (required for sending)")
	duration := fs.Duration("duration", time.Minute, "duration of a live-book dry run")
	dryRun := fs.Bool("dry-run", false, "print each request instead of sending it")
	rule := fs.String("rule", string(sim.Through), "dry-run fill rule: through or touch")
	ecfg := exec.DefaultConfig()
	fs.DurationVar(&ecfg.StepEvery, "step", ecfg.StepEvery, "time at one price before stepping a tick")
	fs.DurationVar(&ecfg.StaleAfter, "stale", ecfg.StaleAfter, "oldest book a send may be priced from")
	if err := fs.Parse(args); err != nil {
		return exitcode.Usage
	}
	if *configPath == "" || *intentPath == "" || fs.NArg() > 0 || ecfg.StepEvery <= 0 || ecfg.StaleAfter <= 0 {
		fmt.Fprintln(stderr, "usage: execmock -config FILE -intent INTENT {-in EVIDENCE -dry-run | -live-book {-dry-run | -send -journal FILE}}")
		return exitcode.Usage
	}
	if !sim.ValidRule(sim.Rule(*rule)) {
		return usageError(stderr, fmt.Sprintf("-rule %q: want through or touch", *rule))
	}
	fc, cfg, err := loadConfig(*configPath, *dryRun)
	if err != nil {
		return fail(stderr, err)
	}
	mode, err := cfg.Mode()
	if err != nil {
		return fail(stderr, &configError{msg: err.Error()})
	}
	switch mode {
	case e1.Off:
		if *send {
			return usageError(stderr, "-send also requires e1.send_enabled=true")
		}
		fmt.Fprintln(stderr, "execmock: e1.send_enabled is not set: nothing sent")
		return exitcode.OK
	case e1.Send:
		if !*send || !*live || *in != "" {
			return usageError(stderr, "send requires -send and -live-book; recorded evidence cannot send")
		}
	}
	if *live {
		if *in != "" || *duration <= 0 {
			return usageError(stderr, "-live-book conflicts with -in and needs a positive duration")
		}
		if err := runLive(fc, cfg, ecfg, *intentPath, *journal, *duration, stdout); err != nil {
			return fail(stderr, err)
		}
		return exitcode.OK
	}
	if *in == "" {
		return usageError(stderr, "-dry-run needs -in EVIDENCE")
	}
	if err := dryRunReplay(cfg, ecfg, *intentPath, *in, sim.Rule(*rule), stdout); err != nil {
		return fail(stderr, err)
	}
	return exitcode.OK
}

func dryRunReplay(cfg e1.Config, ecfg exec.Config, intentPath, evidencePath string, rule sim.Rule, stdout io.Writer) error {
	data, err := os.ReadFile(intentPath)
	if err != nil {
		return err
	}
	intent, err := exec.ParseIntent(bytes.TrimSpace(data))
	if err != nil {
		return err
	}
	evidence, err := os.ReadFile(evidencePath)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "execmock-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	ecfg.SendEnabled = cfg.ExecutorSends()
	res, err := sim.RunOne(evidence, intent, ecfg, rule, filepath.Join(dir, "exec.journal"),
		func(g *exectest.Gateway) (exec.Gateway, error) {
			return e1.New(cfg, intent, e1.Options{Log: stdout, Answer: fakeAnswer(g)})
		})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		Result sim.Result `json:"result"`
	}{res})
}

// fakeAnswer answers a dry-run request with the fake gateway's outcome, in
// E1's outcome shape.
func fakeAnswer(g *exectest.Gateway) func(exec.Child) (e1.Outcome, error) {
	return func(c exec.Child) (e1.Outcome, error) {
		o, err := g.Send(c)
		if err != nil {
			return e1.Outcome{}, err
		}
		if o.Key != c.Key {
			return e1.Outcome{}, errors.New("fake answered another key")
		}
		out := e1.Outcome{Status: o.Status}
		if o.Reason != "" {
			out.Reason = &o.Reason
		}
		if o.BrokerOrderID != "" {
			out.BrokerOrderID = &o.BrokerOrderID
		}
		return out, nil
	}
}
