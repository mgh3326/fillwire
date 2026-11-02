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

func loadConfig(path string, dryRun bool) (e1.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return e1.Config{}, err
	}
	var fc fileConfig
	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return e1.Config{}, fmt.Errorf("%s: %w", path, err)
	}
	cfg := e1.Config{
		Account: fc.E1.Account, SendEnabled: fc.E1.SendEnabled, DryRun: dryRun,
		URL: fc.E1.URL, TokenHeader: fc.E1.TokenHeader,
	}
	if fc.E1.Timeout != "" {
		if cfg.Timeout, err = time.ParseDuration(fc.E1.Timeout); err != nil || cfg.Timeout <= 0 {
			return e1.Config{}, fmt.Errorf("%s: e1.timeout %q", path, fc.E1.Timeout)
		}
	}
	return cfg, nil
}

type liveRunner func(string, e1.Config, exec.Config, string, string, time.Duration, io.Writer) error

func run(args []string, stdout, stderr io.Writer) int {
	return runWithLive(args, stdout, stderr, liveRun)
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
		return 2
	}
	if *configPath == "" || *intentPath == "" || fs.NArg() > 0 || ecfg.StepEvery <= 0 || ecfg.StaleAfter <= 0 {
		fmt.Fprintln(stderr, "usage: execmock -config FILE -intent INTENT {-in EVIDENCE -dry-run | -live-book {-dry-run | -send -journal FILE}}")
		return 2
	}
	cfg, err := loadConfig(*configPath, *dryRun)
	if err != nil {
		fmt.Fprintln(stderr, "execmock:", err)
		return 2
	}
	mode, err := cfg.Mode()
	if err != nil {
		fmt.Fprintln(stderr, "execmock:", err)
		return 2
	}
	switch mode {
	case e1.Off:
		if *send {
			fmt.Fprintln(stderr, "execmock: -send also requires e1.send_enabled=true")
			return 2
		}
		fmt.Fprintln(stderr, "execmock: e1.send_enabled is not set: nothing sent")
		return 0
	case e1.Send:
		if !*send || !*live || *in != "" {
			fmt.Fprintln(stderr, "execmock: send requires -send and -live-book; recorded evidence cannot send")
			return 2
		}
	}
	if *live {
		if *in != "" || *duration <= 0 {
			fmt.Fprintln(stderr, "execmock: -live-book conflicts with -in and needs a positive duration")
			return 2
		}
		if err := runLive(*configPath, cfg, ecfg, *intentPath, *journal, *duration, stdout); err != nil {
			fmt.Fprintln(stderr, "execmock:", err)
			return 1
		}
		return 0
	}
	if *in == "" {
		fmt.Fprintln(stderr, "execmock: -dry-run needs -in EVIDENCE")
		return 2
	}
	if err := dryRunReplay(cfg, ecfg, *intentPath, *in, sim.Rule(*rule), stdout); err != nil {
		fmt.Fprintln(stderr, "execmock:", err)
		return 1
	}
	return 0
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
