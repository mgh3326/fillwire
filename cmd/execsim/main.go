// Command execsim replays recorded book evidence (bookrec record) through the
// E2 executor and prints, for every intent, the fill quantity, average price
// and slippage under the conservative (through) and optimistic (touch) fill
// rules, one JSON object per line.
//
//	execsim -in EVIDENCE -intents INTENTS [-step 5s] [-stale 2s]
//
// INTENTS holds one E1 ExecutionIntentV1 JSON object per line. execsim sends
// to an in-process fake gateway only; it has no network client.
//
// With -venue upbit it replays the recorder's Upbit frames (fillwire upbit)
// instead, for the shadow intents of internal/exec/upbit, under the fill
// rules of auto_trader docs/design/5852-upbit-krw-shadow-strategy.md §7.2:
//
//	execsim -venue upbit -intents INTENTS -books FILE [-books FILE ...] [-stale 2s]
//
// INTENTS holds one Upbit replay intent JSON object per line and each FILE is
// one recorder file (.jsonl, or .jsonl.gz for a closed day) in time order. The
// output is one JSON line per child and one result line per run, through and
// touch counted separately. Nothing is sent: there is no order code at all.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/sim"
	"github.com/mgh3326/fillwire/internal/exec/upbit"
)

// stringList is a flag that may repeat.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

const upbitUsage = "usage: execsim -venue upbit -intents INTENTS -books FILE [-books FILE ...] [-stale 2s]"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("execsim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	venue := fs.String("venue", "kis", "kis (E2 executor over bookrec evidence) or upbit (recorded Upbit frames)")
	in := fs.String("in", "", "book evidence file")
	var books stringList
	fs.Var(&books, "books", "recorder file of Upbit frames, in time order (-venue upbit; repeatable)")
	intentsPath := fs.String("intents", "", "intents file, one E1 intent JSON per line")
	cfg := exec.DefaultConfig()
	fs.DurationVar(&cfg.StepEvery, "step", cfg.StepEvery, "time at one price before stepping a tick")
	fs.DurationVar(&cfg.StaleAfter, "stale", cfg.StaleAfter, "oldest book a send may be priced from")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *venue == "upbit" {
		return runUpbit(fs, *intentsPath, books, *in != "", cfg, stdout, stderr)
	}
	if *venue != "kis" || len(books) > 0 {
		fmt.Fprintln(stderr, "execsim: -venue must be kis or upbit, and -books is for -venue upbit only")
		return 2
	}
	if *in == "" || *intentsPath == "" || fs.NArg() > 0 || cfg.StepEvery <= 0 || cfg.StaleAfter <= 0 {
		fmt.Fprintln(stderr, "usage: execsim -in EVIDENCE -intents INTENTS [-step 5s] [-stale 2s]")
		return 2
	}
	// The fake gateway is the only destination; sending to it is the point.
	cfg.SendEnabled = true
	if err := simulate(*in, *intentsPath, cfg, stdout); err != nil {
		fmt.Fprintln(stderr, "execsim:", err)
		return 1
	}
	return 0
}

func simulate(evidencePath, intentsPath string, cfg exec.Config, stdout io.Writer) error {
	evidence, err := os.ReadFile(evidencePath)
	if err != nil {
		return err
	}
	intents, err := readIntents(intentsPath)
	if err != nil {
		return err
	}
	byRule := map[sim.Rule][]sim.Result{}
	for _, rule := range sim.Rules {
		if byRule[rule], err = sim.Run(evidence, intents, cfg, rule); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(stdout)
	for i := range intents {
		for _, rule := range sim.Rules {
			if err := enc.Encode(byRule[rule][i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func readIntents(path string) ([]exec.Intent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []exec.Intent
	sc := bufio.NewScanner(bytes.NewReader(data))
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		in, err := exec.ParseIntent(line)
		if err != nil {
			return nil, fmt.Errorf("intents line %d: %w", n, err)
		}
		out = append(out, in)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no intents", path)
	}
	return out, nil
}

// runUpbit is the -venue upbit mode.
func runUpbit(fs *flag.FlagSet, intentsPath string, books []string, haveIn bool, cfg exec.Config, stdout, stderr io.Writer) int {
	stepSet := false
	fs.Visit(func(f *flag.Flag) { stepSet = stepSet || f.Name == "step" })
	if intentsPath == "" || len(books) == 0 || haveIn || stepSet || fs.NArg() > 0 || cfg.StaleAfter <= 0 {
		fmt.Fprintln(stderr, upbitUsage)
		return 2
	}
	if err := simulateUpbit(intentsPath, books, upbit.Config{StaleAfter: cfg.StaleAfter}, stdout); err != nil {
		fmt.Fprintln(stderr, "execsim:", err)
		return 1
	}
	return 0
}

func simulateUpbit(intentsPath string, books []string, cfg upbit.Config, stdout io.Writer) error {
	data, err := os.ReadFile(intentsPath)
	if err != nil {
		return err
	}
	var intents []upbit.ReplayIntent
	seen := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		in, err := upbit.ParseReplayIntent(line)
		if err != nil {
			return fmt.Errorf("intents line %d: %w", n, err)
		}
		if seen[in.IntentID] {
			return fmt.Errorf("intents line %d: duplicate intent_id %s", n, in.IntentID)
		}
		seen[in.IntentID] = true
		intents = append(intents, in)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if len(intents) == 0 {
		return fmt.Errorf("%s: no intents", intentsPath)
	}
	replay, err := upbit.NewReplay(intents, cfg)
	if err != nil {
		return err
	}
	for _, path := range books {
		if err := upbit.ReadRecords(path, replay.Feed); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(stdout)
	for _, rep := range replay.Finish() {
		for _, c := range rep.Children {
			if err := enc.Encode(c); err != nil {
				return err
			}
		}
		if err := enc.Encode(rep.Result); err != nil {
			return err
		}
	}
	return nil
}
