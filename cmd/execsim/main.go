// Command execsim replays recorded book evidence (bookrec record) through the
// E2 executor and prints, for every intent, the fill quantity, average price
// and slippage under the conservative (through) and optimistic (touch) fill
// rules, one JSON object per line.
//
//	execsim -in EVIDENCE -intents INTENTS [-step 5s] [-stale 2s]
//
// INTENTS holds one E1 ExecutionIntentV1 JSON object per line. execsim sends
// to an in-process fake gateway only; it has no network client.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/sim"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("execsim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	in := fs.String("in", "", "book evidence file")
	intentsPath := fs.String("intents", "", "intents file, one E1 intent JSON per line")
	cfg := exec.DefaultConfig()
	fs.DurationVar(&cfg.StepEvery, "step", cfg.StepEvery, "time at one price before stepping a tick")
	fs.DurationVar(&cfg.StaleAfter, "stale", cfg.StaleAfter, "oldest book a send may be priced from")
	if err := fs.Parse(args); err != nil {
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
