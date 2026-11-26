package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/bookreccmd"
	"github.com/mgh3326/fillwire/internal/canary"
	"github.com/mgh3326/fillwire/internal/execmockcmd"
	"github.com/mgh3326/fillwire/internal/execsimcmd"
	"github.com/mgh3326/fillwire/internal/execupbitcmd"
	"github.com/mgh3326/fillwire/internal/gatewayd"
	"github.com/mgh3326/fillwire/internal/kismockedge"
	"github.com/mgh3326/fillwire/internal/kismockread"
	"github.com/mgh3326/fillwire/internal/kiwoomws"
	"github.com/mgh3326/fillwire/internal/markettapcmd"
)

const usage = `사용법: fillwire <명령> [옵션]
명령: run (별칭 fillwire), serve, upbit, upbit-fills, alpaca, nhplug-ws, coverage, bookrec, edge-canary,
      execmock, execsim, execupbit, gatewayd, kis-mock-edge, kis-mock-read, market-tap,
      kiwoom-kr, kiwoom-us
`

func wantsHelp(args []string) bool {
	return len(args) > 0 && (args[0] == "--help" || args[0] == "-help" || args[0] == "-h")
}

func dispatch(args []string) int {
	if wantsHelp(args) {
		fmt.Fprint(os.Stdout, usage)
		return runCLI(args)
	}
	if len(args) == 0 {
		return runCLI(args)
	}
	command, options := args[0], args[1:]
	if wantsHelp(options) {
		switch command {
		case "bookrec":
			bookreccmd.Run(nil, os.Stdin, os.Stdout, os.Stdout, time.Now)
			return 0
		case "edge-canary":
			fmt.Fprintln(os.Stdout, "사용법: fillwire edge-canary (환경 변수로 설정)")
			return 0
		case "gatewayd":
			fmt.Fprintln(os.Stdout, "사용법: fillwire gatewayd [-providers LIST] [-ensure-interval DURATION]")
			return 0
		case "kis-mock-edge":
			fmt.Fprintln(os.Stdout, "사용법: fillwire kis-mock-edge [resolve --help]")
			return 0
		case "kis-mock-read":
			fmt.Fprintln(os.Stdout, "사용법: fillwire kis-mock-read {domestic-balance|overseas-balance|domestic-order-history|overseas-order-history} [옵션]")
			return 0
		}
	}
	var code int
	switch command {
	case "run", "fillwire", "upbit", "coverage":
		if command == "run" || command == "fillwire" {
			return runCLI(options)
		}
		code = runCLI(args)
	case "alpaca":
		return runAlpacaCLI(options)
	case "nhplug-ws":
		if err := runNHPlugWSCommand(options); err != nil && !errors.Is(err, flag.ErrHelp) && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	case "upbit-fills":
		if err := runUpbitFillsCommand(options); err != nil && !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	case "kiwoom-kr":
		if err := runKiwoomCommand(kiwoomws.AccountKR, options); err != nil && !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	case "kiwoom-us":
		if err := runKiwoomCommand(kiwoomws.AccountUS, options); err != nil && !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	case "serve":
		return runServeCLI(options)
	case "bookrec":
		code = bookreccmd.Run(options, os.Stdin, os.Stdout, os.Stderr, time.Now)
	case "edge-canary":
		return canary.Run(context.Background(), canary.Options{})
	case "execmock":
		code = execmockcmd.Run(options, os.Stdout, os.Stderr)
	case "execsim":
		code = execsimcmd.Run(options, os.Stdout, os.Stderr)
	case "execupbit":
		code = execupbitcmd.Run(options, os.Stdout, os.Stderr)
	case "gatewayd":
		return gatewayd.Run(context.Background(), options, os.Getenv, os.Stderr)
	case "kis-mock-edge":
		return kismockedge.RunCLI(context.Background(), options, os.Getenv, os.Stdout, os.Stderr)
	case "kis-mock-read":
		return kismockread.RunCLI(context.Background(), options, os.Getenv, os.Stdout, os.Stderr)
	case "market-tap":
		code = markettapcmd.Run(options, os.Stdout, os.Stderr)
	default:
		if strings.HasPrefix(command, "-") {
			return runCLI(args)
		}
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	if wantsHelp(options) {
		return 0
	}
	return code
}
