// kis-mock-edge accepts one loopback-only, mock-VTS order command boundary.
package main

import (
	"context"
	"os"

	"github.com/mgh3326/broker-edge/internal/kismockedge"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "resolve" {
		os.Exit(kismockedge.RunResolve(context.Background(), os.Args[2:], os.Getenv, os.Stdout, os.Stderr))
	}
	os.Exit(kismockedge.Run(context.Background(), os.Getenv, os.Stderr))
}
