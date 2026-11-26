// kis-mock-edge accepts one loopback-only, mock-VTS order command boundary.
package main

import (
	"context"
	"os"

	"github.com/mgh3326/fillwire/internal/kismockedge"
)

func main() {
	os.Exit(kismockedge.RunCLI(context.Background(), os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}
