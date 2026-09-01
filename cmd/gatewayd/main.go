// gatewayd is the loopback-only, provider-scoped OAuth token issuer.
package main

import (
	"context"
	"os"

	"github.com/mgh3326/broker-edge/internal/gatewayd"
)

func main() {
	os.Exit(gatewayd.Run(context.Background(), os.Args[1:], os.Getenv, os.Stderr))
}
