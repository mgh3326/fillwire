// edge-canary proves the mock order path with one bounded far-below limit order.
package main

import (
	"context"
	"os"

	"github.com/mgh3326/broker-edge/internal/canary"
)

func main() { os.Exit(canary.Run(context.Background(), canary.Options{})) }
