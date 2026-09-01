// kis-mock-edge accepts one loopback-only, mock-VTS order command boundary.
package main

import (
	"context"
	"os"

	"github.com/mgh3326/broker-edge/internal/kismockedge"
)

func main() {
	os.Exit(kismockedge.Run(context.Background(), os.Getenv, os.Stderr))
}
