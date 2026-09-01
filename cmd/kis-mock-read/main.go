// kis-mock-read performs a bounded set of read-only KIS VTS requests.
package main

import (
	"context"
	"os"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func main() {
	os.Exit(kismockread.RunCLI(
		context.Background(),
		os.Args[1:],
		os.Getenv,
		os.Stdout,
		os.Stderr,
	))
}
