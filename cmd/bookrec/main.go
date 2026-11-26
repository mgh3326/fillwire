package main

import (
	"os"
	"time"

	"github.com/mgh3326/fillwire/internal/bookreccmd"
)

func main() {
	os.Exit(bookreccmd.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, time.Now))
}
