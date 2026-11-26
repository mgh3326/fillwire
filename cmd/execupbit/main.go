package main

import (
	"os"

	"github.com/mgh3326/fillwire/internal/execupbitcmd"
)

func main() {
	os.Exit(execupbitcmd.Run(os.Args[1:], os.Stdout, os.Stderr))
}
