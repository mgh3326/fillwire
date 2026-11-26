package main

import (
	"os"

	"github.com/mgh3326/fillwire/internal/execmockcmd"
)

func main() {
	os.Exit(execmockcmd.Run(os.Args[1:], os.Stdout, os.Stderr))
}
