package main

import (
	"os"

	"github.com/mgh3326/fillwire/internal/execsimcmd"
)

func main() {
	os.Exit(execsimcmd.Run(os.Args[1:], os.Stdout, os.Stderr))
}
