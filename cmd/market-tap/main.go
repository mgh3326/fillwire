package main

import (
	"os"

	"github.com/mgh3326/fillwire/internal/markettapcmd"
)

func main() {
	os.Exit(markettapcmd.Run(os.Args[1:], os.Stdout, os.Stderr))
}
