// Package main reserves the market-tap command name for a later, separately
// approved boundary. It intentionally contains no implementation.
package main

// main is empty: the package only reserves the command name, and this lets
// `go build ./...` link every command in the module (the placeholder had no
// main, so that build failed in broker-edge as well).
func main() {}
