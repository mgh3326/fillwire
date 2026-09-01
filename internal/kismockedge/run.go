package kismockedge

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListenAddress = "127.0.0.1:8080"
	defaultSQLitePath    = "kis-mock-edge.sqlite"
)

// ServerConfig contains only local listener and SQLite settings. KIS
// credentials are loaded lazily after the mock placement gate is enabled.
type ServerConfig struct {
	ListenAddress string
	SQLitePath    string
}

// ServerConfigFromEnv permits only loopback listener addresses. It does not
// accept an environment override that could accidentally expose this
// unauthenticated boundary to a network.
func ServerConfigFromEnv(lookup func(string) string) (ServerConfig, error) {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config := ServerConfig{
		ListenAddress: strings.TrimSpace(lookup("BROKER_EDGE_LISTEN_ADDR")),
		SQLitePath:    strings.TrimSpace(lookup("BROKER_EDGE_SQLITE_PATH")),
	}
	if config.ListenAddress == "" {
		config.ListenAddress = defaultListenAddress
	}
	if config.SQLitePath == "" {
		config.SQLitePath = defaultSQLitePath
	}
	if !loopbackAddress(config.ListenAddress) {
		return ServerConfig{}, errors.New("listener must be loopback")
	}
	return config, nil
}

func loopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Run serves the local command edge until ctx is cancelled. Startup failures
// are deliberately rendered without filesystem, credential, or network detail.
func Run(ctx context.Context, lookup func(string) string, stderr io.Writer) int {
	config, err := ServerConfigFromEnv(lookup)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	store, err := OpenStore(config.SQLitePath)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	defer store.Close()
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	server := &http.Server{
		Handler:           NewHandler(NewEnvironmentService(store, lookup, nil)),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	select {
	case err := <-serveResult:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			writeStartupError(stderr)
			return 1
		}
		return 0
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
		<-serveResult
		return 0
	}
}

func writeStartupError(stderr io.Writer) {
	if stderr != nil {
		_, _ = io.WriteString(stderr, "kis-mock-edge: startup_failed\n")
	}
}
