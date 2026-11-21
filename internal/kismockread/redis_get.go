package kismockread

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	redisDialTimeout = 3 * time.Second
	redisReplyLimit  = 128 * 1024
)

// RedisGETClient is a deliberately narrow RESP client. Its only cache data
// operation is GET; optional AUTH and SELECT are connection setup commands.
type RedisGETClient struct {
	scheme   string
	address  string
	server   string
	username string
	password string
	database int
}

// NewRedisGETClient parses a standard redis:// or rediss:// URL without ever
// including the URL or its credentials in an error.
func NewRedisGETClient(rawURL string) (*RedisGETClient, *SafeError) {
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") ||
		parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, safeError(CodeTokenCacheUnavailable)
	}

	host := parsed.Hostname()
	if host == "" {
		return nil, safeError(CodeTokenCacheUnavailable)
	}
	port := parsed.Port()
	if port == "" {
		port = "6379"
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, safeError(CodeTokenCacheUnavailable)
	}

	database, err := parseRedisDatabase(parsed.Path)
	if err != nil {
		return nil, safeError(CodeTokenCacheUnavailable)
	}

	client := &RedisGETClient{
		scheme:   parsed.Scheme,
		address:  net.JoinHostPort(host, port),
		server:   host,
		database: database,
	}
	if parsed.User != nil {
		password, hasPassword := parsed.User.Password()
		if parsed.User.Username() != "" && !hasPassword {
			return nil, safeError(CodeTokenCacheUnavailable)
		}
		client.username = parsed.User.Username()
		client.password = password
	}
	return client, nil
}

func parseRedisDatabase(path string) (int, error) {
	if path == "" || path == "/" {
		return 0, nil
	}
	if !strings.HasPrefix(path, "/") || strings.Count(path, "/") != 1 {
		return 0, fmt.Errorf("invalid database path")
	}
	database, err := strconv.Atoi(strings.TrimPrefix(path, "/"))
	if err != nil || database < 0 || database > 65535 {
		return 0, fmt.Errorf("invalid database")
	}
	return database, nil
}

// Get reads a single key. All error paths collapse to a safe code at this
// boundary so Redis protocol details cannot escape to CLI output.
func (client *RedisGETClient) Get(ctx context.Context, key string) (string, bool, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return "", false, safeError(CodeTokenCacheUnavailable)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(redisDialTimeout))

	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)
	if client.password != "" {
		arguments := []string{"AUTH"}
		if client.username != "" {
			arguments = append(arguments, client.username)
		}
		arguments = append(arguments, client.password)
		if err := writeRESPArray(writer, arguments...); err != nil || readRESPOK(reader) != nil {
			return "", false, safeError(CodeTokenCacheUnavailable)
		}
	}
	if client.database != 0 {
		if err := writeRESPArray(writer, "SELECT", strconv.Itoa(client.database)); err != nil || readRESPOK(reader) != nil {
			return "", false, safeError(CodeTokenCacheUnavailable)
		}
	}
	if err := writeRESPArray(writer, "GET", key); err != nil {
		return "", false, safeError(CodeTokenCacheUnavailable)
	}
	value, present, err := readRESPBulkString(reader)
	if err != nil {
		return "", false, safeError(CodeTokenCacheUnavailable)
	}
	return value, present, nil
}

func (client *RedisGETClient) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: redisDialTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", client.address)
	if err != nil || client.scheme != "rediss" {
		return connection, err
	}
	tlsConnection := tls.Client(connection, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: client.server,
	})
	if err := tlsConnection.HandshakeContext(ctx); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return tlsConnection, nil
}

func writeRESPArray(writer *bufio.Writer, values ...string) error {
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(value), value); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func readRESPOK(reader *bufio.Reader) error {
	marker, err := reader.ReadByte()
	if err != nil || marker != '+' {
		return fmt.Errorf("unexpected redis status")
	}
	line, err := readRESPLine(reader)
	if err != nil || line != "OK" {
		return fmt.Errorf("redis status failure")
	}
	return nil
}

func readRESPBulkString(reader *bufio.Reader) (string, bool, error) {
	marker, err := reader.ReadByte()
	if err != nil || marker != '$' {
		return "", false, fmt.Errorf("unexpected redis bulk reply")
	}
	lengthLine, err := readRESPLine(reader)
	if err != nil {
		return "", false, err
	}
	length, err := strconv.Atoi(lengthLine)
	if err != nil {
		return "", false, err
	}
	if length == -1 {
		return "", false, nil
	}
	if length < 0 || length > redisReplyLimit {
		return "", false, fmt.Errorf("invalid redis bulk length")
	}
	payload := make([]byte, length+2)
	if _, err := io.ReadFull(reader, payload); err != nil ||
		payload[length] != '\r' || payload[length+1] != '\n' {
		return "", false, fmt.Errorf("invalid redis payload")
	}
	return string(payload[:length]), true, nil
}

func readRESPLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil || len(line) < 2 || len(line) > redisReplyLimit || !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("invalid redis line")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}
