package gatewayd

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
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

var errRedisUnavailable = errors.New("redis unavailable")

// RedisStore is the narrow capability set required by the issuer. The
// read-only KIS paths retain their separate GET-only client.
type RedisStore interface {
	Get(ctx context.Context, key string) (value string, present bool, err error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	CompareAndDelete(ctx context.Context, key, value string) error
}

// RedisClient is a small RESP client for the issuer's GET, SET, SET NX EX,
// and ownership-checked lock release. It never includes the Redis URL or a
// command payload in an error.
type RedisClient struct {
	scheme   string
	address  string
	server   string
	username string
	password string
	database int
}

// NewRedisClient parses redis:// and rediss:// URLs without allowing query or
// fragment options that could widen its behavior.
func NewRedisClient(rawURL string) (*RedisClient, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") ||
		parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errRedisUnavailable
	}
	host := parsed.Hostname()
	if host == "" {
		return nil, errRedisUnavailable
	}
	port := parsed.Port()
	if port == "" {
		port = "6379"
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, errRedisUnavailable
	}
	database, err := parseRedisDatabase(parsed.Path)
	if err != nil {
		return nil, errRedisUnavailable
	}
	client := &RedisClient{
		scheme:   parsed.Scheme,
		address:  net.JoinHostPort(host, port),
		server:   host,
		database: database,
	}
	if parsed.User != nil {
		password, hasPassword := parsed.User.Password()
		if parsed.User.Username() != "" && !hasPassword {
			return nil, errRedisUnavailable
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
		return 0, errors.New("invalid redis database")
	}
	database, err := strconv.Atoi(strings.TrimPrefix(path, "/"))
	if err != nil || database < 0 || database > 65535 {
		return 0, errors.New("invalid redis database")
	}
	return database, nil
}

// Get loads one cache value. Missing values are not an error.
func (client *RedisClient) Get(ctx context.Context, key string) (string, bool, error) {
	reply, err := client.execute(ctx, "GET", key)
	if err != nil {
		return "", false, errRedisUnavailable
	}
	if reply.nilValue {
		return "", false, nil
	}
	if reply.kind != '$' {
		return "", false, errRedisUnavailable
	}
	return reply.text, true, nil
}

// Set stores a cache payload with an EX TTL measured in whole seconds.
func (client *RedisClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	seconds, valid := redisTTLSeconds(ttl)
	if !valid {
		return errRedisUnavailable
	}
	reply, err := client.execute(ctx, "SET", key, value, "EX", seconds)
	if err != nil || reply.kind != '+' || reply.text != "OK" {
		return errRedisUnavailable
	}
	return nil
}

// SetNX atomically takes a lock with the required EX TTL.
func (client *RedisClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	seconds, valid := redisTTLSeconds(ttl)
	if !valid {
		return false, errRedisUnavailable
	}
	reply, err := client.execute(ctx, "SET", key, value, "NX", "EX", seconds)
	if err != nil {
		return false, errRedisUnavailable
	}
	if reply.nilValue {
		return false, nil
	}
	if reply.kind == '+' && reply.text == "OK" {
		return true, nil
	}
	return false, errRedisUnavailable
}

// CompareAndDelete removes a lock only if this process still owns its opaque
// value. This matches the Python manager's Lua release semantics.
func (client *RedisClient) CompareAndDelete(ctx context.Context, key, value string) error {
	const releaseScript = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
	reply, err := client.execute(ctx, "EVAL", releaseScript, "1", key, value)
	if err != nil || reply.kind != ':' {
		return errRedisUnavailable
	}
	return nil
}

func redisTTLSeconds(ttl time.Duration) (string, bool) {
	if ttl <= 0 || ttl%time.Second != 0 {
		return "", false
	}
	seconds := int64(ttl / time.Second)
	if seconds <= 0 {
		return "", false
	}
	return strconv.FormatInt(seconds, 10), true
}

func (client *RedisClient) execute(ctx context.Context, values ...string) (redisReply, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return redisReply{}, errRedisUnavailable
	}
	defer connection.Close()
	if err := setRedisDeadline(connection, ctx); err != nil {
		return redisReply{}, errRedisUnavailable
	}
	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)
	if client.password != "" {
		arguments := []string{"AUTH"}
		if client.username != "" {
			arguments = append(arguments, client.username)
		}
		arguments = append(arguments, client.password)
		if err := writeRESPArray(writer, arguments...); err != nil {
			return redisReply{}, errRedisUnavailable
		}
		reply, err := readRESPReply(reader)
		if err != nil || reply.kind != '+' || reply.text != "OK" {
			return redisReply{}, errRedisUnavailable
		}
	}
	if client.database != 0 {
		if err := writeRESPArray(writer, "SELECT", strconv.Itoa(client.database)); err != nil {
			return redisReply{}, errRedisUnavailable
		}
		reply, err := readRESPReply(reader)
		if err != nil || reply.kind != '+' || reply.text != "OK" {
			return redisReply{}, errRedisUnavailable
		}
	}
	if err := writeRESPArray(writer, values...); err != nil {
		return redisReply{}, errRedisUnavailable
	}
	reply, err := readRESPReply(reader)
	if err != nil {
		return redisReply{}, errRedisUnavailable
	}
	return reply, nil
}

func (client *RedisClient) dial(ctx context.Context) (net.Conn, error) {
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

func setRedisDeadline(connection net.Conn, ctx context.Context) error {
	deadline := time.Now().Add(redisDialTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return connection.SetDeadline(deadline)
}

func writeRESPArray(writer *bufio.Writer, values ...string) error {
	if len(values) == 0 || len(values) > 8 {
		return errors.New("invalid redis command")
	}
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if len(value) > redisReplyLimit {
			return errors.New("redis command too large")
		}
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(value), value); err != nil {
			return err
		}
	}
	return writer.Flush()
}

type redisReply struct {
	kind     byte
	text     string
	nilValue bool
}

func readRESPReply(reader *bufio.Reader) (redisReply, error) {
	marker, err := reader.ReadByte()
	if err != nil {
		return redisReply{}, err
	}
	switch marker {
	case '+':
		line, err := readRESPLine(reader)
		return redisReply{kind: marker, text: line}, err
	case '-':
		_, err := readRESPLine(reader)
		if err != nil {
			return redisReply{}, err
		}
		return redisReply{}, errors.New("redis error reply")
	case ':':
		line, err := readRESPLine(reader)
		if err != nil {
			return redisReply{}, err
		}
		if _, err := strconv.ParseInt(line, 10, 64); err != nil {
			return redisReply{}, err
		}
		return redisReply{kind: marker, text: line}, nil
	case '$':
		lengthLine, err := readRESPLine(reader)
		if err != nil {
			return redisReply{}, err
		}
		length, err := strconv.Atoi(lengthLine)
		if err != nil {
			return redisReply{}, err
		}
		if length == -1 {
			return redisReply{kind: marker, nilValue: true}, nil
		}
		if length < 0 || length > redisReplyLimit {
			return redisReply{}, errors.New("invalid redis bulk length")
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil || payload[length] != '\r' || payload[length+1] != '\n' {
			return redisReply{}, errors.New("invalid redis bulk payload")
		}
		return redisReply{kind: marker, text: string(payload[:length])}, nil
	case '_':
		line, err := readRESPLine(reader)
		if err != nil || line != "" {
			return redisReply{}, errors.New("invalid redis null")
		}
		return redisReply{kind: marker, nilValue: true}, nil
	default:
		return redisReply{}, errors.New("unexpected redis reply")
	}
}

func readRESPLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil || len(line) < 2 || len(line) > redisReplyLimit || !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("invalid redis line")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}
