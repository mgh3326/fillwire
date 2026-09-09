// Package sink delivers persisted fill records to the execution-ledger ingest
// endpoint and acknowledges only confirmed stream entries.
package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/mgh3326/fillwire/internal/stream"
)

const source = "fillwire"

// HTTPConfig configures the token-authenticated ingest client.
type HTTPConfig struct {
	URL     string
	Token   string
	Timeout time.Duration
}

// Client posts only the allowlisted ledger upsert structure.
type Client struct {
	url        string
	token      string
	httpClient *http.Client
}

// NewClient creates a client with a finite timeout and redirects disabled.
func NewClient(cfg HTTPConfig) (*Client, error) {
	if err := ValidateIngestURL(cfg.URL); err != nil {
		return nil, err
	}
	if cfg.Token == "" {
		return nil, errors.New("sink: ingest token is required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("sink: ingest timeout must be positive")
	}
	return &Client{
		url:   cfg.URL,
		token: cfg.Token,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// ValidateIngestURL rejects transport settings that could disclose the ingest
// bearer token. Plain HTTP is safe only for an explicitly local test or
// colocated endpoint; production endpoints must use HTTPS.
func ValidateIngestURL(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("sink: ingest URL is invalid")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		return nil
	case "http":
		if isLoopbackHost(parsed.Hostname()) {
			return nil
		}
	}
	return errors.New("sink: ingest URL scheme is not permitted")
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Envelope is the only HTTP request shape fillwire sends.
type Envelope struct {
	Fills       []decode.Record `json:"fills"`
	Source      string          `json:"source"`
	SourceRunID *string         `json:"source_run_id"`
}

// Result is positionally aligned with the fills request array.
type Result struct {
	Status string  `json:"status"`
	RowID  *int64  `json:"row_id"`
	Reason *string `json:"reason"`
}

// Response is the documented execution-ledger ingest response.
type Response struct {
	Source      string   `json:"source"`
	SourceRunID *string  `json:"source_run_id"`
	Received    int      `json:"received"`
	Accepted    int      `json:"accepted"`
	Rejected    int      `json:"rejected"`
	Results     []Result `json:"results"`
}

// HTTPStatusError preserves only a status code, never an endpoint or token.
type HTTPStatusError struct{ StatusCode int }

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("sink: ingest returned HTTP status %d", e.StatusCode)
}

// Post sends a bounded batch. It intentionally does not return endpoint URLs,
// response bodies, or authorization material in errors.
func (c *Client) Post(ctx context.Context, fills []decode.Record) (Response, error) {
	if len(fills) < 1 || len(fills) > 200 {
		return Response{}, errors.New("sink: batch size must be between 1 and 200")
	}
	body, err := json.Marshal(Envelope{Fills: fills, Source: source, SourceRunID: nil})
	if err != nil {
		return Response{}, errors.New("sink: could not encode ingest request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Response{}, errors.New("sink: could not create ingest request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	response, err := c.httpClient.Do(req)
	if err != nil {
		return Response{}, errors.New("sink: ingest network failure")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return Response{}, &HTTPStatusError{StatusCode: response.StatusCode}
	}
	limited := io.LimitReader(response.Body, 4<<20)
	var decoded Response
	if err := json.NewDecoder(limited).Decode(&decoded); err != nil {
		return Response{}, errors.New("sink: invalid ingest response")
	}
	return decoded, nil
}

// Config controls sequential delivery and retry pacing. No goroutine sends
// batches in parallel, preserving the single-consumer ordering model.
type Config struct {
	RetryMin time.Duration
	RetryMax time.Duration
	Factor   float64
	Logger   *slog.Logger
	Counters *decode.Counters
}

// Runner consumes one Redis group member and sends batches in order.
type Runner struct {
	queue    *stream.Queue
	client   *Client
	retryMin time.Duration
	retryMax time.Duration
	factor   float64
	logger   *slog.Logger
	counters *decode.Counters
}

// NewRunner validates retry settings and connects one queue to one sink.
func NewRunner(queue *stream.Queue, client *Client, cfg Config) (*Runner, error) {
	if queue == nil || client == nil {
		return nil, errors.New("sink: queue and client are required")
	}
	if cfg.RetryMin <= 0 || cfg.RetryMax < cfg.RetryMin {
		return nil, errors.New("sink: invalid retry bounds")
	}
	if cfg.Factor < 1 {
		return nil, errors.New("sink: retry factor must be at least one")
	}
	return &Runner{
		queue:    queue,
		client:   client,
		retryMin: cfg.RetryMin,
		retryMax: cfg.RetryMax,
		factor:   cfg.Factor,
		logger:   cfg.Logger,
		counters: cfg.Counters,
	}, nil
}

// Run creates its group, drains reclaimable pending messages before new ones,
// then reads and delivers batches until the context ends.
func (r *Runner) Run(ctx context.Context) error {
	if err := r.queue.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("sink: ensure consumer group: %w", err)
	}
	if err := r.reclaimPending(ctx); err != nil {
		return err
	}
	for {
		messages, err := r.queue.ReadNew(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("sink: read group: %w", err)
		}
		if len(messages) == 0 {
			continue
		}
		if err := r.deliverWithRetry(ctx, messages); err != nil {
			return err
		}
	}
}

func (r *Runner) reclaimPending(ctx context.Context) error {
	cursor := "0-0"
	for {
		messages, next, err := r.queue.AutoClaim(ctx, cursor)
		if err != nil {
			return fmt.Errorf("sink: auto-claim pending: %w", err)
		}
		if len(messages) > 0 {
			if err := r.deliverWithRetry(ctx, messages); err != nil {
				return err
			}
		}
		if next == "" || next == "0-0" || next == cursor {
			return nil
		}
		cursor = next
	}
}

func (r *Runner) deliverWithRetry(ctx context.Context, messages []stream.Message) error {
	for attempt := 0; ; attempt++ {
		err := r.DeliverOnce(ctx, messages)
		if err == nil {
			return nil
		}
		r.counters.IncIngestFailure()
		r.logFailure(err)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := wait(ctx, r.retryDelay(attempt)); err != nil {
			return err
		}
	}
}

// DeliverOnce posts one batch and acknowledges only positional success
// statuses. A malformed result count acknowledges nothing and returns an
// error so the entire original batch is retried.
func (r *Runner) DeliverOnce(ctx context.Context, messages []stream.Message) error {
	if len(messages) == 0 {
		return nil
	}
	if len(messages) > 200 {
		return errors.New("sink: batch exceeds 200 records")
	}
	fills := make([]decode.Record, len(messages))
	for i, message := range messages {
		fills[i] = message.Record
	}
	response, err := r.client.Post(ctx, fills)
	if err != nil {
		return err
	}
	if len(response.Results) != len(messages) {
		return fmt.Errorf("sink: result count %d does not match request count %d", len(response.Results), len(messages))
	}

	ackIDs := make([]string, 0, len(messages))
	inserted, updated, unchanged, rejected := 0, 0, 0, 0
	for index, result := range response.Results {
		switch result.Status {
		case "inserted":
			inserted++
			ackIDs = append(ackIDs, messages[index].ID)
		case "updated":
			updated++
			ackIDs = append(ackIDs, messages[index].ID)
		case "unchanged":
			unchanged++
			ackIDs = append(ackIDs, messages[index].ID)
			if messages[index].DupSuspect {
				r.logDuplicateAbsorbed(messages[index])
			}
		case "rejected":
			rejected++
			// Leave it pending. Acknowledging an item the ledger rejected would
			// discard evidence and make recovery impossible.
		default:
			return fmt.Errorf("sink: unrecognized ingest result status %q", result.Status)
		}
	}
	if r.logger != nil {
		r.logger.Info("ingest response statuses observed",
			"batch_size", len(messages),
			"inserted", inserted,
			"updated", updated,
			"unchanged", unchanged,
			"rejected", rejected,
		)
	}
	return r.queue.Ack(ctx, ackIDs...)
}

func (r *Runner) retryDelay(attempt int) time.Duration {
	delay := r.retryMin
	for i := 0; i < attempt && delay < r.retryMax; i++ {
		next := time.Duration(float64(delay) * r.factor)
		if next <= delay || next > r.retryMax {
			delay = r.retryMax
			break
		}
		delay = next
	}
	return delay
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runner) logFailure(err error) {
	if r.logger == nil {
		return
	}
	var status *HTTPStatusError
	if errors.As(err, &status) && status.StatusCode >= 400 && status.StatusCode < 500 {
		r.logger.Error("ingest request rejected; retaining pending records", "status_code", status.StatusCode)
		return
	}
	r.logger.Warn("ingest attempt failed; retaining pending records", "reason", safeErrorReason(err))
}

func (r *Runner) logDuplicateAbsorbed(message stream.Message) {
	if r.logger == nil {
		return
	}
	r.logger.Info("duplicate KIS fill absorbed by ledger", "order_no", message.Record.BrokerOrderID, "fill_seq", message.Record.FillSeq, "observation_count", message.DupObservationCount)
}

func safeErrorReason(err error) string {
	if err == nil {
		return ""
	}
	// All errors constructed in this package are URL- and token-free. Keep the
	// whitelist narrow in case a dependency error enters later.
	text := err.Error()
	if strings.Contains(text, "://") || strings.Contains(strings.ToLower(text), "authorization") {
		return "redacted"
	}
	return text
}
