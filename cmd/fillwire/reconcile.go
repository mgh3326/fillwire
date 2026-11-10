package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/reconcile"
	"github.com/mgh3326/fillwire/internal/sink"
)

const (
	defaultReconcileTimeout = 10 * time.Second
	// minReconcileInterval matches auto_trader's 60s same-market dedupe: a
	// shorter period would only produce deduped calls.
	minReconcileInterval = time.Minute
)

// reconcileFileConfig is the [reconcile] table, decoded strictly: an unknown
// key rejects the config.
type reconcileFileConfig struct {
	Enabled  bool      `toml:"enabled"`
	URL      string    `toml:"url"`
	Markets  *[]string `toml:"markets"`
	DryRun   *bool     `toml:"dry_run"`
	Interval string    `toml:"interval"`
	Timeout  string    `toml:"timeout"`
}

// reconcileSettings is the resolved table. The zero value is "off".
type reconcileSettings struct {
	enabled  bool
	url      string
	markets  []string
	dryRun   bool
	interval time.Duration
	timeout  time.Duration
}

func resolveReconcileSettings(contents []byte) (reconcileSettings, error) {
	var f reconcileFileConfig
	present, err := strictTable(contents, "reconcile", &f)
	if err != nil {
		return reconcileSettings{}, err
	}
	if !present || !f.Enabled {
		return reconcileSettings{}, nil
	}
	if err := validateReconcileURL(f.URL); err != nil {
		return reconcileSettings{}, err
	}
	out := reconcileSettings{enabled: true, url: f.URL, markets: []string{"kr"}, dryRun: true, timeout: defaultReconcileTimeout}
	if f.Markets != nil {
		out.markets = nil
		seen := map[string]bool{}
		for _, m := range *f.Markets {
			if m != "kr" && m != "us" && m != "crypto" {
				return reconcileSettings{}, errors.New("reconcile: markets must be kr, us or crypto")
			}
			if seen[m] {
				return reconcileSettings{}, errors.New("reconcile: markets must not repeat")
			}
			seen[m] = true
			out.markets = append(out.markets, m)
		}
		if len(out.markets) == 0 {
			return reconcileSettings{}, errors.New("reconcile: markets must not be empty")
		}
	}
	if f.DryRun != nil {
		out.dryRun = *f.DryRun
	}
	if f.Interval != "" {
		d, err := time.ParseDuration(f.Interval)
		if err != nil || (d != 0 && d < minReconcileInterval) {
			return reconcileSettings{}, fmt.Errorf("reconcile: interval must be 0 or at least %s", minReconcileInterval)
		}
		out.interval = d
	}
	if f.Timeout != "" {
		d, err := time.ParseDuration(f.Timeout)
		if err != nil || d <= 0 {
			return reconcileSettings{}, errors.New("reconcile: invalid timeout")
		}
		out.timeout = d
	}
	return out, nil
}

// validateReconcileURL applies the ingest URL scheme policy (https, or http
// only on loopback) and requires an exact path with nothing else attached.
// The message never echoes the URL.
func validateReconcileURL(raw string) error {
	if err := sink.ValidateIngestURL(raw); err != nil {
		return errors.New("reconcile: url rejected")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path == "" || parsed.Path == "/" {
		return errors.New("reconcile: url must be an exact path without credentials, query or fragment")
	}
	return nil
}

// startReconcile returns nil when the table is off. The scheduler is bound to
// ctx; the caller cancels it and calls Wait at shutdown.
func startReconcile(ctx context.Context, cfg runtimeConfig, logger *slog.Logger) (*reconcile.Scheduler, error) {
	if !cfg.reconcile.enabled {
		return nil, nil
	}
	client, err := reconcile.NewClient(reconcile.HTTPConfig{
		URL:         cfg.reconcile.url,
		Token:       cfg.ingestToken,
		TokenHeader: cfg.Ingest.TokenHeader,
		DryRun:      cfg.reconcile.dryRun,
		Timeout:     cfg.reconcile.timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile: %s", strings.TrimPrefix(err.Error(), "reconcile: "))
	}
	scheduler := reconcile.NewScheduler(ctx, reconcile.SchedulerConfig{
		Caller:    client,
		Markets:   cfg.reconcile.markets,
		Interval:  cfg.reconcile.interval,
		RateLimit: cfg.alertRateLimit,
		Logger:    logger,
	})
	go scheduler.Run(ctx)
	return scheduler, nil
}
