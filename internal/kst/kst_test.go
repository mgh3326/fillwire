package kst

import (
	"testing"
	"time"
)

func TestZone(t *testing.T) {
	at := time.Date(2026, time.November, 4, 15, 30, 0, 0, time.UTC).In(Zone)
	if got := at.Format("2006-01-02 15:04 MST"); got != "2026-11-05 00:30 KST" {
		t.Fatalf("got %s", got)
	}
}
