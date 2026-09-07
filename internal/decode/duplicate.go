package decode

import "sync"

type duplicateKey struct {
	orderNo string
	fillSeq int64
}

// duplicateTracker is deliberately process-local and bounded. It is a hint
// for an otherwise indistinguishable KIS partial fill, not a ledger of truth.
type duplicateTracker struct {
	mu     sync.Mutex
	max    int
	counts map[duplicateKey]uint64
	fifo   []duplicateKey
}

func newDuplicateTracker(max int) *duplicateTracker {
	if max <= 0 {
		max = 10000
	}
	return &duplicateTracker{max: max, counts: make(map[duplicateKey]uint64)}
}

func (t *duplicateTracker) observe(key duplicateKey) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if count, exists := t.counts[key]; exists {
		count++
		t.counts[key] = count
		return count
	}
	if len(t.counts) >= t.max {
		oldest := t.fifo[0]
		t.fifo = t.fifo[1:]
		delete(t.counts, oldest)
	}
	t.counts[key] = 1
	t.fifo = append(t.fifo, key)
	return 1
}

func (t *duplicateTracker) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.counts)
}
