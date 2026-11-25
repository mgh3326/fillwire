package ratebudget

import (
	"sort"
	"sync"
)

// Slots is the subscription-slot budget for one provider session. Fixed
// consumers reserve capacity first; promoted symbols take what is left, and a
// short capacity demotes the lowest priority instead of overflowing. A reserved
// slot is never demoted.
type Slots struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]slotEntry
}

type slotEntry struct {
	priority int
	reserved bool
}

// Held is one occupied slot as returned by Held.
type Held struct {
	ID       string
	Priority int
	Reserved bool
}

// NewSlots builds a budget of capacity slots. A capacity of zero refuses every
// admission.
func NewSlots(capacity int) (*Slots, error) {
	if capacity < 0 {
		return nil, configf("slot capacity %d must not be negative", capacity)
	}
	return &Slots{capacity: capacity, entries: make(map[string]slotEntry)}, nil
}

// Reserve gives a fixed consumer a slot. When the budget is full it demotes the
// lowest priority promoted entry; a budget fully held by reservations refuses
// with budget_cut. It returns the demoted id, if any. Reserving an id already
// held updates its priority and does not demote anything.
func (s *Slots) Reserve(id string, priority int) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return "", configf("slot id must not be empty")
	}
	if _, ok := s.entries[id]; ok {
		s.entries[id] = slotEntry{priority: priority, reserved: true}
		return "", nil
	}
	if len(s.entries) < s.capacity {
		s.entries[id] = slotEntry{priority: priority, reserved: true}
		return "", nil
	}
	victim := s.lowestPromoted(nil)
	if victim == "" {
		return "", &Refusal{Code: CodeBudgetCut}
	}
	delete(s.entries, victim)
	s.entries[id] = slotEntry{priority: priority, reserved: true}
	return victim, nil
}

// Admit gives a promoted symbol a slot. When the budget is full it demotes the
// lowest priority promoted entry only if that entry's priority is below
// priority; otherwise it refuses with budget_cut. It returns the demoted id, if
// any. Admitting an id already held updates its priority.
func (s *Slots) Admit(id string, priority int) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return "", configf("slot id must not be empty")
	}
	if e, ok := s.entries[id]; ok {
		e.priority = priority
		s.entries[id] = e
		return "", nil
	}
	if len(s.entries) < s.capacity {
		s.entries[id] = slotEntry{priority: priority}
		return "", nil
	}
	victim := s.lowestPromoted(&priority)
	if victim == "" {
		return "", &Refusal{Code: CodeBudgetCut}
	}
	delete(s.entries, victim)
	s.entries[id] = slotEntry{priority: priority}
	return victim, nil
}

// Release frees id and reports whether it was held.
func (s *Slots) Release(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[id]; !ok {
		return false
	}
	delete(s.entries, id)
	return true
}

// Held returns the occupied slots, highest priority first, ties by id.
func (s *Slots) Held() []Held {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := make([]Held, 0, len(s.entries))
	for id, e := range s.entries {
		held = append(held, Held{ID: id, Priority: e.priority, Reserved: e.reserved})
	}
	sort.Slice(held, func(i, j int) bool {
		if held[i].Priority != held[j].Priority {
			return held[i].Priority > held[j].Priority
		}
		return held[i].ID < held[j].ID
	})
	return held
}

// lowestPromoted returns the id of the lowest priority non-reserved entry, or
// "" when there is none. With below set it only considers entries whose
// priority is strictly lower than *below. Ties go to the lexicographically
// smallest id so the choice is deterministic.
func (s *Slots) lowestPromoted(below *int) string {
	victim := ""
	var victimPriority int
	for id, e := range s.entries {
		if e.reserved || (below != nil && e.priority >= *below) {
			continue
		}
		if victim == "" || e.priority < victimPriority || (e.priority == victimPriority && id < victim) {
			victim = id
			victimPriority = e.priority
		}
	}
	return victim
}
