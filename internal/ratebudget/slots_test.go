package ratebudget_test

import (
	"errors"
	"testing"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

func heldIDs(s *ratebudget.Slots) []string {
	held := s.Held()
	ids := make([]string, len(held))
	for i, h := range held {
		ids[i] = h.ID
	}
	return ids
}

func TestReserveFillsTheBudgetThenCuts(t *testing.T) {
	slots, err := ratebudget.NewSlots(2)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if evicted, err := slots.Reserve("fills", 100); err != nil || evicted != "" {
		t.Fatalf("Reserve fills = %q, %v", evicted, err)
	}
	if _, err := slots.Reserve("books", 100); err != nil {
		t.Fatalf("Reserve books: %v", err)
	}
	_, err = slots.Reserve("extra", 100)
	mustRefuse(t, err, ratebudget.CodeBudgetCut)
}

func TestReserveDemotesPromoted(t *testing.T) {
	slots, err := ratebudget.NewSlots(2)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Admit("sym-a", 10); err != nil {
		t.Fatalf("Admit sym-a: %v", err)
	}
	if _, err := slots.Admit("sym-b", 20); err != nil {
		t.Fatalf("Admit sym-b: %v", err)
	}
	evicted, err := slots.Reserve("fills", 100)
	if err != nil {
		t.Fatalf("Reserve fills: %v", err)
	}
	if evicted != "sym-a" {
		t.Fatalf("evicted = %q, want sym-a (the lowest priority)", evicted)
	}
	if got := heldIDs(slots); len(got) != 2 || got[0] != "fills" || got[1] != "sym-b" {
		t.Fatalf("held = %v, want [fills sym-b]", got)
	}
}

func TestAdmitDemotesTheLowestPriority(t *testing.T) {
	slots, err := ratebudget.NewSlots(2)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Admit("a", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := slots.Admit("b", 20); err != nil {
		t.Fatal(err)
	}
	evicted, err := slots.Admit("c", 15)
	if err != nil {
		t.Fatalf("Admit c: %v", err)
	}
	if evicted != "a" {
		t.Fatalf("evicted = %q, want a", evicted)
	}
	if got := heldIDs(slots); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("held = %v, want [b c]", got)
	}
}

func TestAdmitRefusesWhenItIsNotHigherPriority(t *testing.T) {
	slots, err := ratebudget.NewSlots(2)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Admit("a", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := slots.Admit("b", 10); err != nil {
		t.Fatal(err)
	}
	_, err = slots.Admit("c", 10)
	mustRefuse(t, err, ratebudget.CodeBudgetCut)
	if got := heldIDs(slots); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("held = %v, want [a b]", got)
	}
}

func TestReservedSlotIsNeverDemoted(t *testing.T) {
	slots, err := ratebudget.NewSlots(1)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Reserve("fills", 1); err != nil {
		t.Fatalf("Reserve fills: %v", err)
	}
	_, err = slots.Admit("sym", 1000)
	mustRefuse(t, err, ratebudget.CodeBudgetCut)
	if got := heldIDs(slots); len(got) != 1 || got[0] != "fills" {
		t.Fatalf("held = %v, want [fills]", got)
	}
}

func TestReleaseFreesASlot(t *testing.T) {
	slots, err := ratebudget.NewSlots(1)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Admit("a", 1); err != nil {
		t.Fatal(err)
	}
	if !slots.Release("a") {
		t.Fatal("Release(a) = false, want true")
	}
	if slots.Release("a") {
		t.Fatal("second Release(a) = true, want false")
	}
	if _, err := slots.Admit("b", 1); err != nil {
		t.Fatalf("Admit b after release: %v", err)
	}
}

func TestAdmitExistingUpdatesPriorityWithoutEviction(t *testing.T) {
	slots, err := ratebudget.NewSlots(1)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Admit("a", 1); err != nil {
		t.Fatal(err)
	}
	evicted, err := slots.Admit("a", 30)
	if err != nil || evicted != "" {
		t.Fatalf("Admit a again = %q, %v", evicted, err)
	}
	held := slots.Held()
	if len(held) != 1 || held[0].ID != "a" || held[0].Priority != 30 {
		t.Fatalf("held = %+v, want one a at priority 30", held)
	}
}

func TestHeldIsOrderedByPriorityThenID(t *testing.T) {
	slots, err := ratebudget.NewSlots(4)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	if _, err := slots.Admit("b", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := slots.Admit("a", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := slots.Reserve("fills", 50); err != nil {
		t.Fatal(err)
	}
	want := []ratebudget.Held{
		{ID: "fills", Priority: 50, Reserved: true},
		{ID: "a", Priority: 5},
		{ID: "b", Priority: 5},
	}
	got := slots.Held()
	if len(got) != len(want) {
		t.Fatalf("held = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("held[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestSlotBudgetEdgeCases(t *testing.T) {
	if _, err := ratebudget.NewSlots(-1); !errors.Is(err, ratebudget.ErrConfig) {
		t.Fatalf("negative capacity error = %v, want ErrConfig", err)
	}
	empty, err := ratebudget.NewSlots(0)
	if err != nil {
		t.Fatalf("NewSlots(0): %v", err)
	}
	_, err = empty.Reserve("fills", 100)
	mustRefuse(t, err, ratebudget.CodeBudgetCut)
	if _, err := empty.Admit("", 1); !errors.Is(err, ratebudget.ErrConfig) {
		t.Fatalf("empty id error = %v, want ErrConfig", err)
	}
}
