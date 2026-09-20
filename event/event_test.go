package event

import (
	"errors"
	"testing"
	"time"
)

func fixedClock(t time.Time) Clock { return func() time.Time { return t } }

func TestValidateMissingTimePolicies(t *testing.T) {
	now := time.Unix(1000, 0)
	// reject
	v := NewTimeValidator(PolicyReject, time.Time{}, 0, fixedClock(now))
	_, reason, rejected := v.Validate(Event{ID: "x"})
	if reason != ReasonMissingTime || !rejected {
		t.Fatalf("reject: reason=%v rejected=%v", reason, rejected)
	}
	if !errors.Is(ErrEventRejected, ErrEventRejected) {
		t.Fatal("sentinel")
	}
	// assign
	v2 := NewTimeValidator(PolicyAssign, time.Time{}, 0, fixedClock(now))
	fixed, reason2, rejected2 := v2.Validate(Event{ID: "x"})
	if reason2 != ReasonMissingTime || rejected2 || !fixed.Timestamp.Equal(now) {
		t.Fatalf("assign: reason=%v rejected=%v ts=%v", reason2, rejected2, fixed.Timestamp)
	}
}

func TestValidateBounds(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	v := NewTimeValidator(PolicyReject, time.Unix(1000, 0), time.Minute, fixedClock(now))
	if _, r, rej := v.Validate(Event{Timestamp: time.Unix(10, 0)}); r != ReasonBeforeMinTime || !rej {
		t.Fatalf("min bound: %v %v", r, rej)
	}
	if _, r, rej := v.Validate(Event{Timestamp: now.Add(2 * time.Minute)}); r != ReasonAfterFutureTolerance || !rej {
		t.Fatalf("future bound: %v %v", r, rej)
	}
	if _, r, rej := v.Validate(Event{Timestamp: now.Add(30 * time.Second)}); r != ReasonOK || rej {
		t.Fatalf("valid event: %v %v", r, rej)
	}
}
