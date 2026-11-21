package clock

import (
	"sync"
	"time"
)

// Manual is a clock.Clock whose time moves only when the test advances it.
// Between two advances every reading of the time returns the same instant,
// however long the host takes to run the code in between, so tests of
// protocol timing do not depend on host load.
type Manual struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	at time.Time
	d  time.Duration
	ch chan time.Time
}

// NewManual returns a manual clock standing at start.
func NewManual(start time.Time) *Manual { return &Manual{now: start} }

// Now is the current manual time.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After fires once the time has been advanced by d; d <= 0 fires at once.
func (m *Manual) After(d time.Duration) <-chan time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- m.now
		return ch
	}
	m.waiters = append(m.waiters, &waiter{at: m.now.Add(d), d: d, ch: ch})
	return ch
}

// Pending counts the unfired waits that asked for exactly d.
func (m *Manual) Pending(d time.Duration) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, w := range m.waiters {
		if w.d == d {
			n++
		}
	}
	return n
}

// AdvanceToNext moves the time to the earliest pending deadline and fires
// every wait due by then. It returns false, leaving the time unchanged, when
// nothing waits.
func (m *Manual) AdvanceToNext() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.waiters) == 0 {
		return false
	}
	next := m.waiters[0].at
	for _, w := range m.waiters[1:] {
		if w.at.Before(next) {
			next = w.at
		}
	}
	if next.After(m.now) {
		m.now = next
	}
	kept := m.waiters[:0]
	for _, w := range m.waiters {
		if w.at.After(m.now) {
			kept = append(kept, w)
			continue
		}
		w.ch <- m.now
	}
	m.waiters = kept
	return true
}
