// Package clock is the time source of the Executor. Production uses the
// wall clock; tests use a scaled clock so minute-long protocol timers run
// in milliseconds.
package clock

import "time"

// Clock provides the current time and timers.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type real struct{}

// Real returns the wall clock.
func Real() Clock { return real{} }

func (real) Now() time.Time                         { return time.Now() }
func (real) After(d time.Duration) <-chan time.Time { return time.After(d) }
