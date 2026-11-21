// Package clock holds the scaled test clock: simulated time runs factor
// times faster than wall time, so protocol timers of seconds and minutes
// run in milliseconds while their ratios stay intact.
package clock

import "time"

// Scaled is a clock.Clock whose time runs factor times faster.
type Scaled struct {
	start  time.Time
	real0  time.Time
	factor float64
}

// NewScaled returns a scaled clock starting now.
func NewScaled(factor float64) *Scaled {
	now := time.Now()
	return &Scaled{start: now, real0: now, factor: factor}
}

// Now is the simulated time.
func (s *Scaled) Now() time.Time {
	return s.start.Add(time.Duration(float64(time.Since(s.real0)) * s.factor))
}

// After fires after simulated duration d.
func (s *Scaled) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		time.Sleep(s.Real(d))
		ch <- s.Now()
	}()
	return ch
}

// Real converts a simulated duration to wall time.
func (s *Scaled) Real(d time.Duration) time.Duration {
	return time.Duration(float64(d) / s.factor)
}

// Factor is the speed-up.
func (s *Scaled) Factor() float64 { return s.factor }
