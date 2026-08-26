// Package retry implements bounded exponential backoff with full jitter.
//
// Written from scratch rather than imported (CLAUDE.md R3): the backoff
// strategy is one of the parts of this system worth being able to defend in
// detail, and the whole implementation is under a hundred lines.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
)

// Policy describes a backoff schedule.
type Policy struct {
	// MaxAttempts is the total number of tries, including the first.
	// A value of 1 means "no retries".
	MaxAttempts int

	// Base is the delay before the first retry, doubled each attempt.
	Base time.Duration

	// Max caps the computed delay so exponential growth cannot run away.
	Max time.Duration

	// ThrottleMultiplier scales the delay when the error is a rate-limit
	// signal rather than an ordinary transient failure.
	//
	// Throttling means the server has told us we are collectively going too
	// fast; the correct response is to slow down harder than we would for a
	// dropped connection, which carries no such information.
	ThrottleMultiplier float64

	// Rand supplies jitter. Nil means use the package-level source.
	//
	// Injectable purely so tests can be deterministic. Rejected: a global
	// seed set in TestMain, which makes tests order-dependent.
	Rand *rand.Rand
}

// DefaultPolicy is the schedule used for provider calls.
//
// Five attempts with a 100ms base caps total sleep at roughly 3s before the
// jitter is applied, which is short enough that a genuinely broken run fails
// fast, and long enough to ride out a brief throttle. These are reasoned
// defaults, not measured ones — no real provider was ever called (R7), so
// nothing here is tuned against observed latency.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts:        5,
		Base:               100 * time.Millisecond,
		Max:                10 * time.Second,
		ThrottleMultiplier: 3.0,
	}
}

// delay computes the backoff for a given zero-based attempt number.
//
// This is "full jitter": the delay is drawn uniformly from [0, cap) where cap
// grows exponentially. The alternatives, and why they lost:
//
//   - No jitter: every client that failed at the same instant retries at the
//     same instant. The thundering herd that caused the failure reassembles
//     itself on a schedule. This is the worst option and the most common.
//   - Equal jitter (half fixed, half random): keeps clients spread out, but
//     the fixed half still synchronises a portion of the load, and it always
//     waits at least half the cap even when the service has recovered.
//   - Decorrelated jitter (delay derived from the previous delay): comparable
//     in practice, but carries state between attempts, which makes it harder
//     to reason about and to test.
//
// Full jitter spreads retries across the whole window and lets some clients
// retry almost immediately, which recovers throughput fastest when the
// underlying problem has cleared.
func (p Policy) delay(attempt int, throttled bool) time.Duration {
	// math.Pow on a float is used rather than a shift so that a large attempt
	// number saturates at +Inf instead of overflowing into a negative
	// duration. A negative duration would make time.After fire instantly and
	// turn backoff into a spin loop.
	capped := float64(p.Base) * math.Pow(2, float64(attempt))
	if throttled && p.ThrottleMultiplier > 0 {
		capped *= p.ThrottleMultiplier
	}
	if capped > float64(p.Max) || math.IsInf(capped, 1) {
		capped = float64(p.Max)
	}
	if capped <= 0 {
		return 0
	}

	r := p.Rand
	var n int64
	if r != nil {
		n = r.Int63n(int64(capped))
	} else {
		n = rand.Int63n(int64(capped)) //nolint:gosec // jitter, not cryptographic
	}
	return time.Duration(n)
}

// Do runs fn until it succeeds, until the policy is exhausted, or until ctx
// is done, whichever comes first.
//
// fn receives the zero-based attempt number so a caller can log or refresh
// per-attempt state (an expired token, for instance).
func Do(ctx context.Context, p Policy, op string, fn func(ctx context.Context, attempt int) error) error {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}

	var lastErr error
	for attempt := range p.MaxAttempts {
		// Check cancellation before doing work rather than only after
		// failing. Otherwise a canceled context still pays for one full
		// attempt, which on a large backup means thousands of in-flight
		// requests continue after the user pressed Ctrl-C.
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}

		err := fn(ctx, attempt)
		if err == nil {
			return nil
		}
		lastErr = err

		if !errs.IsRetryable(err) {
			return err
		}
		if attempt == p.MaxAttempts-1 {
			break
		}

		d := p.delay(attempt, errs.IsThrottle(err))

		// time.NewTimer rather than time.After: time.After leaks its timer
		// until it fires, and this loop can abandon a wait on every
		// cancellation. At scale that is a real leak, not a theoretical one.
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return errs.E(errs.KindCanceled, op, ctx.Err())
		case <-t.C:
		}
	}

	return fmt.Errorf("%s: exhausted %d attempts: %w", op, p.MaxAttempts, lastErr)
}

// DoValue is Do for an operation that produces a value.
//
// Go generics do not allow a method to introduce a type parameter, so this is
// a function rather than a method on Policy.
func DoValue[T any](ctx context.Context, p Policy, op string, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	var result T
	err := Do(ctx, p, op, func(ctx context.Context, attempt int) error {
		v, err := fn(ctx, attempt)
		if err != nil {
			return err
		}
		result = v
		return nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}
