package retry

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
)

// fastPolicy keeps test runtime negligible while preserving the shape of the
// schedule. Deterministic Rand so a failure is reproducible.
func fastPolicy(attempts int) Policy {
	return Policy{
		MaxAttempts:        attempts,
		Base:               time.Microsecond,
		Max:                time.Millisecond,
		ThrottleMultiplier: 3.0,
		Rand:               rand.New(rand.NewSource(1)), //nolint:gosec // deterministic test jitter
	}
}

func TestDoSucceedsFirstTry(t *testing.T) {
	calls := 0
	err := Do(context.Background(), fastPolicy(5), "op", func(context.Context, int) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("called %d times, want 1", calls)
	}
}

func TestDoRetriesTransientThenSucceeds(t *testing.T) {
	calls := 0
	err := Do(context.Background(), fastPolicy(5), "op", func(_ context.Context, attempt int) error {
		calls++
		if attempt < 2 {
			return errs.E(errs.KindTransient, "op", errors.New("boom"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if calls != 3 {
		t.Fatalf("called %d times, want 3", calls)
	}
}

// A permanent error must not consume the retry budget. This is the behaviour
// that stops a genuine bug from taking the full backoff schedule to surface.
func TestDoDoesNotRetryPermanent(t *testing.T) {
	for _, kind := range []errs.Kind{
		errs.KindCorrupt,
		errs.KindInvalid,
		errs.KindNotFound,
		errs.KindUnsupported,
		errs.KindExpired,
		errs.KindUnknown,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			calls := 0
			err := Do(context.Background(), fastPolicy(5), "op", func(context.Context, int) error {
				calls++
				return errs.E(kind, "op", errors.New("nope"))
			})
			if err == nil {
				t.Fatal("got nil, want error")
			}
			if calls != 1 {
				t.Fatalf("kind %s retried %d times; permanent errors must not be retried", kind, calls)
			}
		})
	}
}

func TestDoExhaustsAndWrapsLastError(t *testing.T) {
	sentinel := errors.New("sentinel")
	calls := 0
	err := Do(context.Background(), fastPolicy(3), "op", func(context.Context, int) error {
		calls++
		return errs.E(errs.KindTransient, "op", sentinel)
	})
	if err == nil {
		t.Fatal("got nil, want error")
	}
	if calls != 3 {
		t.Fatalf("called %d times, want 3", calls)
	}
	// The original cause must survive to the top so a caller can inspect it.
	if !errors.Is(err, sentinel) {
		t.Fatalf("exhausted error lost its cause: %v", err)
	}
}

// Cancellation must win immediately, not after the current backoff elapses.
func TestDoRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	p := Policy{
		MaxAttempts:        10,
		Base:               10 * time.Second, // long enough that only cancellation can end the wait
		Max:                10 * time.Second,
		ThrottleMultiplier: 1,
		Rand:               rand.New(rand.NewSource(1)), //nolint:gosec // deterministic test jitter
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := Do(ctx, p, "op", func(context.Context, int) error {
		calls++
		return errs.E(errs.KindTransient, "op", errors.New("boom"))
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("got nil, want cancellation error")
	}
	if errs.KindOf(err) != errs.KindCanceled {
		t.Fatalf("kind = %s, want canceled", errs.KindOf(err))
	}
	if elapsed > 5*time.Second {
		t.Fatalf("took %v; cancellation did not interrupt the backoff wait", elapsed)
	}
}

func TestDoReturnsCanceledWithoutCallingFn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := Do(ctx, fastPolicy(5), "op", func(context.Context, int) error {
		calls++
		return nil
	})
	if err == nil {
		t.Fatal("got nil, want cancellation error")
	}
	if calls != 0 {
		t.Fatalf("fn called %d times on an already-canceled context, want 0", calls)
	}
}

// Throttling must back off harder than an ordinary transient failure.
func TestThrottleBacksOffHarder(t *testing.T) {
	p := Policy{
		MaxAttempts:        5,
		Base:               100 * time.Millisecond,
		Max:                10 * time.Second,
		ThrottleMultiplier: 3.0,
	}

	// delay draws uniformly from [0, cap), so compare the caps by sampling
	// the maximum observed across many draws rather than a single value.
	maxOf := func(throttled bool) time.Duration {
		var m time.Duration
		for range 2000 {
			if d := p.delay(2, throttled); d > m {
				m = d
			}
		}
		return m
	}

	plain := maxOf(false)
	throttled := maxOf(true)
	if throttled <= plain {
		t.Fatalf("throttled max delay %v not greater than plain %v", throttled, plain)
	}
}

// Exponential growth must saturate at Max rather than overflowing into a
// negative duration, which would turn backoff into a spin loop.
func TestDelaySaturatesAtMax(t *testing.T) {
	p := Policy{
		MaxAttempts:        100,
		Base:               time.Second,
		Max:                5 * time.Second,
		ThrottleMultiplier: 1,
	}
	for _, attempt := range []int{0, 10, 62, 63, 64, 1000} {
		d := p.delay(attempt, false)
		if d < 0 {
			t.Fatalf("attempt %d produced negative delay %v", attempt, d)
		}
		if d >= p.Max {
			t.Fatalf("attempt %d produced delay %v, want < Max (%v)", attempt, d, p.Max)
		}
	}
}

func TestDoValueReturnsResult(t *testing.T) {
	got, err := DoValue(context.Background(), fastPolicy(3), "op",
		func(_ context.Context, attempt int) (string, error) {
			if attempt < 1 {
				return "", errs.E(errs.KindTransient, "op", errors.New("boom"))
			}
			return "ok", nil
		})
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if got != "ok" {
		t.Fatalf("got %q, want %q", got, "ok")
	}
}

func TestDoValueReturnsZeroOnFailure(t *testing.T) {
	got, err := DoValue(context.Background(), fastPolicy(2), "op",
		func(context.Context, int) (int, error) {
			return 42, errs.E(errs.KindTransient, "op", errors.New("boom"))
		})
	if err == nil {
		t.Fatal("got nil, want error")
	}
	if got != 0 {
		t.Fatalf("got %d, want zero value on failure", got)
	}
}

func TestMaxAttemptsBelowOneStillRunsOnce(t *testing.T) {
	calls := 0
	_ = Do(context.Background(), Policy{MaxAttempts: 0}, "op", func(context.Context, int) error {
		calls++
		return nil
	})
	if calls != 1 {
		t.Fatalf("called %d times, want exactly 1", calls)
	}
}
