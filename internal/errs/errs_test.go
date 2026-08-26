package errs

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestKindSurvivesWrapping(t *testing.T) {
	base := E(KindThrottled, "s3.Put", errors.New("slow down"))
	wrapped := fmt.Errorf("upload failed: %w", base)
	doubleWrapped := fmt.Errorf("pipeline: %w", wrapped)

	if KindOf(doubleWrapped) != KindThrottled {
		t.Fatalf("kind = %s, want throttled", KindOf(doubleWrapped))
	}
	if !IsRetryable(doubleWrapped) {
		t.Fatal("throttled error should be retryable through two wraps")
	}
}

func TestUnknownIsNotRetryable(t *testing.T) {
	// Defaulting to "do not retry" is deliberate: an unclassified provider
	// error should surface as a clear failure, not silently consume the
	// whole backoff schedule.
	if IsRetryable(errors.New("mystery")) {
		t.Fatal("unclassified error must not be retryable")
	}
	if KindOf(errors.New("mystery")) != KindUnknown {
		t.Fatal("unclassified error should report KindUnknown")
	}
}

func TestRetryableKinds(t *testing.T) {
	retryable := map[Kind]bool{
		KindThrottled: true,
		KindTransient: true,
	}
	all := []Kind{
		KindUnknown, KindNotFound, KindAlreadyExists, KindCorrupt,
		KindThrottled, KindTransient, KindExpired, KindInvalid,
		KindPermission, KindUnsupported, KindCanceled,
	}
	for _, k := range all {
		err := E(k, "op", errors.New("x"))
		if got, want := IsRetryable(err), retryable[k]; got != want {
			t.Errorf("IsRetryable(%s) = %v, want %v", k, got, want)
		}
	}
}

// Corruption must never be retried: the bytes on disk are wrong and asking
// again returns the same wrong bytes.
func TestCorruptIsNeverRetryable(t *testing.T) {
	err := E(KindCorrupt, "pack.Read", errors.New("checksum mismatch"))
	if IsRetryable(err) {
		t.Fatal("corruption must not be retryable")
	}
	if !IsCorrupt(err) {
		t.Fatal("IsCorrupt should match")
	}
}

// Context cancellation can be wrapped by any layer. Classifying it centrally
// means no individual layer has to remember to, and a shutting-down pipeline
// exits instead of working through a backoff schedule.
func TestContextErrorsClassifyAsCanceled(t *testing.T) {
	cases := map[string]error{
		"canceled":  context.Canceled,
		"deadline":  context.DeadlineExceeded,
		"wrapped":   fmt.Errorf("read block: %w", context.Canceled),
		"double":    fmt.Errorf("a: %w", fmt.Errorf("b: %w", context.DeadlineExceeded)),
		"under E":   E(KindTransient, "op", context.Canceled),
		"unwrapped": context.Canceled,
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			if KindOf(err) != KindCanceled {
				t.Fatalf("kind = %s, want canceled", KindOf(err))
			}
			if IsRetryable(err) {
				t.Fatal("canceled context must not be retryable")
			}
		})
	}
}

func TestErrorMessageIncludesOpAndKind(t *testing.T) {
	err := E(KindNotFound, "store.Get", errors.New("no such key"))
	msg := err.Error()
	for _, want := range []string{"store.Get", "not_found", "no such key"} {
		if !contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

func TestErrorMessageHandlesNilCause(t *testing.T) {
	err := E(KindInvalid, "chunker.New", nil)
	if err.Error() == "" {
		t.Fatal("empty message for nil cause")
	}
	if !contains(err.Error(), "invalid") {
		t.Fatalf("message %q missing kind", err.Error())
	}
}

func TestUnwrapReachesCause(t *testing.T) {
	cause := errors.New("root cause")
	err := E(KindTransient, "op", cause)
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is failed to reach the cause")
	}
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatal("errors.As failed to extract *Error")
	}
	if typed.Op != "op" {
		t.Fatalf("Op = %q, want %q", typed.Op, "op")
	}
}

func TestKindOfNil(t *testing.T) {
	if KindOf(nil) != KindUnknown {
		t.Fatal("KindOf(nil) should be KindUnknown")
	}
	if IsRetryable(nil) {
		t.Fatal("nil must not be retryable")
	}
}

func TestPredicates(t *testing.T) {
	if !IsNotFound(E(KindNotFound, "op", nil)) {
		t.Error("IsNotFound failed")
	}
	if !IsAlreadyExists(E(KindAlreadyExists, "op", nil)) {
		t.Error("IsAlreadyExists failed")
	}
	if !IsThrottle(E(KindThrottled, "op", nil)) {
		t.Error("IsThrottle failed")
	}
	if IsThrottle(E(KindTransient, "op", nil)) {
		t.Error("transient must not report as throttle")
	}
}

func TestKindStringsAreStable(t *testing.T) {
	want := map[Kind]string{
		KindUnknown:       "unknown",
		KindNotFound:      "not_found",
		KindAlreadyExists: "already_exists",
		KindCorrupt:       "corrupt",
		KindThrottled:     "throttled",
		KindTransient:     "transient",
		KindExpired:       "expired",
		KindInvalid:       "invalid",
		KindPermission:    "permission_denied",
		KindUnsupported:   "unsupported",
		KindCanceled:      "canceled",
	}
	for k, s := range want {
		if k.String() != s {
			t.Errorf("Kind(%d).String() = %q, want %q", k, k.String(), s)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
