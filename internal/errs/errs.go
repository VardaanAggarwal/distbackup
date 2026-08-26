// Package errs defines distbackup's typed error hierarchy and the
// classification helpers the retry and pipeline layers depend on.
//
// The design goal is that a caller never inspects an error's text to decide
// what to do. Every decision — retry or not, fail the run or skip the item,
// report corruption — is driven by a Kind that survives wrapping.
package errs

import (
	"context"
	"errors"
	"fmt"
)

// Kind classifies an error into the categories the engine actually branches on.
//
// This is a small closed set on purpose. Every Kind here exists because some
// caller makes a different decision because of it; a category nothing branches
// on is noise. Rejected: a sentinel error per failure mode, which multiplies
// without bound and pushes classification logic into every call site.
type Kind int

const (
	// KindUnknown is an unclassified error. Treated as permanent: the engine
	// will not retry something it does not understand, because retrying an
	// unknown failure is how a bug becomes an infinite loop.
	KindUnknown Kind = iota

	// KindNotFound means the requested object does not exist.
	KindNotFound

	// KindAlreadyExists means a create-only write lost a race. For
	// content-addressed data this is usually success, not failure.
	KindAlreadyExists

	// KindCorrupt means stored data failed an integrity check: a checksum
	// mismatch, a truncated pack, a bad magic number. Never retryable —
	// the bytes on disk are wrong and asking again will return the same
	// wrong bytes.
	KindCorrupt

	// KindThrottled means the provider is rate-limiting. Retryable, and the
	// signal to back off harder than an ordinary transient failure.
	KindThrottled

	// KindTransient means a temporary failure: a 5xx, a dropped connection,
	// a timeout. Retryable.
	KindTransient

	// KindExpired means a credential or token is no longer valid. Not
	// retryable as-is; the caller must obtain a fresh token first. This is
	// separate from KindTransient because retrying with the same expired
	// token can never succeed, and EBS block tokens make this a routine
	// case rather than an exotic one.
	KindExpired

	// KindInvalid means the request was malformed or violated a constraint.
	// A bug in this program, not a condition to retry.
	KindInvalid

	// KindUnsupported means the operation is not implemented by this
	// provider or format version.
	KindUnsupported

	// KindCanceled means the context was canceled or its deadline passed.
	// Never retryable — the caller has already given up.
	KindCanceled
)

// String returns a stable lowercase name for the Kind, used in logs.
func (k Kind) String() string {
	switch k {
	case KindNotFound:
		return "not_found"
	case KindAlreadyExists:
		return "already_exists"
	case KindCorrupt:
		return "corrupt"
	case KindThrottled:
		return "throttled"
	case KindTransient:
		return "transient"
	case KindExpired:
		return "expired"
	case KindInvalid:
		return "invalid"
	case KindUnsupported:
		return "unsupported"
	case KindCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// Error is distbackup's error type. It carries a Kind, the operation that
// failed, and the underlying cause.
type Error struct {
	// Kind drives every retry and control-flow decision.
	Kind Kind
	// Op names the operation, e.g. "pack.Write" or "s3.PutIfAbsent".
	Op string
	// Err is the wrapped cause. May be nil.
	Err error
}

// Error implements the error interface.
func (e *Error) Error() string {
	switch {
	case e.Op != "" && e.Err != nil:
		return fmt.Sprintf("%s: %s: %v", e.Op, e.Kind, e.Err)
	case e.Op != "":
		return fmt.Sprintf("%s: %s", e.Op, e.Kind)
	case e.Err != nil:
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	default:
		return e.Kind.String()
	}
}

// Unwrap supports errors.Is and errors.As through the chain.
func (e *Error) Unwrap() error { return e.Err }

// E constructs an Error. Callers wrap with %w through this rather than
// fmt.Errorf so that Kind is preserved for classification.
func E(kind Kind, op string, err error) *Error {
	return &Error{Kind: kind, Op: op, Err: err}
}

// KindOf extracts the Kind from an error, walking the wrap chain.
//
// Context cancellation is special-cased first: a canceled context can be
// wrapped by any layer, and every one of them would otherwise have to
// remember to classify it. Getting this wrong means a shutting-down pipeline
// retries its way through a backoff schedule instead of exiting.
func KindOf(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return KindCanceled
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindUnknown
}

// IsRetryable reports whether an operation that produced err is worth
// attempting again.
//
// Only two kinds qualify. Everything else — including KindUnknown — is
// permanent by default. Defaulting to "do not retry" means an unclassified
// provider error surfaces as a clear failure instead of silently consuming
// the whole backoff schedule.
func IsRetryable(err error) bool {
	switch KindOf(err) {
	case KindThrottled, KindTransient:
		return true
	default:
		return false
	}
}

// IsThrottle reports whether err is a rate-limit signal. The retry layer
// backs off more aggressively for these than for ordinary transient errors.
func IsThrottle(err error) bool { return KindOf(err) == KindThrottled }

// IsNotFound reports whether err indicates a missing object.
func IsNotFound(err error) bool { return KindOf(err) == KindNotFound }

// IsCorrupt reports whether err indicates a failed integrity check.
func IsCorrupt(err error) bool { return KindOf(err) == KindCorrupt }

// IsAlreadyExists reports whether err indicates a lost create-only race.
func IsAlreadyExists(err error) bool { return KindOf(err) == KindAlreadyExists }
