// Package s3 implements store.ObjectStore over the Amazon S3 API.
//
// # Nothing here has ever run against AWS
//
// docs/ENGINEERING-RULES.md R7 forbids it, and the CLI deliberately offers no way to select
// this backend. Everything below was written against the published API
// reference (checked 2026-08-26) and is exercised against Fake, which models
// S3's conditional-write semantics including the failure modes.
//
// # Why this package exists at all
//
// It is the second implementation of store.ObjectStore, and a second
// implementation is what turns "we have an interface" into evidence that the
// interface is actually an abstraction. It passes the same conformance suite
// as the local filesystem backend (internal/store/storetest), byte for byte
// the same assertions — which is the mechanical check that the engine cannot
// depend on filesystem-specific behaviour without noticing.
//
// The genuinely interesting part is PutIfAbsent. S3 signals "this key already
// exists" and "someone else is writing right now" as two different HTTP
// statuses that mean two different things, and collapsing them loses data.
// See D-005 and D-006.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/retry"
	"github.com/VardaanAggarwal/distbackup/internal/store"
)

// Conditional-write header values, verified 2026-08-26.
//
// "If-None-Match: uploads the object only if the object key name does not
// already exist in the specified bucket. Otherwise, Amazon S3 returns a 412
// Precondition Failed error. It expects the * character (asterisk)."
const IfNoneMatchAny = "*"

// HTTP statuses this package distinguishes.
//
// These two are the whole reason PutIfAbsent needs care:
//
//   - 412 PreconditionFailed: the key exists. Settled. For content-addressed
//     data the bytes under that key are by definition the bytes we were about
//     to write, so this is *success* — the blob is stored, just not by us.
//   - 409 ConditionalRequestConflict: a conflicting operation was in flight.
//     Unsettled. Nothing is known about whether the object landed, so the
//     request must be retried. Treating this as "already exists" would skip a
//     blob that may never have been written — silent data loss on restore.
const (
	StatusPreconditionFailed = 412
	StatusConflict           = 409
)

// PutObjectInput is a conditional or unconditional write.
type PutObjectInput struct {
	Bucket string
	Key    string
	Body   []byte
	// IfNoneMatch, when set to IfNoneMatchAny, makes the write conditional on
	// the key not already existing.
	IfNoneMatch string
}

// GetObjectInput reads an object or a byte range of one.
type GetObjectInput struct {
	Bucket string
	Key    string
	// Range is an HTTP range header value such as "bytes=0-1023", or empty
	// for the whole object.
	Range string
}

// GetObjectOutput is a read response.
type GetObjectOutput struct {
	// Body must be drained and closed by the caller.
	Body          io.ReadCloser
	ContentLength int64
	LastModified  time.Time
}

// HeadObjectInput requests object metadata.
type HeadObjectInput struct {
	Bucket string
	Key    string
}

// HeadObjectOutput is object metadata.
type HeadObjectOutput struct {
	ContentLength int64
	LastModified  time.Time
}

// ListObjectsInput lists a prefix, one page at a time.
type ListObjectsInput struct {
	Bucket            string
	Prefix            string
	ContinuationToken string
	MaxKeys           int32
}

// ListObjectsOutput is one page of a listing.
type ListObjectsOutput struct {
	Objects []ObjectSummary
	// NextContinuationToken is empty on the last page.
	NextContinuationToken string
}

// ObjectSummary describes one listed object.
type ObjectSummary struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// DeleteObjectInput removes an object.
type DeleteObjectInput struct {
	Bucket string
	Key    string
}

// APIError carries an S3 HTTP status so PutIfAbsent can distinguish 412 from
// 409 without matching on error strings.
type APIError struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Code is the S3 error code, e.g. "PreconditionFailed".
	Code string
	// Message is the human-readable detail.
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("s3: %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// API is the subset of S3 this package uses.
//
// Declared here rather than depending on the SDK's client type, so Store can
// be exercised against Fake — which under R7 is the only way it is ever
// exercised at all.
type API interface {
	PutObject(ctx context.Context, in PutObjectInput) error
	GetObject(ctx context.Context, in GetObjectInput) (*GetObjectOutput, error)
	HeadObject(ctx context.Context, in HeadObjectInput) (*HeadObjectOutput, error)
	ListObjects(ctx context.Context, in ListObjectsInput) (*ListObjectsOutput, error)
	DeleteObject(ctx context.Context, in DeleteObjectInput) error
}

// Store implements store.ObjectStore over S3.
type Store struct {
	api    API
	bucket string
	prefix string
	policy retry.Policy
}

// Option configures a Store.
type Option func(*Store)

// WithPrefix confines the repository to a key prefix within the bucket, so
// one bucket can hold several repositories.
func WithPrefix(p string) Option {
	return func(s *Store) { s.prefix = strings.Trim(p, "/") }
}

// WithRetryPolicy overrides the retry schedule.
func WithRetryPolicy(p retry.Policy) Option {
	return func(s *Store) { s.policy = p }
}

// New returns a Store writing into bucket through api.
func New(api API, bucket string, opts ...Option) (*Store, error) {
	const op = "s3.New"

	if api == nil {
		return nil, errs.E(errs.KindInvalid, op, errors.New("nil API"))
	}
	if bucket == "" {
		return nil, errs.E(errs.KindInvalid, op, errors.New("empty bucket"))
	}

	s := &Store{api: api, bucket: bucket, policy: retry.DefaultPolicy()}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// fullKey applies the configured prefix after validating the caller's key.
func (s *Store) fullKey(key string) (string, error) {
	if err := store.ValidateKey(key); err != nil {
		return "", err
	}
	if s.prefix == "" {
		return key, nil
	}
	return s.prefix + "/" + key, nil
}

// trimPrefix removes the configured prefix from a listed key.
func (s *Store) trimPrefix(key string) string {
	if s.prefix == "" {
		return key
	}
	return strings.TrimPrefix(key, s.prefix+"/")
}

// Put writes an object, replacing any existing one.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	const op = "s3.Put"

	if err := ctx.Err(); err != nil {
		return errs.E(errs.KindCanceled, op, err)
	}
	full, err := s.fullKey(key)
	if err != nil {
		return err
	}

	return retry.Do(ctx, s.policy, op, func(ctx context.Context, _ int) error {
		return classify(op, s.api.PutObject(ctx, PutObjectInput{
			Bucket: s.bucket, Key: full, Body: data,
		}))
	})
}

// PutIfAbsent writes an object only if the key is free.
//
// The implementation is short and every line of it matters:
//
//   - `If-None-Match: *` makes S3 itself perform the check, atomically.
//     There is no read-then-write here and there must never be: two clients
//     racing on the same key would both read "absent" and both write.
//   - A 412 means the key already exists. Returned as (false, nil) — success,
//     because for content-addressed data an existing object under this key
//     holds exactly the bytes we were going to write (D-005).
//   - A 409 means a conflicting write was in flight and nothing is known
//     about the outcome. Returned as a retryable error so the retry layer
//     asks again (D-006). Collapsing it into the 412 case would report a blob
//     as stored when it may not be — the restore would fail much later, on
//     data the user believed was safe.
func (s *Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	const op = "s3.PutIfAbsent"

	if err := ctx.Err(); err != nil {
		return false, errs.E(errs.KindCanceled, op, err)
	}
	full, err := s.fullKey(key)
	if err != nil {
		return false, err
	}

	created := false
	err = retry.Do(ctx, s.policy, op, func(ctx context.Context, _ int) error {
		putErr := s.api.PutObject(ctx, PutObjectInput{
			Bucket: s.bucket, Key: full, Body: data, IfNoneMatch: IfNoneMatchAny,
		})
		if putErr == nil {
			created = true
			return nil
		}

		var apiErr *APIError
		if errors.As(putErr, &apiErr) && apiErr.StatusCode == StatusPreconditionFailed {
			// The key exists. Settled, and success for our purposes.
			created = false
			return nil
		}
		// Everything else, 409 included, goes through classify — which marks
		// 409 retryable so this closure runs again.
		return classify(op, putErr)
	})
	if err != nil {
		return false, err
	}
	return created, nil
}

// Get returns the whole object.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.getRange(ctx, "s3.Get", key, "")
}

// GetRange returns n bytes starting at off, or to the end if n is negative.
func (s *Store) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	const op = "s3.GetRange"

	if off < 0 {
		return nil, errs.E(errs.KindInvalid, op, errors.New("negative offset"))
	}
	if n == 0 {
		// An empty range is not expressible as an HTTP range header, and S3
		// would reject "bytes=x-(x-1)". Short-circuit rather than send
		// something the service will refuse.
		return io.NopCloser(strings.NewReader("")), nil
	}

	// RFC 7233 ranges are inclusive at both ends, so the last byte is
	// off+n-1, not off+n. Getting this wrong reads one byte too many, which
	// for a pack tail means a header that fails to parse.
	rangeHeader := fmt.Sprintf("bytes=%d-", off)
	if n > 0 {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", off, off+n-1)
	}
	return s.getRange(ctx, op, key, rangeHeader)
}

func (s *Store) getRange(ctx context.Context, op, key, rangeHeader string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}
	full, err := s.fullKey(key)
	if err != nil {
		return nil, err
	}

	out, err := retry.DoValue(ctx, s.policy, op,
		func(ctx context.Context, _ int) (*GetObjectOutput, error) {
			o, gerr := s.api.GetObject(ctx, GetObjectInput{
				Bucket: s.bucket, Key: full, Range: rangeHeader,
			})
			return o, classify(op, gerr)
		})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Stat returns metadata for one object.
func (s *Store) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	const op = "s3.Stat"

	if err := ctx.Err(); err != nil {
		return store.ObjectInfo{}, errs.E(errs.KindCanceled, op, err)
	}
	full, err := s.fullKey(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}

	out, err := retry.DoValue(ctx, s.policy, op,
		func(ctx context.Context, _ int) (*HeadObjectOutput, error) {
			o, herr := s.api.HeadObject(ctx, HeadObjectInput{Bucket: s.bucket, Key: full})
			return o, classify(op, herr)
		})
	if err != nil {
		return store.ObjectInfo{}, err
	}
	return store.ObjectInfo{Key: key, Size: out.ContentLength, ModTime: out.LastModified}, nil
}

// List walks every object under prefix.
//
// It pages until the continuation token is empty, never stopping on an empty
// page — the same rule the EBS listing follows, and for the same reason: a
// short or empty page does not mean the end of the data.
func (s *Store) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	const op = "s3.List"

	searchPrefix := prefix
	if s.prefix != "" {
		searchPrefix = s.prefix + "/" + prefix
	}

	var token string
	for {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}

		out, err := retry.DoValue(ctx, s.policy, op,
			func(ctx context.Context, _ int) (*ListObjectsOutput, error) {
				o, lerr := s.api.ListObjects(ctx, ListObjectsInput{
					Bucket:            s.bucket,
					Prefix:            searchPrefix,
					ContinuationToken: token,
					MaxKeys:           1000,
				})
				return o, classify(op, lerr)
			})
		if err != nil {
			return err
		}

		for _, obj := range out.Objects {
			if err := ctx.Err(); err != nil {
				return errs.E(errs.KindCanceled, op, err)
			}
			if err := fn(store.ObjectInfo{
				Key:     s.trimPrefix(obj.Key),
				Size:    obj.Size,
				ModTime: obj.LastModified,
			}); err != nil {
				return err
			}
		}

		if out.NextContinuationToken == "" {
			return nil
		}
		token = out.NextContinuationToken
	}
}

// Delete removes an object. Deleting a missing key is not an error — S3
// itself treats DELETE as idempotent, which matches the interface contract.
func (s *Store) Delete(ctx context.Context, key string) error {
	const op = "s3.Delete"

	if err := ctx.Err(); err != nil {
		return errs.E(errs.KindCanceled, op, err)
	}
	full, err := s.fullKey(key)
	if err != nil {
		return err
	}

	return retry.Do(ctx, s.policy, op, func(ctx context.Context, _ int) error {
		derr := classify(op, s.api.DeleteObject(ctx, DeleteObjectInput{Bucket: s.bucket, Key: full}))
		if errs.IsNotFound(derr) {
			return nil
		}
		return derr
	})
}

// Close releases resources. The client holds none of its own.
func (s *Store) Close() error { return nil }

// classify maps S3 errors into the engine's taxonomy.
//
// This is the provider's job under R11: the engine decides whether to retry
// from errs.Kind, and translating HTTP statuses into that vocabulary belongs
// here, not in the core.
func classify(op string, err error) error {
	if err == nil {
		return nil
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		// Not an S3 protocol error — a transport failure, most likely.
		// Transient so the retry layer gets a chance.
		return errs.E(errs.KindTransient, op, err)
	}

	switch {
	case apiErr.StatusCode == 404:
		return errs.E(errs.KindNotFound, op, err)
	case apiErr.StatusCode == StatusPreconditionFailed:
		return errs.E(errs.KindAlreadyExists, op, err)
	case apiErr.StatusCode == StatusConflict:
		// Retryable: the outcome is genuinely unknown. See PutIfAbsent.
		return errs.E(errs.KindTransient, op, err)
	case apiErr.StatusCode == 403:
		return errs.E(errs.KindPermission, op, err)
	case apiErr.StatusCode == 503 || apiErr.Code == "SlowDown":
		return errs.E(errs.KindThrottled, op, err)
	case apiErr.StatusCode >= 500:
		// Documented guidance is to retry 5xx.
		return errs.E(errs.KindTransient, op, err)
	case apiErr.StatusCode >= 400:
		return errs.E(errs.KindInvalid, op, err)
	default:
		return errs.E(errs.KindUnknown, op, err)
	}
}

var _ store.ObjectStore = (*Store)(nil)
