package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fake is an in-memory implementation of the S3 subset this package uses.
//
// Under CLAUDE.md R7 the real client is never run, so this is the only thing
// that ever exercises Store. It therefore models the semantics that matter
// rather than just storing bytes:
//
//   - `If-None-Match: *` returns 412 when the key exists
//   - a configurable number of 409 ConditionalRequestConflict responses, so
//     the retry path for an unsettled write is actually taken
//   - paginated listings that never return everything at once
//   - RFC 7233 inclusive byte ranges, including ranges that run past the end
//   - 503 SlowDown throttling
//
// The zero value is not usable; call NewFake.
type Fake struct {
	mu      sync.Mutex
	objects map[string]fakeObject

	// conflictsRemaining makes the next n conditional writes fail with 409.
	conflictsRemaining int
	// throttleEvery makes every nth call return 503 SlowDown.
	throttleEvery int
	callCount     int
	// pageSize caps a listing page, so pagination is always exercised.
	pageSize int
	now      func() time.Time
}

type fakeObject struct {
	data     []byte
	modified time.Time
}

// FakeOption configures a Fake.
type FakeOption func(*Fake)

// WithConflicts makes the next n conditional writes return 409
// ConditionalRequestConflict, which the client must retry rather than treat
// as "already exists".
func WithConflicts(n int) FakeOption {
	return func(f *Fake) { f.conflictsRemaining = n }
}

// WithThrottleEvery makes every nth call return 503 SlowDown.
func WithThrottleEvery(n int) FakeOption {
	return func(f *Fake) { f.throttleEvery = n }
}

// WithPageSize caps how many keys a listing returns per page.
func WithPageSize(n int) FakeOption {
	return func(f *Fake) { f.pageSize = n }
}

// NewFake returns an empty Fake.
func NewFake(opts ...FakeOption) *Fake {
	f := &Fake{
		objects:  make(map[string]fakeObject),
		pageSize: 3, // small on purpose: pagination is always exercised
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// ObjectCount returns how many objects the fake holds.
func (f *Fake) ObjectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

// throttleIfDue returns a 503 when one is scheduled. Caller holds f.mu.
func (f *Fake) throttleIfDue() error {
	f.callCount++
	if f.throttleEvery > 0 && f.callCount%f.throttleEvery == 0 {
		return &APIError{StatusCode: 503, Code: "SlowDown", Message: "Please reduce your request rate"}
	}
	return nil
}

// PutObject implements API.
func (f *Fake) PutObject(ctx context.Context, in PutObjectInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.throttleIfDue(); err != nil {
		return err
	}

	if in.IfNoneMatch == IfNoneMatchAny {
		// A conflicting operation is in flight. The outcome is unknown, so
		// the client must retry rather than assume anything.
		if f.conflictsRemaining > 0 {
			f.conflictsRemaining--
			return &APIError{
				StatusCode: StatusConflict,
				Code:       "ConditionalRequestConflict",
				Message:    "A conflicting conditional write is in progress",
			}
		}
		if _, exists := f.objects[in.Key]; exists {
			return &APIError{
				StatusCode: StatusPreconditionFailed,
				Code:       "PreconditionFailed",
				Message:    "At least one of the pre-conditions you specified did not hold",
			}
		}
	}

	data := make([]byte, len(in.Body))
	copy(data, in.Body)
	f.objects[in.Key] = fakeObject{data: data, modified: f.now()}
	return nil
}

// GetObject implements API.
func (f *Fake) GetObject(ctx context.Context, in GetObjectInput) (*GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.throttleIfDue(); err != nil {
		return nil, err
	}

	obj, ok := f.objects[in.Key]
	if !ok {
		return nil, &APIError{StatusCode: 404, Code: "NoSuchKey", Message: "The specified key does not exist"}
	}

	data := obj.data
	if in.Range != "" {
		var err error
		data, err = applyRange(obj.data, in.Range)
		if err != nil {
			return nil, err
		}
	}

	out := make([]byte, len(data))
	copy(out, data)
	return &GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(out)),
		ContentLength: int64(len(out)),
		LastModified:  obj.modified,
	}, nil
}

// applyRange implements RFC 7233 byte ranges as S3 does.
//
// Both ends are inclusive, and a range whose end runs past the object is
// satisfied with whatever is available rather than rejected — which the pack
// reader relies on, because it guesses a tail size that may exceed the object.
func applyRange(data []byte, header string) ([]byte, error) {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return nil, &APIError{StatusCode: 400, Code: "InvalidRange", Message: "malformed range"}
	}

	startStr, endStr, hasEnd := strings.Cut(spec, "-")
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return nil, &APIError{StatusCode: 400, Code: "InvalidRange", Message: "malformed range start"}
	}
	if start >= int64(len(data)) {
		// S3 returns 416 when the range starts past the end of the object.
		return nil, &APIError{
			StatusCode: 416, Code: "InvalidRange",
			Message: fmt.Sprintf("range start %d is past the object length %d", start, len(data)),
		}
	}

	end := int64(len(data)) - 1
	if hasEnd && endStr != "" {
		parsed, perr := strconv.ParseInt(endStr, 10, 64)
		if perr != nil {
			return nil, &APIError{StatusCode: 400, Code: "InvalidRange", Message: "malformed range end"}
		}
		// Clamp rather than reject: a range that overshoots is satisfied
		// with what exists.
		if parsed < end {
			end = parsed
		}
	}
	if end < start {
		return nil, &APIError{StatusCode: 400, Code: "InvalidRange", Message: "range end precedes start"}
	}
	return data[start : end+1], nil
}

// HeadObject implements API.
func (f *Fake) HeadObject(ctx context.Context, in HeadObjectInput) (*HeadObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.throttleIfDue(); err != nil {
		return nil, err
	}

	obj, ok := f.objects[in.Key]
	if !ok {
		return nil, &APIError{StatusCode: 404, Code: "NotFound", Message: "Not Found"}
	}
	return &HeadObjectOutput{ContentLength: int64(len(obj.data)), LastModified: obj.modified}, nil
}

// ListObjects implements API.
func (f *Fake) ListObjects(ctx context.Context, in ListObjectsInput) (*ListObjectsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.throttleIfDue(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, in.Prefix) {
			keys = append(keys, k)
		}
	}
	// S3 lists in lexicographic order. The engine must not depend on that —
	// the conformance suite sorts before comparing — but the fake reproduces
	// it because pagination needs a stable order to resume from.
	sort.Strings(keys)

	start := 0
	if in.ContinuationToken != "" {
		for i, k := range keys {
			if k > in.ContinuationToken {
				start = i
				break
			}
			start = i + 1
		}
	}

	limit := f.pageSize
	if in.MaxKeys > 0 && int(in.MaxKeys) < limit {
		limit = int(in.MaxKeys)
	}

	out := &ListObjectsOutput{}
	for i := start; i < len(keys) && len(out.Objects) < limit; i++ {
		obj := f.objects[keys[i]]
		out.Objects = append(out.Objects, ObjectSummary{
			Key:          keys[i],
			Size:         int64(len(obj.data)),
			LastModified: obj.modified,
		})
	}

	if n := start + len(out.Objects); n < len(keys) {
		out.NextContinuationToken = out.Objects[len(out.Objects)-1].Key
	}
	return out, nil
}

// DeleteObject implements API.
func (f *Fake) DeleteObject(ctx context.Context, in DeleteObjectInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.throttleIfDue(); err != nil {
		return err
	}

	// S3 DELETE is idempotent: removing a key that is not there succeeds.
	delete(f.objects, in.Key)
	return nil
}

var _ API = (*Fake)(nil)
