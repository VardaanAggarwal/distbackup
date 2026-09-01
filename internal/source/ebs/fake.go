package ebs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

// Fake is an in-memory implementation of the EBS direct APIs.
//
// It is not a stub. Under docs/ENGINEERING-RULES.md R7 no code in this package will ever run
// against AWS, so this is the only thing that ever exercises Source — which
// makes its fidelity the project's entire evidence that the client is
// correct. It therefore reproduces the *documented failure modes*, not the
// happy path:
//
//   - empty pages that still carry a continuation token
//   - MaxResults clamped up to 100
//   - pagination tokens that expire after 60 minutes
//   - block tokens that expire after 7 days
//   - block tokens invalidated by a subsequent listing (the strict reading of
//     an ambiguity in the docs — see docs/OPEN_QUESTIONS.md Q-001)
//   - throttling and 5xx responses
//   - response bodies that must be drained and closed, tracked so a leak
//     fails a test
//
// A fake that only did the happy path would let every one of those bugs ship.
//
// The zero value is not usable; call NewFake.
type Fake struct {
	mu sync.Mutex

	// blocks maps block index to contents. Sparse by design: only written
	// blocks are present, matching "It returns only block indexes and tokens
	// that have data written to them."
	blocks map[int64][]byte

	// volumeSizeGB is reported by list operations.
	volumeSizeGB int64

	// now is the fake's clock, so token expiry can be tested without
	// sleeping. Injectable rather than time.Now for the obvious reason: a
	// test for a 60-minute expiry cannot wait 60 minutes.
	now func() time.Time

	// generation increments on every listing. Block tokens embed the
	// generation that issued them, which is how the fake implements the
	// strict reading of Q-001.
	generation int

	// validTokens maps a live block token to its block index.
	validTokens map[string]int64
	// tokenExpiry maps a block token to when it stops working.
	tokenExpiry map[string]time.Time
	// pageTokens maps a pagination token to its resume position and issue time.
	pageTokens map[string]pageState

	// openBodies counts response bodies handed out but not yet closed.
	openBodies int
	// maxOpenBodies is the high-water mark, so a test can assert the client
	// never holds more than one body at a time.
	maxOpenBodies int

	// Fault injection.
	failNext      []error
	throttleEvery int
	callCount     int
	emptyPages    int
	corruptBlocks map[int64]bool

	// invalidateOnRelist controls whether a new listing kills old tokens.
	invalidateOnRelist bool

	// pageSize caps entries per page below MaxResults.
	//
	// This is documented behaviour, not an artificial test hook: "Even if
	// additional blocks can be retrieved from the snapshot, the request can
	// return less blocks than MaxResults." A client that assumed a full page
	// meant more data, or a short page meant the end, would be wrong.
	pageSize int
}

type pageState struct {
	nextIndex int64
	issuedAt  time.Time
}

// FakeOption configures a Fake.
type FakeOption func(*Fake)

// WithClock replaces the fake's clock, so token expiry can be exercised
// without waiting.
func WithClock(now func() time.Time) FakeOption {
	return func(f *Fake) { f.now = now }
}

// WithThrottleEvery makes every nth call return a throttling error.
func WithThrottleEvery(n int) FakeOption {
	return func(f *Fake) { f.throttleEvery = n }
}

// WithEmptyPages makes the fake emit n empty pages, each with a non-null
// continuation token, before returning any data.
//
// This reproduces the documented behaviour that catches people out: "If the
// changed blocks are scarce in the snapshot, the response may be empty but
// the API will return a next page token value."
func WithEmptyPages(n int) FakeOption {
	return func(f *Fake) { f.emptyPages = n }
}

// WithTokenInvalidationOnRelist implements the strict reading of Q-001: a new
// listing invalidates block tokens issued by a previous one.
func WithTokenInvalidationOnRelist() FakeOption {
	return func(f *Fake) { f.invalidateOnRelist = true }
}

// WithPageSize makes the fake return at most n entries per page, regardless
// of the MaxResults requested. See the pageSize field.
func WithPageSize(n int) FakeOption {
	return func(f *Fake) { f.pageSize = n }
}

// WithCorruptBlock makes GetSnapshotBlock return data that does not match the
// checksum it reports, for the given block index.
func WithCorruptBlock(index int64) FakeOption {
	return func(f *Fake) {
		if f.corruptBlocks == nil {
			f.corruptBlocks = map[int64]bool{}
		}
		f.corruptBlocks[index] = true
	}
}

// NewFake returns a Fake holding the given blocks, keyed by block index.
func NewFake(blocks map[int64][]byte, volumeSizeGB int64, opts ...FakeOption) *Fake {
	f := &Fake{
		blocks:       make(map[int64][]byte, len(blocks)),
		volumeSizeGB: volumeSizeGB,
		now:          time.Now,
		validTokens:  make(map[string]int64),
		tokenExpiry:  make(map[string]time.Time),
		pageTokens:   make(map[string]pageState),
	}
	for i, b := range blocks {
		cp := make([]byte, len(b))
		copy(cp, b)
		f.blocks[i] = cp
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FailNext queues errors to be returned by the next calls, one each.
func (f *Fake) FailNext(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext = append(f.failNext, errs...)
}

// OpenBodies returns how many response bodies are currently unclosed.
// A test asserts this is zero after the client is done.
func (f *Fake) OpenBodies() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.openBodies
}

// MaxOpenBodies returns the highest number of simultaneously open bodies.
func (f *Fake) MaxOpenBodies() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxOpenBodies
}

// CallCount returns how many API calls have been made, for cost assertions.
func (f *Fake) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

// nextError returns an injected fault, if one is due. Caller holds f.mu.
func (f *Fake) nextError(op string) error {
	f.callCount++
	if len(f.failNext) > 0 {
		err := f.failNext[0]
		f.failNext = f.failNext[1:]
		return err
	}
	if f.throttleEvery > 0 && f.callCount%f.throttleEvery == 0 {
		return errs.E(errs.KindThrottled, op, errors.New("RequestThrottledException"))
	}
	return nil
}

// sortedIndexes returns the present block indexes in numerical order, which
// the real service guarantees.
func (f *Fake) sortedIndexes() []int64 {
	out := make([]int64, 0, len(f.blocks))
	for i := range f.blocks {
		out = append(out, i)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

// clampMaxResults reproduces the documented clamp.
//
// "Can I submit a request with a MaxResults parameter value of under 100? No.
// The minimum MaxResult parameter value you can use is 100. If you submit a
// request with a MaxResult parameter value of under 100, and there are more
// than 100 blocks in the snapshot, then the API will return at least 100
// results." So it clamps rather than erroring.
func clampMaxResults(n int32) int32 {
	switch {
	case n <= 0:
		return DefaultMaxResults
	case n < MinMaxResults:
		return MinMaxResults
	case n > MaxMaxResults:
		return MaxMaxResults
	default:
		return n
	}
}

// ListSnapshotBlocks implements API.
func (f *Fake) ListSnapshotBlocks(ctx context.Context, in ListSnapshotBlocksInput) (*ListSnapshotBlocksOutput, error) {
	const op = "fake.ListSnapshotBlocks"

	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.nextError(op); err != nil {
		return nil, err
	}
	if in.SnapshotID == "" {
		return nil, errs.E(errs.KindInvalid, op, errors.New("ValidationException: missing snapshot ID"))
	}

	start := in.StartingBlockIndex
	now := f.now()

	// Resolve the continuation token first: "If you specify NextToken, then
	// StartingBlockIndex is ignored."
	if in.NextToken != "" {
		st, ok := f.pageTokens[in.NextToken]
		if !ok {
			return nil, errs.E(errs.KindInvalid, op, errors.New("ValidationException: unknown page token"))
		}
		if now.Sub(st.issuedAt) > NextTokenLifetime {
			return nil, errs.E(errs.KindExpired, op,
				fmt.Errorf("ValidationException: page token expired (issued %v ago, limit %v)",
					now.Sub(st.issuedAt), NextTokenLifetime))
		}
		start = st.nextIndex
	} else {
		// A fresh listing. Under the strict reading of Q-001 this
		// invalidates every block token a previous listing handed out.
		f.generation++
		if f.invalidateOnRelist {
			f.validTokens = make(map[string]int64)
			f.tokenExpiry = make(map[string]time.Time)
		}
	}

	// Emit the configured number of empty-but-continuing pages first.
	if in.NextToken == "" && f.emptyPages > 0 {
		token := f.issuePageToken(start, now)
		f.emptyPages--
		return &ListSnapshotBlocksOutput{
			Blocks:     nil, // empty page …
			BlockSize:  BlockSize,
			ExpiryTime: now.Add(BlockTokenLifetime),
			NextToken:  token, // … with a continuation token
			VolumeSize: f.volumeSizeGB,
		}, nil
	}

	limit := int(clampMaxResults(in.MaxResults))
	if f.pageSize > 0 && f.pageSize < limit {
		limit = f.pageSize
	}
	indexes := f.sortedIndexes()

	out := &ListSnapshotBlocksOutput{
		BlockSize:  BlockSize,
		ExpiryTime: now.Add(BlockTokenLifetime),
		VolumeSize: f.volumeSizeGB,
	}

	var lastEmitted int64 = -1
	for _, idx := range indexes {
		if idx < start {
			continue
		}
		if len(out.Blocks) >= limit {
			break
		}
		token := f.issueBlockToken(idx, now)
		out.Blocks = append(out.Blocks, Block{BlockIndex: idx, BlockToken: token})
		lastEmitted = idx
	}

	if lastEmitted >= 0 && f.hasBlockAfter(indexes, lastEmitted) {
		out.NextToken = f.issuePageToken(lastEmitted+1, now)
	}
	return out, nil
}

func (f *Fake) hasBlockAfter(indexes []int64, after int64) bool {
	for _, i := range indexes {
		if i > after {
			return true
		}
	}
	return false
}

func (f *Fake) issueBlockToken(index int64, now time.Time) string {
	token := base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("blk-%d-gen%d", index, f.generation)))
	f.validTokens[token] = index
	f.tokenExpiry[token] = now.Add(BlockTokenLifetime)
	return token
}

func (f *Fake) issuePageToken(nextIndex int64, now time.Time) string {
	token := base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("page-%d-gen%d-%d", nextIndex, f.generation, len(f.pageTokens))))
	f.pageTokens[token] = pageState{nextIndex: nextIndex, issuedAt: now}
	return token
}

// ListChangedBlocks implements API.
//
// changedIndexes are the blocks that differ; newIndexes is the subset that
// does not exist in the first snapshot at all and must therefore be reported
// with an absent FirstBlockToken.
func (f *Fake) ListChangedBlocks(ctx context.Context, in ListChangedBlocksInput) (*ListChangedBlocksOutput, error) {
	const op = "fake.ListChangedBlocks"

	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.nextError(op); err != nil {
		return nil, err
	}
	if in.FirstSnapshotID == "" || in.SecondSnapshotID == "" {
		return nil, errs.E(errs.KindInvalid, op,
			errors.New("ValidationException: both snapshot IDs are required"))
	}

	now := f.now()
	start := in.StartingBlockIndex

	if in.NextToken != "" {
		st, ok := f.pageTokens[in.NextToken]
		if !ok {
			return nil, errs.E(errs.KindInvalid, op, errors.New("ValidationException: unknown page token"))
		}
		if now.Sub(st.issuedAt) > NextTokenLifetime {
			return nil, errs.E(errs.KindExpired, op, errors.New("ValidationException: page token expired"))
		}
		start = st.nextIndex
	} else {
		f.generation++
	}

	if in.NextToken == "" && f.emptyPages > 0 {
		token := f.issuePageToken(start, now)
		f.emptyPages--
		return &ListChangedBlocksOutput{
			ChangedBlocks: nil,
			BlockSize:     BlockSize,
			ExpiryTime:    now.Add(BlockTokenLifetime),
			NextToken:     token,
			VolumeSize:    f.volumeSizeGB,
		}, nil
	}

	limit := int(clampMaxResults(in.MaxResults))
	if f.pageSize > 0 && f.pageSize < limit {
		limit = f.pageSize
	}
	indexes := f.sortedIndexes()

	out := &ListChangedBlocksOutput{
		BlockSize:  BlockSize,
		ExpiryTime: now.Add(BlockTokenLifetime),
		VolumeSize: f.volumeSizeGB,
	}

	var lastEmitted int64 = -1
	for _, idx := range indexes {
		if idx < start {
			continue
		}
		if len(out.ChangedBlocks) >= limit {
			break
		}
		cb := ChangedBlock{
			BlockIndex:       idx,
			SecondBlockToken: f.issueBlockToken(idx, now),
		}
		// Odd indexes are treated as pre-existing (so they carry a first
		// token) and even ones as new. Arbitrary, but it means every test
		// exercises both branches of the absent-token rule rather than only
		// the common one.
		if idx%2 == 1 {
			cb.FirstBlockToken = base64.StdEncoding.EncodeToString(
				[]byte(fmt.Sprintf("old-%d", idx)))
		}
		out.ChangedBlocks = append(out.ChangedBlocks, cb)
		lastEmitted = idx
	}

	if lastEmitted >= 0 && f.hasBlockAfter(indexes, lastEmitted) {
		out.NextToken = f.issuePageToken(lastEmitted+1, now)
	}
	return out, nil
}

// GetSnapshotBlock implements API.
func (f *Fake) GetSnapshotBlock(ctx context.Context, in GetSnapshotBlockInput) (*GetSnapshotBlockOutput, error) {
	const op = "fake.GetSnapshotBlock"

	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.nextError(op); err != nil {
		return nil, err
	}
	if in.BlockToken == "" {
		return nil, errs.E(errs.KindInvalid, op, errors.New("ValidationException: missing block token"))
	}

	idx, ok := f.validTokens[in.BlockToken]
	if !ok {
		// Either never issued, or invalidated by a re-listing (Q-001).
		return nil, errs.E(errs.KindInvalid, op,
			errors.New("ValidationException: unknown or invalidated block token"))
	}
	if exp, ok := f.tokenExpiry[in.BlockToken]; ok && f.now().After(exp) {
		return nil, errs.E(errs.KindExpired, op, errors.New("ValidationException: block token expired"))
	}
	if idx != in.BlockIndex {
		return nil, errs.E(errs.KindInvalid, op,
			fmt.Errorf("ValidationException: token is for block %d, request says %d", idx, in.BlockIndex))
	}

	data, ok := f.blocks[in.BlockIndex]
	if !ok {
		return nil, errs.E(errs.KindNotFound, op,
			fmt.Errorf("ResourceNotFoundException: block %d has no data", in.BlockIndex))
	}

	// The checksum is computed over the true data; if this block is marked
	// corrupt, the *body* is altered afterwards, so the mismatch is exactly
	// what a real corruption looks like to the client.
	sum := sha256.Sum256(data)
	checksum := base64.StdEncoding.EncodeToString(sum[:])

	body := make([]byte, len(data))
	copy(body, data)
	if f.corruptBlocks[in.BlockIndex] && len(body) > 0 {
		body[0] ^= 0xFF
	}

	f.openBodies++
	if f.openBodies > f.maxOpenBodies {
		f.maxOpenBodies = f.openBodies
	}

	return &GetSnapshotBlockOutput{
		BlockData:         &trackedBody{Reader: bytes.NewReader(body), fake: f},
		DataLength:        int32(len(data)), //nolint:gosec // block data is at most 512 KiB
		Checksum:          checksum,
		ChecksumAlgorithm: ChecksumAlgorithmSHA256,
	}, nil
}

// trackedBody is a response body that reports when it is closed, so a leaked
// body fails a test instead of going unnoticed until production.
type trackedBody struct {
	io.Reader
	fake   *Fake
	closed bool
}

func (b *trackedBody) Close() error {
	if b.closed {
		return errors.New("body closed twice")
	}
	b.closed = true
	b.fake.mu.Lock()
	b.fake.openBodies--
	b.fake.mu.Unlock()
	return nil
}

var _ API = (*Fake)(nil)
