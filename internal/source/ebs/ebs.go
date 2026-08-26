package ebs

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
	"github.com/vardaanaggarwal/distbackup/internal/retry"
	"github.com/vardaanaggarwal/distbackup/internal/source"
)

// Source reads an EBS snapshot as a block device.
type Source struct {
	api        API
	snapshotID string
	policy     retry.Policy

	// verifyChecksums re-computes the SHA-256 of every block and compares it
	// against the checksum the service reported.
	//
	// On by default. The service already validates in transit, but this
	// catches the case that matters to a backup: a block that arrives intact
	// and is then mangled locally before being stored. It costs one SHA-256
	// per block, which the pipeline pays anyway to compute the blob ID.
	verifyChecksums bool
}

// Option configures a Source.
type Option func(*Source)

// WithRetryPolicy overrides the retry schedule.
func WithRetryPolicy(p retry.Policy) Option {
	return func(s *Source) { s.policy = p }
}

// WithoutChecksumVerification disables per-block checksum verification.
func WithoutChecksumVerification() Option {
	return func(s *Source) { s.verifyChecksums = false }
}

// New returns a Source reading the given snapshot through api.
//
// api is injected rather than constructed here so the whole implementation
// can be exercised against Fake. Under CLAUDE.md R7 that is not a convenience
// — it is the only way this code is ever executed at all.
func New(api API, snapshotID string, opts ...Option) (*Source, error) {
	const op = "ebs.New"

	if api == nil {
		return nil, errs.E(errs.KindInvalid, op, errors.New("nil API"))
	}
	if snapshotID == "" {
		return nil, errs.E(errs.KindInvalid, op, errors.New("empty snapshot ID"))
	}

	s := &Source{
		api:             api,
		snapshotID:      snapshotID,
		policy:          retry.DefaultPolicy(),
		verifyChecksums: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// BlockSize returns the fixed EBS block size.
func (s *Source) BlockSize() int64 { return BlockSize }

// ID returns the snapshot identifier.
func (s *Source) ID() string { return s.snapshotID }

// Close releases resources. The client holds none of its own.
func (s *Source) Close() error { return nil }

// Size returns the volume's logical size in bytes.
//
// It costs one listing request, because VolumeSize is only reported as part
// of a list response. The documented unit is GB, so it is converted here —
// returning the raw number would hand the caller a value 10^9 times too small
// with no indication anything was wrong.
func (s *Source) Size(ctx context.Context) (int64, error) {
	out, err := s.listPage(ctx, ListSnapshotBlocksInput{
		SnapshotID: s.snapshotID,
		MaxResults: MinMaxResults,
	})
	if err != nil {
		return 0, err
	}
	// Verified 2026-08-26: "VolumeSize — The size of the volume in GB."
	// AWS bills and reports EBS volume sizes in GiB despite the field name,
	// so this uses 1024-based units.
	return out.VolumeSize * 1024 * 1024 * 1024, nil
}

// ListBlocks calls fn for every block that has data.
//
// # The pagination contract, which is where the bugs live
//
// Two documented behaviours make the obvious loop wrong:
//
//  1. A page may be empty and still have a continuation token. Verified
//     2026-08-26 (FAQ): "If the changed blocks are scarce in the snapshot,
//     the response may be empty but the API will return a next page token
//     value." Terminating on an empty page would silently back up nothing
//     and report success — the worst possible failure for a backup tool.
//     This loop terminates only on an empty NextToken.
//
//  2. The continuation token expires after 60 minutes, while the block
//     tokens it yields last 7 days. So the listing must be *completed*
//     promptly even though the results stay usable for a week. That is why
//     this materialises the whole listing rather than interleaving reads
//     with pagination over hours (docs/RISKS.md R-006).
//
// Only blocks that have been written are returned, so a sparse volume yields
// far fewer than Size/BlockSize entries. Nothing may assume otherwise.
func (s *Source) ListBlocks(ctx context.Context, fn func(source.BlockRef) error) error {
	const op = "ebs.ListBlocks"

	var (
		nextToken   string
		pages       int
		lastIndex   = int64(-1)
		listedAt    = time.Now()
		blocksSoFar int
	)

	for {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}

		// The pagination token's own 60-minute lifetime. Checked locally so
		// the failure is a clear diagnostic rather than an opaque server
		// error, and so it is reported even if the service is lenient.
		if nextToken != "" && time.Since(listedAt) > NextTokenLifetime {
			return errs.E(errs.KindExpired, op,
				fmt.Errorf("pagination token expired after %v (limit %v); "+
					"listed %d blocks over %d pages",
					time.Since(listedAt).Round(time.Second), NextTokenLifetime, blocksSoFar, pages))
		}

		out, err := s.listPage(ctx, ListSnapshotBlocksInput{
			SnapshotID: s.snapshotID,
			MaxResults: DefaultMaxResults,
			NextToken:  nextToken,
		})
		if err != nil {
			return err
		}
		pages++

		for _, b := range out.Blocks {
			// Cancellation is checked per block, not just per page. A page
			// can carry up to MaxResults (10,000) entries, so checking only
			// between pages would keep invoking the caller thousands of times
			// after they had already given up.
			if err := ctx.Err(); err != nil {
				return errs.E(errs.KindCanceled, op, err)
			}
			// Verified: "The block indexes returned are unique, and in
			// numerical order." Checked rather than trusted, because the
			// restore path depends on it and a silent violation would
			// produce a corrupt image.
			if b.BlockIndex <= lastIndex {
				return errs.E(errs.KindCorrupt, op,
					fmt.Errorf("block index %d follows %d; the service must return them in increasing order",
						b.BlockIndex, lastIndex))
			}
			lastIndex = b.BlockIndex
			blocksSoFar++

			if err := fn(source.BlockRef{
				Index:  b.BlockIndex,
				Token:  b.BlockToken,
				Expiry: out.ExpiryTime,
			}); err != nil {
				return err
			}
		}

		// Terminate ONLY on an empty NextToken. See the doc comment.
		if out.NextToken == "" {
			return nil
		}
		nextToken = out.NextToken
	}
}

// ListChangedBlocks calls fn for every block that differs from `since`.
//
// This is what makes an incremental backup of a large volume cheap: the
// service reports which blocks changed, so unchanged data is never fetched
// and never paid for.
func (s *Source) ListChangedBlocks(ctx context.Context, since string, fn func(source.ChangedBlockRef) error) error {
	const op = "ebs.ListChangedBlocks"

	if since == "" {
		return errs.E(errs.KindInvalid, op, errors.New("no baseline snapshot given"))
	}

	var (
		nextToken string
		lastIndex = int64(-1)
		listedAt  = time.Now()
	)

	for {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}
		if nextToken != "" && time.Since(listedAt) > NextTokenLifetime {
			return errs.E(errs.KindExpired, op,
				fmt.Errorf("pagination token expired after %v (limit %v)",
					time.Since(listedAt).Round(time.Second), NextTokenLifetime))
		}

		out, err := retry.DoValue(ctx, s.policy, op,
			func(ctx context.Context, _ int) (*ListChangedBlocksOutput, error) {
				return s.api.ListChangedBlocks(ctx, ListChangedBlocksInput{
					FirstSnapshotID:  since,
					SecondSnapshotID: s.snapshotID,
					MaxResults:       DefaultMaxResults,
					NextToken:        nextToken,
				})
			})
		if err != nil {
			return err
		}

		for _, cb := range out.ChangedBlocks {
			if err := ctx.Err(); err != nil {
				return errs.E(errs.KindCanceled, op, err)
			}
			if cb.BlockIndex <= lastIndex {
				return errs.E(errs.KindCorrupt, op,
					fmt.Errorf("changed block index %d follows %d", cb.BlockIndex, lastIndex))
			}
			lastIndex = cb.BlockIndex

			// An absent SecondBlockToken would mean the block was deleted
			// from the newer snapshot, which the API does not document as
			// possible for a changed block. Refuse rather than fetch with an
			// empty token and get an opaque error later.
			if cb.SecondBlockToken == "" {
				return errs.E(errs.KindCorrupt, op,
					fmt.Errorf("changed block %d has no token for the newer snapshot", cb.BlockIndex))
			}

			if err := fn(source.ChangedBlockRef{
				Ref: source.BlockRef{
					Index:  cb.BlockIndex,
					Token:  cb.SecondBlockToken,
					Expiry: out.ExpiryTime,
				},
				// Absent first token == the block is new in this snapshot.
				// See the ChangedBlock doc comment in api.go.
				IsNew: cb.FirstBlockToken == "",
			}); err != nil {
				return err
			}
		}

		if out.NextToken == "" {
			return nil
		}
		nextToken = out.NextToken
	}
}

// ReadBlock fetches one block into buf.
func (s *Source) ReadBlock(ctx context.Context, ref source.BlockRef, buf []byte) (int, error) {
	const op = "ebs.ReadBlock"

	if int64(len(buf)) < BlockSize {
		return 0, errs.E(errs.KindInvalid, op,
			fmt.Errorf("buffer is %d bytes, need at least %d", len(buf), BlockSize))
	}
	if ref.Token == "" {
		return 0, errs.E(errs.KindInvalid, op,
			fmt.Errorf("block %d has no token", ref.Index))
	}
	// Checked before the request so an expired token produces a clear
	// diagnostic instead of a generic validation error from the service.
	if ref.Expired(time.Now()) {
		return 0, errs.E(errs.KindExpired, op,
			fmt.Errorf("block token for index %d expired at %s", ref.Index, ref.Expiry.Format(time.RFC3339)))
	}

	return retry.DoValue(ctx, s.policy, op, func(ctx context.Context, _ int) (int, error) {
		return s.readOnce(ctx, ref, buf)
	})
}

// readOnce performs a single GetSnapshotBlock and copies the body into buf.
//
// The response body is drained and closed on every path, including the error
// paths. That is the whole reason this is a separate function: with the
// cleanup in one place, a future edit cannot add an early return that skips
// it (docs/RISKS.md R-008).
func (s *Source) readOnce(ctx context.Context, ref source.BlockRef, buf []byte) (int, error) {
	const op = "ebs.ReadBlock"

	out, err := s.api.GetSnapshotBlock(ctx, GetSnapshotBlockInput{
		SnapshotID: s.snapshotID,
		BlockIndex: ref.Index,
		BlockToken: ref.Token,
	})
	if err != nil {
		return 0, err
	}
	if out.BlockData == nil {
		return 0, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("block %d returned no body", ref.Index))
	}
	defer func() {
		// Drain before closing: an unread body can prevent the underlying
		// connection from being reused, which turns a keep-alive pool into a
		// connection-per-request storm on a long backup.
		_, _ = io.Copy(io.Discard, out.BlockData)
		_ = out.BlockData.Close()
	}()

	if out.DataLength < 0 || int64(out.DataLength) > BlockSize {
		return 0, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("block %d reports length %d, outside [0,%d]", ref.Index, out.DataLength, BlockSize))
	}

	n, err := io.ReadFull(out.BlockData, buf[:out.DataLength])
	if err != nil {
		// A short body is a transport problem, so it is retryable — unlike a
		// checksum mismatch, which means the bytes themselves are wrong.
		return 0, errs.E(errs.KindTransient, op,
			fmt.Errorf("block %d: read %d of %d bytes: %w", ref.Index, n, out.DataLength, err))
	}

	if s.verifyChecksums && out.Checksum != "" {
		if err := verifyChecksum(buf[:n], out.Checksum, out.ChecksumAlgorithm); err != nil {
			return 0, errs.E(errs.KindCorrupt, op, fmt.Errorf("block %d: %w", ref.Index, err))
		}
	}
	return n, nil
}

// verifyChecksum checks a block against the service-supplied digest.
//
// Verified 2026-08-26: "the service provides Base64-encoded SHA256 checksums
// for each block of data transmitted, which you can use for validation."
func verifyChecksum(data []byte, want, algorithm string) error {
	if algorithm != "" && algorithm != ChecksumAlgorithmSHA256 {
		// Refuse rather than skip. Silently ignoring an algorithm we do not
		// understand would turn integrity checking off without saying so.
		return fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
	sum := sha256.Sum256(data)
	got := base64.StdEncoding.EncodeToString(sum[:])
	if got != want {
		return fmt.Errorf("checksum mismatch: service reported %s, computed %s", want, got)
	}
	return nil
}

// listPage performs one ListSnapshotBlocks call under the retry policy.
func (s *Source) listPage(ctx context.Context, in ListSnapshotBlocksInput) (*ListSnapshotBlocksOutput, error) {
	return retry.DoValue(ctx, s.policy, "ebs.ListSnapshotBlocks",
		func(ctx context.Context, _ int) (*ListSnapshotBlocksOutput, error) {
			return s.api.ListSnapshotBlocks(ctx, in)
		})
}

var _ source.BlockSource = (*Source)(nil)
