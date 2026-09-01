// Package ebs implements source.BlockSource over the AWS EBS direct APIs.
//
// # Nothing here has ever run against AWS
//
// docs/ENGINEERING-RULES.md R7 forbids it absolutely. Every line in this package was written
// against the published API reference (checked 2026-08-26, citations inline)
// and is exercised only against Fake, which reproduces the documented
// behaviour including its failure modes.
//
// That makes the fake load-bearing rather than a test convenience: it is the
// only thing standing between this client and being silently wrong. It is
// therefore built to reproduce the awkward parts of the contract, not the
// happy path — expired tokens, empty pages that still carry a continuation
// token, throttling, checksum mismatches, and response bodies that must be
// drained and closed.
//
// # Layering
//
// The engine never sees any of this. `Source` satisfies source.BlockSource,
// which the core owns (docs/ENGINEERING-RULES.md R11); the AWS SDK appears only in
// awsclient.go, and internal/arch enforces that.
package ebs

import (
	"context"
	"io"
	"time"
)

// BlockSize is the EBS direct API's fixed block size.
//
// Verified 2026-08-26 (GetSnapshotBlock reference): "A block index is a
// logical index in units of 512 KiB blocks. To identify the block index,
// divide the logical offset of the data in the logical volume by the block
// size (logical offset of data/524288)."
//
// It is a constant, not a parameter: the API offers no other size, and
// pricing is quoted per 512 KiB SnapshotAPIUnit.
const BlockSize = 512 * 1024

// MaxResults bounds.
//
// Verified 2026-08-26 (ListSnapshotBlocks reference): "Valid Range: Minimum
// value of 100. Maximum value of 10000." The FAQ adds that a request below
// the minimum is not an error — "the API will return at least 100 results" —
// so the floor is a server-side clamp rather than a validation failure.
const (
	MinMaxResults     = 100
	MaxMaxResults     = 10000
	DefaultMaxResults = 10000
)

// Token lifetimes, verified 2026-08-26 (EBS direct APIs FAQ): "Block tokens
// are valid for seven days, and next tokens are valid for 60 minutes."
//
// The 60-minute pagination window is the binding constraint on a long
// backup, not the 7-day block token — which is the opposite of what most
// people assume. See docs/RISKS.md R-006.
const (
	BlockTokenLifetime = 7 * 24 * time.Hour
	NextTokenLifetime  = 60 * time.Minute
)

// Block is one block in a snapshot listing.
type Block struct {
	// BlockIndex is the block's index. Verified: indexes are unique and
	// returned in numerical order.
	BlockIndex int64
	// BlockToken is the opaque handle GetSnapshotBlock requires.
	BlockToken string
}

// ChangedBlock is one block that differs between two snapshots.
type ChangedBlock struct {
	// BlockIndex is the block's index.
	BlockIndex int64

	// FirstBlockToken locates the block in the older snapshot.
	//
	// Verified 2026-08-26 (ChangedBlock reference): "This value is absent if
	// the first snapshot does not have the changed block that is on the
	// second snapshot." Absent therefore means *newly written*, not
	// unchanged. Reading it the other way round would silently skip genuinely
	// new data — a data-loss bug that no amount of testing against a
	// happy-path fake would find.
	FirstBlockToken string

	// SecondBlockToken locates the block in the newer snapshot.
	SecondBlockToken string
}

// ListSnapshotBlocksInput mirrors the documented request.
type ListSnapshotBlocksInput struct {
	SnapshotID         string
	MaxResults         int32
	NextToken          string
	StartingBlockIndex int64
}

// ListSnapshotBlocksOutput mirrors the documented response.
type ListSnapshotBlocksOutput struct {
	Blocks []Block
	// BlockSize is the block size in bytes, as reported by the service.
	BlockSize int64
	// ExpiryTime is when the returned BlockTokens stop working.
	ExpiryTime time.Time
	// NextToken is empty when there are no more pages.
	NextToken string
	// VolumeSize is the volume size in GB (not bytes — verified: "The size of
	// the volume in GB").
	VolumeSize int64
}

// ListChangedBlocksInput mirrors the documented request.
type ListChangedBlocksInput struct {
	FirstSnapshotID    string
	SecondSnapshotID   string
	MaxResults         int32
	NextToken          string
	StartingBlockIndex int64
}

// ListChangedBlocksOutput mirrors the documented response.
type ListChangedBlocksOutput struct {
	ChangedBlocks []ChangedBlock
	BlockSize     int64
	ExpiryTime    time.Time
	NextToken     string
	VolumeSize    int64
}

// GetSnapshotBlockInput mirrors the documented request.
type GetSnapshotBlockInput struct {
	SnapshotID string
	BlockIndex int64
	BlockToken string
}

// GetSnapshotBlockOutput mirrors the documented response.
type GetSnapshotBlockOutput struct {
	// BlockData is the block's contents.
	//
	// An io.ReadCloser, matching aws-sdk-go-v2's
	// ebs.GetSnapshotBlockOutput.BlockData (verified 2026-08-26 against
	// module v1.36.8). Every caller must drain *and* close it, including on
	// the retry path — abandoning a response body without closing it leaks a
	// connection, and on a long backup that exhausts the pool with symptoms
	// that appear far from the cause. See docs/RISKS.md R-008.
	BlockData io.ReadCloser
	// DataLength is the number of bytes in the block.
	DataLength int32
	// Checksum is the Base64-encoded digest the service computed.
	Checksum string
	// ChecksumAlgorithm is "SHA256"; it is the only documented value.
	ChecksumAlgorithm string
}

// API is the subset of the EBS direct APIs this package uses.
//
// Declared here rather than depending on the SDK's client type so that Source
// can be tested against Fake. The three write operations (StartSnapshot,
// PutSnapshotBlock, CompleteSnapshot) are deliberately absent: distbackup
// reads snapshots into its own repository and never writes an EBS snapshot
// back, so including them would be scope this project does not have
// (docs/ENGINEERING-RULES.md R5).
type API interface {
	ListSnapshotBlocks(ctx context.Context, in ListSnapshotBlocksInput) (*ListSnapshotBlocksOutput, error)
	ListChangedBlocks(ctx context.Context, in ListChangedBlocksInput) (*ListChangedBlocksOutput, error)
	GetSnapshotBlock(ctx context.Context, in GetSnapshotBlockInput) (*GetSnapshotBlockOutput, error)
}

// ChecksumAlgorithmSHA256 is the only algorithm the service documents.
const ChecksumAlgorithmSHA256 = "SHA256"
