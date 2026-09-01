package ebs

import (
	"context"
	"errors"
	"fmt"
	"math"

	awsebs "github.com/aws/aws-sdk-go-v2/service/ebs"
	ebstypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

// AWSClient adapts the real aws-sdk-go-v2 EBS client to the API interface.
//
// # This code has never executed
//
// docs/ENGINEERING-RULES.md R7 forbids contacting a real cloud account, and the CLI offers no
// way to construct one of these. So why does it exist at all?
//
// Because it is the only available evidence that the modelling in api.go is
// right. Everything else in this package is checked against Fake, and a fake
// can only ever confirm that the client agrees with my own reading of the
// documentation. This file is checked by the Go compiler against the real
// SDK's real types at v1.36.8 — if a field were the wrong shape, the wrong
// name, or the wrong pointer-ness, the build would fail.
//
// That is a genuine but narrow guarantee, and it should be described as
// exactly that: the types line up. It says nothing about whether the service
// behaves as documented at runtime.
//
// # What the compiler pinned down
//
// Verified by compilation on 2026-08-26 against module v1.36.8:
//
//   - Every index is *int32, not int64 or int. Block indexes are therefore
//     capped at ~2.1 billion, which at 512 KiB per block is ~1 PiB per
//     snapshot. The interface in api.go uses int64 anyway, because
//     index × BlockSize overflows int32 well before that ceiling and doing
//     the arithmetic in the wider type is cheaper than auditing every call
//     site for it.
//   - GetSnapshotBlockOutput.BlockData is an io.ReadCloser, so every response
//     must be drained and closed (docs/RISKS.md R-008).
//   - Nearly every field is a pointer, so a missing value is nil rather than
//     a zero. Dereferencing without checking is the obvious way to turn a
//     partial response into a panic, which is why deref helpers are used
//     throughout rather than bare *x.
type AWSClient struct {
	client *awsebs.Client
}

// NewAWSClient wraps an SDK client.
//
// The caller constructs the *awsebs.Client, which means the caller is the one
// that has to load credentials. Nothing in this package reads a credential
// chain, an environment variable, or an instance metadata endpoint (R7).
func NewAWSClient(client *awsebs.Client) *AWSClient {
	return &AWSClient{client: client}
}

// ListSnapshotBlocks implements API.
func (c *AWSClient) ListSnapshotBlocks(ctx context.Context, in ListSnapshotBlocksInput) (*ListSnapshotBlocksOutput, error) {
	const op = "aws.ListSnapshotBlocks"

	req := &awsebs.ListSnapshotBlocksInput{
		SnapshotId: strPtr(in.SnapshotID),
		MaxResults: int32Ptr(clampMaxResults(in.MaxResults)),
	}
	if in.NextToken != "" {
		req.NextToken = strPtr(in.NextToken)
	} else if in.StartingBlockIndex > 0 {
		idx, err := toInt32(in.StartingBlockIndex)
		if err != nil {
			return nil, errs.E(errs.KindInvalid, op, err)
		}
		req.StartingBlockIndex = int32Ptr(idx)
	}

	out, err := c.client.ListSnapshotBlocks(ctx, req)
	if err != nil {
		return nil, classify(op, err)
	}

	res := &ListSnapshotBlocksOutput{
		BlockSize:  int64(derefInt32(out.BlockSize)),
		NextToken:  derefStr(out.NextToken),
		VolumeSize: derefInt64(out.VolumeSize),
	}
	if out.ExpiryTime != nil {
		res.ExpiryTime = *out.ExpiryTime
	}
	for _, b := range out.Blocks {
		res.Blocks = append(res.Blocks, Block{
			BlockIndex: int64(derefInt32(b.BlockIndex)),
			BlockToken: derefStr(b.BlockToken),
		})
	}
	return res, nil
}

// ListChangedBlocks implements API.
func (c *AWSClient) ListChangedBlocks(ctx context.Context, in ListChangedBlocksInput) (*ListChangedBlocksOutput, error) {
	const op = "aws.ListChangedBlocks"

	req := &awsebs.ListChangedBlocksInput{
		FirstSnapshotId:  strPtr(in.FirstSnapshotID),
		SecondSnapshotId: strPtr(in.SecondSnapshotID),
		MaxResults:       int32Ptr(clampMaxResults(in.MaxResults)),
	}
	if in.NextToken != "" {
		req.NextToken = strPtr(in.NextToken)
	} else if in.StartingBlockIndex > 0 {
		idx, err := toInt32(in.StartingBlockIndex)
		if err != nil {
			return nil, errs.E(errs.KindInvalid, op, err)
		}
		req.StartingBlockIndex = int32Ptr(idx)
	}

	out, err := c.client.ListChangedBlocks(ctx, req)
	if err != nil {
		return nil, classify(op, err)
	}

	res := &ListChangedBlocksOutput{
		BlockSize:  int64(derefInt32(out.BlockSize)),
		NextToken:  derefStr(out.NextToken),
		VolumeSize: derefInt64(out.VolumeSize),
	}
	if out.ExpiryTime != nil {
		res.ExpiryTime = *out.ExpiryTime
	}
	for _, cb := range out.ChangedBlocks {
		res.ChangedBlocks = append(res.ChangedBlocks, ChangedBlock{
			BlockIndex: int64(derefInt32(cb.BlockIndex)),
			// derefStr turns a nil FirstBlockToken into "", which is exactly
			// the "absent" signal the rest of the package tests for. That is
			// deliberate: the SDK's nil and the documented "absent" mean the
			// same thing, and collapsing them here keeps the semantics in one
			// place rather than spread across every caller.
			FirstBlockToken:  derefStr(cb.FirstBlockToken),
			SecondBlockToken: derefStr(cb.SecondBlockToken),
		})
	}
	return res, nil
}

// GetSnapshotBlock implements API.
func (c *AWSClient) GetSnapshotBlock(ctx context.Context, in GetSnapshotBlockInput) (*GetSnapshotBlockOutput, error) {
	const op = "aws.GetSnapshotBlock"

	idx, err := toInt32(in.BlockIndex)
	if err != nil {
		return nil, errs.E(errs.KindInvalid, op, err)
	}

	out, err := c.client.GetSnapshotBlock(ctx, &awsebs.GetSnapshotBlockInput{
		SnapshotId: strPtr(in.SnapshotID),
		BlockIndex: int32Ptr(idx),
		BlockToken: strPtr(in.BlockToken),
	})
	if err != nil {
		return nil, classify(op, err)
	}

	return &GetSnapshotBlockOutput{
		BlockData:         out.BlockData,
		DataLength:        derefInt32(out.DataLength),
		Checksum:          derefStr(out.Checksum),
		ChecksumAlgorithm: string(out.ChecksumAlgorithm),
	}, nil
}

// classify maps SDK errors into the engine's taxonomy.
//
// This is the whole reason a provider package exists (docs/ENGINEERING-RULES.md R11): the
// engine decides whether to retry based on errs.Kind, and translating a
// vendor's error vocabulary into that vocabulary is the provider's job. If
// the core had to know what a RequestThrottledException is, the abstraction
// would have leaked.
//
// The mapping follows the documented retry guidance: "You should always retry
// requests that receive server (5xx) error responses, and ThrottlingException
// and RequestThrottledException client error responses." Verified 2026-08-26.
func classify(op string, err error) error {
	if err == nil {
		return nil
	}

	var throttled *ebstypes.RequestThrottledException
	if errors.As(err, &throttled) {
		return errs.E(errs.KindThrottled, op, err)
	}

	var concurrent *ebstypes.ConcurrentLimitExceededException
	if errors.As(err, &concurrent) {
		// A concurrency ceiling, not a permanent refusal — backing off is
		// the correct response, same as throttling.
		return errs.E(errs.KindThrottled, op, err)
	}

	var internal *ebstypes.InternalServerException
	if errors.As(err, &internal) {
		return errs.E(errs.KindTransient, op, err)
	}

	var conflict *ebstypes.ConflictException
	if errors.As(err, &conflict) {
		return errs.E(errs.KindTransient, op, err)
	}

	var notFound *ebstypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return errs.E(errs.KindNotFound, op, err)
	}

	var denied *ebstypes.AccessDeniedException
	if errors.As(err, &denied) {
		return errs.E(errs.KindPermission, op, err)
	}

	var quota *ebstypes.ServiceQuotaExceededException
	if errors.As(err, &quota) {
		// Documented as HTTP 402. A quota ceiling is not something a retry
		// clears, so it is permanent rather than throttled — retrying would
		// burn the budget for no possible gain.
		return errs.E(errs.KindInvalid, op, err)
	}

	var validation *ebstypes.ValidationException
	if errors.As(err, &validation) {
		// Covers expired block and pagination tokens, which the service
		// reports as validation failures rather than a distinct type. They
		// are not retryable with the same token, which is why the engine
		// treats KindInvalid as permanent.
		return errs.E(errs.KindInvalid, op, err)
	}

	// Unrecognised errors are permanent by default (see errs.IsRetryable).
	// An unknown failure that gets retried is how a bug becomes a loop.
	return errs.E(errs.KindUnknown, op, err)
}

// Pointer helpers. The SDK models almost every field as a pointer, so a
// missing value is nil rather than a zero. These centralise the nil checks;
// dereferencing inline is how a partial response becomes a panic.

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func int32Ptr(v int32) *int32 { return &v }

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// toInt32 narrows a block index for the SDK, refusing values it cannot hold.
//
// The interface in api.go uses int64 because index × BlockSize needs the
// width. The SDK uses int32. Converting silently would wrap a large index
// into a small or negative one and read the wrong block — a data-corruption
// bug disguised as a type conversion.
func toInt32(v int64) (int32, error) {
	if v < 0 || v > math.MaxInt32 {
		return 0, fmt.Errorf("block index %d does not fit in the int32 the EBS API uses", v)
	}
	return int32(v), nil
}

var _ API = (*AWSClient)(nil)
