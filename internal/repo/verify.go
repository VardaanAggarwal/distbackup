package repo

import (
	"context"
	"fmt"
	"sort"

	"github.com/vardaanaggarwal/distbackup/internal/blob"
	"github.com/vardaanaggarwal/distbackup/internal/errs"
	"github.com/vardaanaggarwal/distbackup/internal/store"
)

// VerifyOptions controls how thorough a verification is.
type VerifyOptions struct {
	// Full reads and re-hashes every referenced blob.
	//
	// Without it, verify checks structure: that every snapshot parses, that
	// every blob it references resolves to a pack, and that every referenced
	// pack exists. That is enough to prove no snapshot has a dangling
	// reference — which is the property the crash test asserts — and costs a
	// listing plus a Stat per pack.
	//
	// With it, verify additionally proves the stored bytes are intact, at the
	// cost of reading the entire repository. The two modes exist because the
	// cheap one is the one you can afford to run after every backup.
	Full bool
}

// VerifyReport is the result of a verification pass.
type VerifyReport struct {
	// Snapshots is the number of manifests checked.
	Snapshots int
	// Packs is the number of pack objects found in the store.
	Packs int
	// BlobsReferenced is the number of distinct blobs snapshots refer to.
	BlobsReferenced int
	// BlobsRead is the number of blobs actually read and re-hashed. Zero
	// unless Full was set.
	BlobsRead int
	// MissingPacks are packs a snapshot needs that are not in the store.
	// A non-empty list means data loss.
	MissingPacks []blob.ID
	// DanglingBlobs are blobs a snapshot references that the index cannot
	// resolve. A non-empty list means data loss.
	DanglingBlobs []blob.ID
	// OrphanPacks are packs that exist but no snapshot references. These are
	// harmless — usually the residue of an interrupted backup — and are what
	// CollectGarbage reclaims.
	OrphanPacks []blob.ID
	// Problems are the errors encountered, in the order found.
	Problems []error
}

// OK reports whether the repository is intact.
//
// Orphan packs deliberately do not count against it. They waste space but
// reference nothing and lose nothing; treating them as failures would mean
// every interrupted backup left the repository permanently "broken".
func (r *VerifyReport) OK() bool {
	return len(r.Problems) == 0 && len(r.MissingPacks) == 0 && len(r.DanglingBlobs) == 0
}

// Summary renders the report as a single human-readable line.
func (r *VerifyReport) Summary() string {
	status := "ok"
	if !r.OK() {
		status = "FAILED"
	}
	return fmt.Sprintf(
		"%s: %d snapshots, %d packs, %d blobs referenced, %d read, %d missing packs, %d dangling blobs, %d orphan packs",
		status, r.Snapshots, r.Packs, r.BlobsReferenced, r.BlobsRead,
		len(r.MissingPacks), len(r.DanglingBlobs), len(r.OrphanPacks))
}

// Verify checks the repository's integrity.
//
// The check that matters most, and the one the mandatory crash test asserts,
// is that no snapshot references a blob or pack that does not exist. The
// write ordering in this package is designed so that state is unreachable;
// this is what proves it.
func (r *Repository) Verify(ctx context.Context, opts VerifyOptions) (*VerifyReport, error) {
	const op = "repo.Verify"

	report := &VerifyReport{}

	// Every pack actually present in the store.
	presentPacks := make(map[blob.ID]int64)
	err := r.store.List(ctx, PackPrefix, func(info store.ObjectInfo) error {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}
		id, err := packIDFromKey(info.Key)
		if err != nil {
			report.Problems = append(report.Problems,
				fmt.Errorf("object %q under %s is not a valid pack name", info.Key, PackPrefix))
			// Recorded as a problem, not returned: verify's job is to report
			// everything wrong in one pass, not to stop at the first thing.
			return nil //nolint:nilerr // the error is captured in the report
		}
		presentPacks[id] = info.Size
		return nil
	})
	if err != nil {
		return nil, err
	}
	report.Packs = len(presentPacks)

	snaps, err := r.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	report.Snapshots = len(snaps)

	referencedBlobs := make(map[blob.ID]struct{})
	referencedPacks := make(map[blob.ID]struct{})
	missingPacks := make(map[blob.ID]struct{})

	for _, snap := range snaps {
		for _, id := range snap.BlobIDs() {
			if err := ctx.Err(); err != nil {
				return nil, errs.E(errs.KindCanceled, op, err)
			}
			if _, seen := referencedBlobs[id]; seen {
				continue
			}
			referencedBlobs[id] = struct{}{}

			loc, ok := r.idx.Lookup(id)
			if !ok {
				report.DanglingBlobs = append(report.DanglingBlobs, id)
				report.Problems = append(report.Problems,
					fmt.Errorf("snapshot %s references blob %s, which the index cannot resolve",
						snap.ID[:12], id.Short()))
				continue
			}

			referencedPacks[loc.PackID] = struct{}{}
			if _, present := presentPacks[loc.PackID]; !present {
				if _, already := missingPacks[loc.PackID]; !already {
					missingPacks[loc.PackID] = struct{}{}
					report.MissingPacks = append(report.MissingPacks, loc.PackID)
					report.Problems = append(report.Problems,
						fmt.Errorf("snapshot %s needs pack %s, which is not in the store",
							snap.ID[:12], loc.PackID.Short()))
				}
				continue
			}

			if opts.Full {
				if _, err := r.GetBlob(ctx, id); err != nil {
					report.Problems = append(report.Problems,
						fmt.Errorf("blob %s (snapshot %s): %w", id.Short(), snap.ID[:12], err))
					continue
				}
				report.BlobsRead++
			}
		}
	}
	report.BlobsReferenced = len(referencedBlobs)

	for id := range presentPacks {
		if _, used := referencedPacks[id]; !used {
			report.OrphanPacks = append(report.OrphanPacks, id)
		}
	}

	// Sorted so a report is reproducible run to run; map iteration is not.
	sortIDs(report.MissingPacks)
	sortIDs(report.DanglingBlobs)
	sortIDs(report.OrphanPacks)

	return report, nil
}

func sortIDs(ids []blob.ID) {
	sort.Slice(ids, func(i, j int) bool {
		return string(ids[i][:]) < string(ids[j][:])
	})
}

// GCReport describes what garbage collection reclaimed.
type GCReport struct {
	// PacksDeleted is the number of orphan packs removed.
	PacksDeleted int
	// BytesReclaimed is the total size of those packs.
	BytesReclaimed int64
	// DryRun reports whether anything was actually deleted.
	DryRun bool
}

// CollectGarbage deletes packs that no snapshot references.
//
// Orphan packs are the residue of interrupted backups: the write ordering
// stores data before the manifest that references it, so a run that dies in
// between leaves packs behind. That is the safe failure mode by design, and
// this is the routine that cleans up after it.
//
// Deletion order matters. Packs are removed first and the index is rewritten
// afterwards, which means a crash midway leaves the index listing blobs whose
// packs are gone — recoverable, because the next Open rebuilds the index from
// the packs that remain. The reverse order would delete the index's knowledge
// of packs that still exist, stranding live data.
func (r *Repository) CollectGarbage(ctx context.Context, dryRun bool) (*GCReport, error) {
	const op = "repo.CollectGarbage"

	report, err := r.Verify(ctx, VerifyOptions{})
	if err != nil {
		return nil, err
	}
	// Refuse to delete anything from a repository that is already damaged:
	// the orphan set is computed from the same index whose integrity is in
	// question, so acting on it could destroy the packs a repair would need.
	if !report.OK() {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("refusing to collect garbage: %s", report.Summary()))
	}

	gc := &GCReport{DryRun: dryRun}
	for _, id := range report.OrphanPacks {
		if err := ctx.Err(); err != nil {
			return nil, errs.E(errs.KindCanceled, op, err)
		}
		size, err := r.packSize(ctx, id)
		if err != nil && !errs.IsNotFound(err) {
			return nil, err
		}
		if dryRun {
			gc.PacksDeleted++
			gc.BytesReclaimed += size
			continue
		}
		if err := r.store.Delete(ctx, PackKey(id)); err != nil {
			return nil, err
		}
		gc.PacksDeleted++
		gc.BytesReclaimed += size

		r.mu.Lock()
		delete(r.packSizes, id)
		r.mu.Unlock()
	}

	if !dryRun && gc.PacksDeleted > 0 {
		if err := r.RebuildIndex(ctx); err != nil {
			return nil, err
		}
		if err := r.SaveIndex(ctx); err != nil {
			return nil, err
		}
	}
	return gc, nil
}
