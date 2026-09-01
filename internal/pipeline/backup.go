// Package pipeline orchestrates backup and restore.
//
// # The concurrency model
//
// A file backup runs four stages connected by channels:
//
//	walk (1) ──► chunk+hash (N) ──► pack assembly (1) ──► upload (M)
//
// Stage counts and the reasoning for each are in docs/PLAN.md §5. The rules
// that hold everywhere in this package (docs/ENGINEERING-RULES.md):
//
//   - Fixed worker pools. Never one goroutine per chunk — a large backup
//     would create millions of them.
//   - The sender closes a channel, never the receiver.
//   - Every channel operation goes through send/recv, which select on
//     ctx.Done(). A bare send blocks forever once a downstream stage has
//     failed, turning a clean error into a deadlock.
//   - No buffer is reused across a stage boundary (docs/DECISIONS.md D-008).
//
// # Why pack assembly is single-threaded
//
// It is the one stage that cannot be parallel, and that is a feature. The
// assembler is where the deduplication decision is made, and making it in one
// goroutine is what makes it correct: two chunk workers can produce identical
// content at the same time, both find it absent from the index, and both
// believe they should store it. A single assembler holding an in-flight set
// sees the second one as a duplicate. Assembly is also cheap — appending
// bytes to a buffer — so serialising it costs almost nothing while the
// expensive stages (hashing, uploading) stay parallel.
//
// Written from scratch (docs/ENGINEERING-RULES.md R3).
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/chunker"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/pack"
	"github.com/VardaanAggarwal/distbackup/internal/repo"
	"github.com/VardaanAggarwal/distbackup/internal/source"
)

// Options configures a backup run.
type Options struct {
	// ChunkWorkers is the number of goroutines chunking and hashing.
	// Zero means runtime.NumCPU().
	ChunkWorkers int

	// UploadWorkers is the number of goroutines writing packs.
	// Zero means min(16, 2*NumCPU).
	UploadWorkers int

	// PackTargetSize is the size at which a pack is closed off.
	// Zero means the repository's configured value.
	PackTargetSize int64

	// DryRun enumerates and counts work without writing anything.
	//
	// Part of the cost-guardrail set required by docs/ENGINEERING-RULES.md R7. It still reads
	// and chunks the source, so the reported figures are measured rather than
	// estimated — the only thing it skips is the writes.
	DryRun bool

	// MaxBytes aborts the run once this many source bytes have been read.
	// Zero means no limit. The other half of the R7 guardrails.
	MaxBytes int64

	// Hostname is recorded in the snapshot manifest.
	Hostname string

	// Progress, if set, is called periodically from a single goroutine.
	Progress func(Progress)
}

// Progress reports how far a run has got.
type Progress struct {
	// FilesDone is the number of files fully processed.
	FilesDone int
	// BytesRead is the number of source bytes read so far.
	BytesRead int64
	// BlobsNew is the number of blobs stored so far.
	BlobsNew int
	// BlobsDeduped is the number of blobs skipped as already present.
	BlobsDeduped int
	// PacksWritten is the number of packs written so far.
	PacksWritten int
}

func (o *Options) chunkWorkers() int {
	if o.ChunkWorkers > 0 {
		return o.ChunkWorkers
	}
	// Chunking is CPU-bound: SHA-256 dominates, and BenchmarkChunker shows
	// the boundary scan itself runs at ~2 GB/s. One worker per core.
	return runtime.NumCPU()
}

func (o *Options) uploadWorkers() int {
	if o.UploadWorkers > 0 {
		return o.UploadWorkers
	}
	// Upload is I/O-bound, so more workers than cores helps, but each one can
	// hold a full pack in memory, so the count is capped to bound peak
	// memory at roughly workers × PackTargetSize.
	n := 2 * runtime.NumCPU()
	if n > 16 {
		n = 16
	}
	return n
}

// Channel capacities.
//
// Buffered, but not deeply. Buffering absorbs jitter — a worker that stalls
// briefly does not stall the stage in front of it — while a small bound is
// what makes backpressure real: if the uploader cannot keep up, the queue
// fills, chunk workers block, and the walk slows down. An unbounded channel
// would instead read the entire source into memory before noticing.
//
// blobQueue dominates peak memory: at most 128 chunks in flight at up to
// 256 KiB each is ~32 MiB.
const (
	fileQueue = 64
	blobQueue = 128
	packQueue = 8
)

type blobItem struct {
	id   blob.ID
	data []byte
}

type packItem struct {
	id   blob.ID
	data []byte
	hdr  *pack.Header
}

// ErrMaxBytesExceeded is returned when Options.MaxBytes is hit.
var ErrMaxBytesExceeded = errors.New("max bytes exceeded")

// BackupFiles backs up a file tree and returns the snapshot it wrote.
//
// On DryRun the returned snapshot is complete and accurate but was never
// stored, so the caller can report exactly what a real run would do.
func BackupFiles(ctx context.Context, r *repo.Repository, src source.FileSource, opts Options) (*repo.Snapshot, error) {
	const op = "pipeline.BackupFiles"

	start := time.Now()

	// The walk is done up front rather than streamed into the pipeline. It is
	// cheap — one stat per file, no data read — and having the full list
	// first means the manifest's file order is fixed before any concurrency
	// starts, so two runs over an unchanged tree produce identical manifests
	// and therefore the same snapshot ID.
	var files []source.FileEntry
	if err := src.Walk(ctx, func(e source.FileEntry) error {
		files = append(files, e)
		return nil
	}); err != nil {
		return nil, err
	}

	packTarget := opts.PackTargetSize
	if packTarget <= 0 {
		packTarget = r.Config().PackTargetSize
	}

	st := newBackupState(len(files))
	g, gctx := newGroup(ctx)

	fileCh := make(chan int, fileQueue)
	blobCh := make(chan blobItem, blobQueue)
	packCh := make(chan packItem, packQueue)

	// Stage 1 — feed file indices. Closes fileCh, because it is the sender.
	g.Go(func() error {
		defer close(fileCh)
		for i := range files {
			if !send(gctx, fileCh, i) {
				return nil // context cancelled; the cause is reported elsewhere
			}
		}
		return nil
	})

	// Stage 2 — chunk and hash. N workers, each taking whole files, so the
	// chunk order within a file needs no reassembly downstream.
	var chunkWG sync.WaitGroup
	chunkCfg := r.Config().ChunkerConfig()
	for range opts.chunkWorkers() {
		chunkWG.Add(1)
		g.Go(func() error {
			defer chunkWG.Done()
			return chunkWorker(gctx, src, files, chunkCfg, fileCh, blobCh, st, opts)
		})
	}
	// The last chunk worker to finish closes blobCh. A separate goroutine
	// does it because "the sender closes" means the *set* of senders, and
	// there is no single one of them that knows it is last.
	go func() {
		chunkWG.Wait()
		close(blobCh)
	}()

	// Stage 3 — pack assembly. Exactly one, which is what makes the dedup
	// decision atomic. Closes packCh.
	g.Go(func() error {
		defer close(packCh)
		return assembler(gctx, r, blobCh, packCh, packTarget, st)
	})

	// Stage 4 — upload. M workers.
	for range opts.uploadWorkers() {
		g.Go(func() error {
			return uploadWorker(gctx, r, packCh, st, opts)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}

	snap := st.buildSnapshot(files, src.Root(), opts.Hostname, time.Since(start))

	if opts.DryRun {
		return snap, nil
	}

	// Write ordering (see the repo package): packs are already durable, the
	// index goes next, and the manifest is last. A crash before the manifest
	// leaves orphan packs, which is harmless; the reverse order could leave a
	// snapshot referencing data that is not there.
	if err := r.SaveIndex(ctx); err != nil {
		return nil, err
	}
	if err := r.SaveSnapshot(ctx, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// backupState holds everything the stages accumulate.
type backupState struct {
	mu sync.Mutex

	// chunks[i] is the ordered list of blob IDs for files[i]. Indexed rather
	// than appended, so workers can finish in any order without disturbing
	// the manifest's file order.
	chunks [][]blob.ID

	filesDone    int
	bytesRead    int64
	blobsNew     int
	blobsDeduped int
	bytesNew     int64
	bytesDeduped int64
	packsWritten int
}

func newBackupState(nFiles int) *backupState {
	return &backupState{chunks: make([][]blob.ID, nFiles)}
}

func (s *backupState) buildSnapshot(files []source.FileEntry, root, hostname string, dur time.Duration) *repo.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]repo.FileEntry, 0, len(files))
	var totalBytes int64
	for i, f := range files {
		entries = append(entries, repo.FileEntry{
			Path:    f.Path,
			Size:    f.Size,
			Mode:    f.Mode,
			ModTime: f.ModTime.UTC(),
			Chunks:  s.chunks[i],
		})
		totalBytes += f.Size
	}

	return &repo.Snapshot{
		Kind:      repo.KindFiles,
		CreatedAt: time.Now().UTC(),
		Source:    root,
		Hostname:  hostname,
		Files:     entries,
		Stats: repo.Stats{
			FilesTotal:   len(files),
			BytesTotal:   totalBytes,
			BlobsNew:     s.blobsNew,
			BlobsDeduped: s.blobsDeduped,
			BytesNew:     s.bytesNew,
			BytesDeduped: s.bytesDeduped,
			PacksWritten: s.packsWritten,
			Duration:     dur,
		},
	}
}

func (s *backupState) progress() Progress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Progress{
		FilesDone:    s.filesDone,
		BytesRead:    s.bytesRead,
		BlobsNew:     s.blobsNew,
		BlobsDeduped: s.blobsDeduped,
		PacksWritten: s.packsWritten,
	}
}

// chunkWorker reads whole files, splits them, and forwards every chunk.
//
// It sends every chunk downstream rather than pre-filtering against the
// index. Pre-filtering here would look like an optimisation and would make
// the deduplication counters wrong, because a chunk skipped in the worker is
// invisible to the assembler that owns the statistics. The channel carries
// slice headers, not copies, so the cost is a pointer either way.
func chunkWorker(
	ctx context.Context,
	src source.FileSource,
	files []source.FileEntry,
	cfg chunker.Config,
	fileCh <-chan int,
	blobCh chan<- blobItem,
	st *backupState,
	opts Options,
) error {
	for {
		idx, ok, canceled := recv(ctx, fileCh)
		if canceled || !ok {
			return nil
		}

		ids, read, err := chunkOneFile(ctx, src, files[idx], cfg, blobCh, st, opts)
		if err != nil {
			return err
		}

		st.mu.Lock()
		st.chunks[idx] = ids
		st.filesDone++
		st.bytesRead += read
		done := st.filesDone
		st.mu.Unlock()

		if opts.Progress != nil && done%16 == 0 {
			opts.Progress(st.progress())
		}
	}
}

func chunkOneFile(
	ctx context.Context,
	src source.FileSource,
	f source.FileEntry,
	cfg chunker.Config,
	blobCh chan<- blobItem,
	st *backupState,
	opts Options,
) ([]blob.ID, int64, error) {
	const op = "pipeline.chunkFile"

	rc, err := src.Open(ctx, f.Path)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", f.Path, err)
	}
	defer rc.Close() //nolint:errcheck // read path; the chunking error is what matters

	ck, err := chunker.New(rc, cfg)
	if err != nil {
		return nil, 0, err
	}

	var ids []blob.ID
	var read int64

	for {
		chunk, err := ck.Next()
		if errors.Is(err, io.EOF) {
			return ids, read, nil
		}
		if err != nil {
			return nil, 0, fmt.Errorf("%s: %w", f.Path, err)
		}

		read += int64(chunk.Len())
		if opts.MaxBytes > 0 {
			st.mu.Lock()
			total := st.bytesRead + read
			st.mu.Unlock()
			if total > opts.MaxBytes {
				return nil, 0, errs.E(errs.KindInvalid, op,
					fmt.Errorf("%w: read %d bytes, limit %d", ErrMaxBytesExceeded, total, opts.MaxBytes))
			}
		}

		id := blob.Compute(chunk.Data)
		ids = append(ids, id)

		if !send(ctx, blobCh, blobItem{id: id, data: chunk.Data}) {
			return nil, 0, nil
		}
	}
}

// assembler is the single goroutine that decides what actually gets stored.
//
// It holds `inFlight`: every blob ID this run has already committed to a
// pack. Combined with the repository index, that is the complete picture of
// what exists, and because only this goroutine consults and updates it, the
// check-and-claim is atomic without a lock.
func assembler(
	ctx context.Context,
	r *repo.Repository,
	blobCh <-chan blobItem,
	packCh chan<- packItem,
	packTarget int64,
	st *backupState,
) error {
	inFlight := make(map[blob.ID]struct{})

	var buf writeBuffer
	var w *pack.Writer

	flush := func() error {
		if w == nil || w.Count() == 0 {
			return nil
		}
		id, hdr, err := w.Finish()
		if err != nil {
			return err
		}
		item := packItem{id: id, data: buf.Bytes(), hdr: hdr}
		w, buf = nil, writeBuffer{}
		if !send(ctx, packCh, item) {
			return nil
		}
		return nil
	}

	for {
		item, ok, canceled := recv(ctx, blobCh)
		if canceled {
			return nil
		}
		if !ok {
			return flush()
		}

		if _, claimed := inFlight[item.id]; claimed || r.HasBlob(item.id) {
			st.mu.Lock()
			st.blobsDeduped++
			st.bytesDeduped += int64(len(item.data))
			st.mu.Unlock()
			continue
		}
		inFlight[item.id] = struct{}{}

		if w == nil {
			w = pack.NewWriter(&buf)
		}
		added, err := w.Add(item.id, item.data)
		if err != nil {
			return err
		}
		if added {
			st.mu.Lock()
			st.blobsNew++
			st.bytesNew += int64(len(item.data))
			st.mu.Unlock()
		}

		if w.IsFull(packTarget) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// writeBuffer is a minimal append-only byte buffer.
//
// bytes.Buffer would work; this exists so the pack writer's output is owned
// by a value that can be handed downstream and then replaced, making it
// obvious at the call site that the assembler never writes into a buffer it
// has already sent (D-008).
type writeBuffer struct{ b []byte }

func (w *writeBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// Bytes returns the accumulated bytes.
func (w *writeBuffer) Bytes() []byte { return w.b }

// uploadWorker stores finished packs.
func uploadWorker(
	ctx context.Context,
	r *repo.Repository,
	packCh <-chan packItem,
	st *backupState,
	opts Options,
) error {
	for {
		item, ok, canceled := recv(ctx, packCh)
		if canceled || !ok {
			return nil
		}

		if !opts.DryRun {
			if err := r.PutPack(ctx, item.id, item.data, item.hdr); err != nil {
				return err
			}
		}

		st.mu.Lock()
		st.packsWritten++
		st.mu.Unlock()
	}
}

// DefaultHostname returns the machine name for the snapshot manifest, or ""
// if it cannot be determined. A missing hostname is cosmetic, so it is never
// worth failing a backup over.
func DefaultHostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}
