// Command distbackup is a content-addressed, deduplicating backup tool.
//
// This is the only package that calls os.Exit (CLAUDE.md technical
// constraints). Everything below it returns errors.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/pipeline"
	"github.com/VardaanAggarwal/distbackup/internal/repo"
	"github.com/VardaanAggarwal/distbackup/internal/source"
	srclocal "github.com/VardaanAggarwal/distbackup/internal/source/localfs"
	"github.com/VardaanAggarwal/distbackup/internal/store"
	storelocal "github.com/VardaanAggarwal/distbackup/internal/store/localfs"
)

const usage = `distbackup — content-addressed deduplicating backup

Usage:
  distbackup <command> [flags]

Commands:
  init       Create a new repository
  backup     Back up a directory into a repository
  restore    Restore a snapshot into a directory
  snapshots  List snapshots in a repository
  verify     Check repository integrity
  gc         Reclaim space from unreferenced packs
  version    Print version information

Run "distbackup <command> -h" for a command's flags.

Note: distbackup never contacts a cloud provider. Cloud backends are modelled
against published API contracts and exercised against local fakes. See README.
`

// version is set at build time via -ldflags. It is deliberately "dev" by
// default rather than a plausible-looking number, so an unstamped binary
// cannot misreport itself (CLAUDE.md R2).
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	// SIGINT/SIGTERM cancels the root context. Every blocking call in the
	// engine takes a context, so cancellation propagates to every worker and
	// the run stops without writing a snapshot manifest — which means an
	// interrupted backup leaves orphan packs, never a dangling reference.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1], os.Args[2:]); err != nil {
		if errs.KindOf(err) == errs.KindCanceled {
			fmt.Fprintln(os.Stderr, "interrupted; no snapshot was written")
			os.Exit(130) // 128 + SIGINT, the shell convention
		}
		fmt.Fprintf(os.Stderr, "distbackup: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, args []string) error {
	switch cmd {
	case "init":
		return cmdInit(ctx, args)
	case "backup":
		return cmdBackup(ctx, args)
	case "restore":
		return cmdRestore(ctx, args)
	case "snapshots", "list":
		return cmdSnapshots(ctx, args)
	case "verify":
		return cmdVerify(ctx, args)
	case "gc":
		return cmdGC(ctx, args)
	case "version":
		fmt.Printf("distbackup %s\n", version)
		return nil
	case "-h", "--help", "help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// openStore opens the object store backing a repository.
//
// Only the local filesystem backend is wired up. The S3 backend exists and
// passes the same conformance suite, but it is not reachable from the CLI:
// under CLAUDE.md R7 this binary must not be able to contact a real cloud
// account even by accident, and the surest way to guarantee that is for the
// command line to offer no way to ask for one.
func openStore(path string) (store.ObjectStore, error) {
	return storelocal.Open(path)
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func cmdInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	repoPath := fs.String("repo", "", "repository path (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repoPath == "" {
		return errors.New("init: --repo is required")
	}

	s, err := openStore(*repoPath)
	if err != nil {
		return err
	}
	defer s.Close() //nolint:errcheck // the create error is what matters

	if _, err := repo.Create(ctx, s, repo.DefaultConfig()); err != nil {
		return err
	}
	fmt.Printf("initialised repository at %s (format version %d)\n", *repoPath, repo.FormatVersion)
	return nil
}

func cmdBackup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	repoPath := fs.String("repo", "", "repository path (required)")
	srcPath := fs.String("source", "", "directory to back up (required)")
	dryRun := fs.Bool("dry-run", false, "enumerate and measure without writing anything")
	maxBytes := fs.Int64("max-bytes", 0, "abort after reading this many source bytes (0 = no limit)")
	chunkWorkers := fs.Int("chunk-workers", 0, "chunking goroutines (0 = NumCPU)")
	uploadWorkers := fs.Int("upload-workers", 0, "upload goroutines (0 = auto)")
	skipUnreadable := fs.Bool("skip-unreadable", false, "skip files that cannot be read instead of failing")
	verbose := fs.Bool("v", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repoPath == "" || *srcPath == "" {
		return errors.New("backup: --repo and --source are required")
	}

	log := newLogger(*verbose)

	s, err := openStore(*repoPath)
	if err != nil {
		return err
	}
	r, err := repo.Open(ctx, s)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck // the backup error is what matters

	var srcOpts []srclocal.Option
	if *skipUnreadable {
		srcOpts = append(srcOpts, srclocal.WithSkipUnreadable())
	}
	src, err := srclocal.Open(*srcPath, srcOpts...)
	if err != nil {
		return err
	}
	defer src.Close() //nolint:errcheck // read-only source

	// Cost guardrail line, required by CLAUDE.md R7 before any provider call.
	// The local backend costs nothing, and saying so plainly is more useful
	// than omitting the line — it makes the absence of a charge explicit
	// rather than leaving the reader to assume.
	est, err := estimate(ctx, src)
	if err != nil {
		return err
	}
	log.Info("backup starting",
		"repo", *repoPath,
		"source", *srcPath,
		"files", est.files,
		"bytes", est.bytes,
		"backend", "local filesystem",
		"estimated_requests", est.requests(r.Config().PackTargetSize),
		"estimated_cost_usd", "0.00 (local backend makes no billable requests)",
		"dry_run", *dryRun,
	)

	if *maxBytes > 0 {
		log.Info("byte cap in effect", "max_bytes", *maxBytes)
	}

	opts := pipeline.Options{
		ChunkWorkers:  *chunkWorkers,
		UploadWorkers: *uploadWorkers,
		DryRun:        *dryRun,
		MaxBytes:      *maxBytes,
		Hostname:      pipeline.DefaultHostname(),
	}
	if *verbose {
		opts.Progress = func(p pipeline.Progress) {
			log.Debug("progress",
				"files", p.FilesDone, "bytes", p.BytesRead,
				"blobs_new", p.BlobsNew, "blobs_deduped", p.BlobsDeduped)
		}
	}

	snap, err := pipeline.BackupFiles(ctx, r, src, opts)
	if err != nil {
		return err
	}

	printBackupResult(snap, *dryRun)
	return nil
}

type estimateResult struct {
	files int
	bytes int64
}

// requests approximates how many store writes a run would make.
//
// It is an estimate and is labelled as one: the true pack count depends on
// how much deduplicates, which is not known until the data is read. It is the
// upper bound — what a run would cost if nothing deduplicated at all.
func (e estimateResult) requests(packTarget int64) int64 {
	if packTarget <= 0 {
		return 0
	}
	packs := (e.bytes + packTarget - 1) / packTarget
	// packs + index + manifest
	return packs + 2
}

func estimate(ctx context.Context, src *srclocal.Source) (estimateResult, error) {
	var e estimateResult
	err := src.Walk(ctx, func(f source.FileEntry) error {
		e.files++
		e.bytes += f.Size
		return nil
	})
	return e, err
}

func printBackupResult(snap *repo.Snapshot, dryRun bool) {
	st := snap.Stats
	if dryRun {
		fmt.Println("dry run — nothing was written")
	} else {
		fmt.Printf("snapshot %s\n", snap.ID[:16])
	}
	fmt.Printf("  files            %d\n", st.FilesTotal)
	fmt.Printf("  source bytes     %s\n", humanBytes(st.BytesTotal))
	fmt.Printf("  blobs stored     %d (%s)\n", st.BlobsNew, humanBytes(st.BytesNew))
	fmt.Printf("  blobs deduped    %d (%s)\n", st.BlobsDeduped, humanBytes(st.BytesDeduped))
	fmt.Printf("  dedup ratio      %.1f%%\n", st.DedupRatio()*100)
	fmt.Printf("  packs written    %d\n", st.PacksWritten)
	fmt.Printf("  duration         %s\n", st.Duration.Round(time.Millisecond))
}

func cmdRestore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	repoPath := fs.String("repo", "", "repository path (required)")
	snapID := fs.String("snapshot", "", "snapshot ID, or \"latest\" (required)")
	target := fs.String("target", "", "directory to restore into (required)")
	overwrite := fs.Bool("overwrite", false, "replace files that already exist at the target")
	workers := fs.Int("workers", 0, "restore goroutines (0 = NumCPU)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repoPath == "" || *snapID == "" || *target == "" {
		return errors.New("restore: --repo, --snapshot and --target are required")
	}

	s, err := openStore(*repoPath)
	if err != nil {
		return err
	}
	r, err := repo.Open(ctx, s)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck // the restore error is what matters

	snap, err := resolveSnapshot(ctx, r, *snapID)
	if err != nil {
		return err
	}

	report, err := pipeline.RestoreFiles(ctx, r, snap, *target, pipeline.RestoreOptions{
		Workers:   *workers,
		Overwrite: *overwrite,
	})
	if err != nil {
		return err
	}

	fmt.Printf("restored %d files (%s) from snapshot %s in %s\n",
		report.FilesRestored, humanBytes(report.BytesWritten),
		snap.ID[:16], report.Duration.Round(time.Millisecond))
	return nil
}

// resolveSnapshot accepts a full ID, a unique prefix, or "latest".
func resolveSnapshot(ctx context.Context, r *repo.Repository, ref string) (*repo.Snapshot, error) {
	snaps, err := r.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	if len(snaps) == 0 {
		return nil, errors.New("repository has no snapshots")
	}

	if ref == "latest" {
		// "Latest" means most recently created, which is the one case where
		// CreatedAt has to be trusted despite coming from a client clock.
		// The alternative — highest ID — would be meaningless, since an ID is
		// a hash. Documented rather than hidden.
		newest := snaps[0]
		for _, s := range snaps[1:] {
			if s.CreatedAt.After(newest.CreatedAt) {
				newest = s
			}
		}
		return newest, nil
	}

	var matches []*repo.Snapshot
	for _, s := range snaps {
		if s.ID == ref || (len(ref) >= 8 && len(s.ID) >= len(ref) && s.ID[:len(ref)] == ref) {
			matches = append(matches, s)
		}
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("no snapshot matches %q", ref)
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("%q matches %d snapshots; use a longer prefix", ref, len(matches))
	}
}

func cmdSnapshots(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshots", flag.ExitOnError)
	repoPath := fs.String("repo", "", "repository path (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repoPath == "" {
		return errors.New("snapshots: --repo is required")
	}

	s, err := openStore(*repoPath)
	if err != nil {
		return err
	}
	r, err := repo.Open(ctx, s)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck // read-only command

	snaps, err := r.ListSnapshots(ctx)
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		fmt.Println("no snapshots")
		return nil
	}

	fmt.Printf("%-18s  %-20s  %8s  %10s  %s\n", "ID", "CREATED", "FILES", "SIZE", "SOURCE")
	for _, snap := range snaps {
		fmt.Printf("%-18s  %-20s  %8d  %10s  %s\n",
			snap.ID[:16],
			snap.CreatedAt.Format("2006-01-02 15:04:05"),
			snap.Stats.FilesTotal,
			humanBytes(snap.Stats.BytesTotal),
			snap.Source)
	}
	return nil
}

func cmdVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	repoPath := fs.String("repo", "", "repository path (required)")
	full := fs.Bool("full", false, "read and re-hash every blob (slow, thorough)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repoPath == "" {
		return errors.New("verify: --repo is required")
	}

	s, err := openStore(*repoPath)
	if err != nil {
		return err
	}
	r, err := repo.Open(ctx, s)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck // read-only command

	report, err := r.Verify(ctx, repo.VerifyOptions{Full: *full})
	if err != nil {
		return err
	}

	fmt.Println(report.Summary())
	for _, p := range report.Problems {
		fmt.Fprintf(os.Stderr, "  problem: %v\n", p)
	}
	if len(report.OrphanPacks) > 0 {
		fmt.Printf("  %d orphan packs (harmless; run \"distbackup gc\" to reclaim)\n", len(report.OrphanPacks))
	}
	if !report.OK() {
		return errors.New("verification failed")
	}
	return nil
}

func cmdGC(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	repoPath := fs.String("repo", "", "repository path (required)")
	dryRun := fs.Bool("dry-run", false, "report what would be reclaimed without deleting")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repoPath == "" {
		return errors.New("gc: --repo is required")
	}

	s, err := openStore(*repoPath)
	if err != nil {
		return err
	}
	r, err := repo.Open(ctx, s)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck // the gc error is what matters

	report, err := r.CollectGarbage(ctx, *dryRun)
	if err != nil {
		return err
	}

	verb := "reclaimed"
	if report.DryRun {
		verb = "would reclaim"
	}
	fmt.Printf("%s %d packs (%s)\n", verb, report.PacksDeleted, humanBytes(report.BytesReclaimed))
	return nil
}

// humanBytes formats a byte count with binary units.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}
