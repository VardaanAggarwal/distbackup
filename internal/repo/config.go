package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/chunker"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/store"
)

// FormatVersion is the repository format this build reads and writes.
//
// It is checked on every Open. A reader that encounters a version it does not
// know refuses rather than guessing — see Config.Validate. Guessing is how a
// format becomes un-evolvable: once one build has silently mis-parsed a newer
// repository, no future version can safely change anything.
const FormatVersion = 1

// ConfigKey is the object key holding the repository configuration.
const ConfigKey = "config"

// Config is the repository's immutable settings, written once at creation.
type Config struct {
	// FormatVersion is the repository format version.
	FormatVersion int `json:"format_version"`

	// CreatedAt is when the repository was initialised. Informational only:
	// nothing orders or compares on it, because a client clock is not
	// trustworthy (docs/RISKS.md, clock skew).
	CreatedAt time.Time `json:"created_at"`

	// ChunkerMinSize, ChunkerAvgSize, ChunkerMaxSize and ChunkerNormalization
	// record the chunking parameters this repository was written with.
	//
	// They are stored because changing them changes every chunk boundary,
	// which silently destroys deduplication against everything already here —
	// backups would keep succeeding and simply stop sharing data. Recording
	// them lets a mismatch be detected and reported instead.
	ChunkerMinSize       int `json:"chunker_min_size"`
	ChunkerAvgSize       int `json:"chunker_avg_size"`
	ChunkerMaxSize       int `json:"chunker_max_size"`
	ChunkerNormalization int `json:"chunker_normalization"`

	// PackTargetSize is the size at which a pack is closed off.
	PackTargetSize int64 `json:"pack_target_size"`
}

// DefaultConfig returns the configuration a new repository is created with.
func DefaultConfig() Config {
	c := chunker.DefaultConfig()
	return Config{
		FormatVersion:        FormatVersion,
		CreatedAt:            time.Now().UTC(),
		ChunkerMinSize:       c.MinSize,
		ChunkerAvgSize:       c.AvgSize,
		ChunkerMaxSize:       c.MaxSize,
		ChunkerNormalization: c.Normalization,
		PackTargetSize:       defaultPackTargetSize,
	}
}

// ChunkerConfig reconstructs the chunker parameters this repository uses.
func (c Config) ChunkerConfig() chunker.Config {
	return chunker.Config{
		MinSize:       c.ChunkerMinSize,
		AvgSize:       c.ChunkerAvgSize,
		MaxSize:       c.ChunkerMaxSize,
		Normalization: c.ChunkerNormalization,
	}
}

// Validate checks that this build can work with the configuration.
func (c Config) Validate() error {
	const op = "repo.Config.Validate"

	if c.FormatVersion != FormatVersion {
		// Deliberately refuses both newer *and* older versions. A newer one
		// may contain data this build cannot interpret; an older one would
		// need migration code that does not exist. Either way, guessing is
		// worse than stopping.
		return errs.E(errs.KindUnsupported, op,
			fmt.Errorf("repository format version %d, this build understands %d",
				c.FormatVersion, FormatVersion))
	}
	if err := c.ChunkerConfig().Validate(); err != nil {
		return errs.E(errs.KindCorrupt, op, fmt.Errorf("invalid chunker parameters: %w", err))
	}
	if c.PackTargetSize <= 0 {
		return errs.E(errs.KindCorrupt, op, errors.New("pack target size must be positive"))
	}
	return nil
}

// writeConfig stores the configuration. It uses PutIfAbsent so that
// initialising an existing repository cannot silently overwrite the settings
// its data was written with.
func writeConfig(ctx context.Context, s store.ObjectStore, c Config) error {
	const op = "repo.writeConfig"

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return errs.E(errs.KindInvalid, op, err)
	}
	created, err := s.PutIfAbsent(ctx, ConfigKey, data)
	if err != nil {
		return err
	}
	if !created {
		return errs.E(errs.KindAlreadyExists, op,
			errors.New("a repository already exists here"))
	}
	return nil
}

// readConfig loads and validates the configuration.
func readConfig(ctx context.Context, s store.ObjectStore) (Config, error) {
	const op = "repo.readConfig"

	rc, err := s.Get(ctx, ConfigKey)
	if err != nil {
		if errs.IsNotFound(err) {
			return Config{}, errs.E(errs.KindNotFound, op,
				errors.New("no repository here: config is missing"))
		}
		return Config{}, err
	}
	defer rc.Close() //nolint:errcheck // read-only path; the decode error is what matters

	data, err := io.ReadAll(rc)
	if err != nil {
		return Config{}, errs.E(errs.KindTransient, op, err)
	}

	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, errs.E(errs.KindCorrupt, op, fmt.Errorf("decoding config: %w", err))
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}
