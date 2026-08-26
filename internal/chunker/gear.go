package chunker

// gearTable maps each possible byte value to a 64-bit random word. It is the
// substitution table for the Gear rolling hash.
//
// The table is generated deterministically at init rather than written out as
// 256 magic constants. Two reasons:
//
//  1. A literal table is 256 unverifiable numbers. Nobody reviewing this can
//     tell a correct table from one with a typo, and a single wrong entry
//     would degrade boundary quality in a way no test would obviously catch.
//  2. Generated from a named algorithm and a fixed seed, the table is
//     reproducible by anyone who reads this file, and its provenance is the
//     twenty lines below rather than "copied from somewhere".
//
// THIS TABLE IS PART OF THE ON-DISK FORMAT. Changing the seed or the
// generator changes every chunk boundary this program will ever produce,
// which silently destroys deduplication against every existing repository —
// backups would still succeed, and would simply stop sharing any data with
// what came before. TestGearTableIsPinned guards it with a golden checksum so
// an accidental change fails loudly instead of quietly.
var gearTable [256]uint64

// gearSeed is fixed forever. See the warning above.
const gearSeed uint64 = 0x9E3779B97F4A7C15

func init() {
	state := gearSeed
	for i := range gearTable {
		gearTable[i] = splitmix64(&state)
	}
}

// splitmix64 is the SplitMix64 pseudo-random generator.
//
// Chosen because it is a dozen lines, has no state beyond a single uint64,
// passes the standard statistical test suites, and is trivially reproducible
// in any language — someone verifying this table does not need Go.
// Rejected: math/rand, whose output is explicitly not guaranteed stable
// across Go releases. A table that changes when the toolchain is upgraded
// would silently break deduplication against every prior backup.
func splitmix64(state *uint64) uint64 {
	*state += 0x9E3779B97F4A7C15
	z := *state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
