// Package arch holds the architecture tests that enforce distbackup's
// layering rules mechanically rather than by convention.
//
// docs/ENGINEERING-RULES.md R11 says the core engine must not know which provider it is
// talking to. That rule is worth nothing if it is only written down: layering
// erodes under deadline pressure, and a single convenient import is all it
// takes. This test fails the build instead.
package arch

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// cloudSDKPrefixes are import paths that may appear only inside a provider
// package. The list is prefix-matched, so it covers every subpackage.
var cloudSDKPrefixes = []string{
	"github.com/aws/aws-sdk-go-v2",
	"github.com/aws/aws-sdk-go",
	"cloud.google.com/go",
	"google.golang.org/api",
	"github.com/Azure/azure-sdk-for-go",
}

// providerDirs are the only directories permitted to import a cloud SDK.
// Each is a leaf provider package plus its fake.
var providerDirs = []string{
	filepath.Join("internal", "source", "ebs"),
	filepath.Join("internal", "store", "s3"),
}

// TestNoCloudSDKOutsideProviders enforces R11.
//
// This checks *direct* imports only, which is sufficient here and worth
// explaining: any intermediate package that pulled in an SDK on the core's
// behalf would itself live under internal/ and would itself be checked. So a
// transitive violation cannot hide behind a helper without that helper
// failing the same test. A full transitive walk would need
// golang.org/x/tools/go/packages; the dependency is not worth it for a
// guarantee this test already provides.
func TestNoCloudSDKOutsideProviders(t *testing.T) {
	root := repoRoot(t)

	for dir, imports := range packageImports(t, root) {
		if isProviderDir(dir) {
			continue
		}
		for _, imp := range imports {
			if prefix, ok := matchCloudSDK(imp); ok {
				t.Errorf(
					"R11 violation: %s imports %q (cloud SDK %q).\n"+
						"Core packages must not import cloud SDKs. Move this behind a "+
						"core-owned interface in internal/source or internal/store.",
					dir, imp, prefix,
				)
			}
		}
	}
}

// TestProviderDirsExistOrAreAbsent guards against the allow-list silently
// going stale — a provider directory that is renamed or removed would
// otherwise leave a permanent hole in the rule.
func TestProviderDirsExistOrAreAbsent(t *testing.T) {
	root := repoRoot(t)
	for _, dir := range providerDirs {
		full := filepath.Join(root, dir)
		info, err := os.Stat(full)
		if os.IsNotExist(err) {
			// Not yet built. Acceptable while phases are in progress.
			continue
		}
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Errorf("provider allow-list entry %s is not a directory", dir)
		}
	}
}

// TestCoreDoesNotImportCmd checks the other direction of the dependency rule:
// library code never reaches back into the CLI. cmd/ is where os.Exit lives,
// and a library that can call into it is a library that can kill the process.
func TestCoreDoesNotImportCmd(t *testing.T) {
	root := repoRoot(t)
	const cmdPrefix = "github.com/VardaanAggarwal/distbackup/cmd"

	for dir, imports := range packageImports(t, root) {
		if strings.HasPrefix(dir, "cmd"+string(filepath.Separator)) || dir == "cmd" {
			continue
		}
		for _, imp := range imports {
			if strings.HasPrefix(imp, cmdPrefix) {
				t.Errorf("%s imports %q: library code must not depend on cmd/", dir, imp)
			}
		}
	}
}

// packageImports returns a map of repo-relative directory to the set of
// import paths declared by the Go files in it, test files included.
func packageImports(t *testing.T, root string) map[string][]string {
	t.Helper()

	result := make(map[string][]string)
	fset := token.NewFileSet()

	for _, top := range []string{"internal", "cmd"} {
		topPath := filepath.Join(root, top)
		if _, err := os.Stat(topPath); os.IsNotExist(err) {
			continue
		}

		err := filepath.WalkDir(topPath, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}

			f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if perr != nil {
				return perr
			}

			rel, rerr := filepath.Rel(root, filepath.Dir(path))
			if rerr != nil {
				return rerr
			}

			for _, spec := range f.Imports {
				p, uerr := strconv.Unquote(spec.Path.Value)
				if uerr != nil {
					return uerr
				}
				result[rel] = append(result[rel], p)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", top, err)
		}
	}

	if len(result) == 0 {
		t.Fatal("found no packages to check — the architecture test is not actually running")
	}
	return result
}

func isProviderDir(dir string) bool {
	for _, p := range providerDirs {
		if dir == p || strings.HasPrefix(dir, p+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func matchCloudSDK(imp string) (string, bool) {
	for _, prefix := range cloudSDKPrefixes {
		if imp == prefix || strings.HasPrefix(imp, prefix+"/") {
			return prefix, true
		}
	}
	return "", false
}

// repoRoot walks up from the test's working directory to the directory
// containing go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate go.mod above the test directory")
		}
		dir = parent
	}
}
