package engine

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/tnn1t1s/riemann-go"

// core packages import stdlib plus expr-lang/expr and each other, nothing
// else (invariant 8).
var core = []string{"event", "expr", "stream", "rule", "index", "shard", "engine", "sink"}

// adapters import core, never each other and never cmd.
var adapters = []string{"transport/http", "sink/ntfy", "sink/influx", "adapter/prom", "adapter/selfmetrics"}

func isStdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func isCore(path string) bool {
	for _, c := range core {
		if path == module+"/"+c {
			return true
		}
	}
	return false
}

func importsOf(t *testing.T, dir string) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("%s: %v", dir, err)
	}
	out := map[string][]string{}
	for _, p := range pkgs {
		for name, f := range p.Files {
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				out[name] = append(out[name], path)
			}
		}
	}
	return out
}

func TestImportGraph(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range core {
		dir := filepath.Join(root, pkg)
		if _, err := os.Stat(dir); err != nil {
			continue // not built yet
		}
		for file, imports := range importsOf(t, dir) {
			for _, imp := range imports {
				if isStdlib(imp) || isCore(imp) || strings.HasPrefix(imp, "github.com/expr-lang/expr") {
					continue
				}
				t.Errorf("core package %s (%s) imports %s", pkg, filepath.Base(file), imp)
			}
		}
	}
	for _, pkg := range adapters {
		dir := filepath.Join(root, pkg)
		if _, err := os.Stat(dir); err != nil {
			continue
		}
		for file, imports := range importsOf(t, dir) {
			for _, imp := range imports {
				if !strings.HasPrefix(imp, module+"/") {
					continue
				}
				if isCore(imp) {
					continue
				}
				t.Errorf("adapter %s (%s) imports %s", pkg, filepath.Base(file), imp)
			}
		}
	}
}
