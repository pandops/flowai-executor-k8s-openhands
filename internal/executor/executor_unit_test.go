package executor

import (
	"path/filepath"
	"testing"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
)

// TestCacheDirAndDefaults keeps the pre-v0048 oracle: the documented cache
// directory and the base data directory it lives in are not empty. v0048
// moved the documented cache directory from CacheDirPath() (DataDirPath() +
// "/cache") to the cache_dir default of config.Defaults().
func TestCacheDirAndDefaults(t *testing.T) {
	cacheDir := config.Defaults().CacheDir
	if cacheDir == "" {
		t.Fatalf("default cache_dir is empty")
	}
	if dataDir := filepath.Dir(cacheDir); dataDir == "" || dataDir == "." {
		t.Fatalf("data dir of cache_dir %q is empty", cacheDir)
	}
}
