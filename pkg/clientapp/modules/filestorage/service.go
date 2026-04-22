package filestorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type service struct {
	host   moduleapi.FileStorageHost
	seed   moduleapi.SeedStorage
	watch  *watchService
	mu     sync.Mutex
	opened bool
}

func newService(host moduleapi.FileStorageHost) (*service, error) {
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}
	seed := host.SeedStorage()
	if seed == nil {
		return nil, fmt.Errorf("seed storage is required")
	}
	return &service{
		host:  host,
		seed:  seed,
		watch: nil,
	}, nil
}

func (s *service) syncOnce(ctx context.Context) error {
	return s.syncAll(ctx)
}

func (s *service) syncAll(ctx context.Context) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("file storage service is not ready")
	}
	store := s.host.Store()
	if store == nil {
		return fmt.Errorf("store is required")
	}
	if s.seed == nil {
		return fmt.Errorf("seed storage is required")
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, 128)
	for _, root := range roots {
		root := strings.TrimSpace(root)
		if root == "" {
			continue
		}
		err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			abs, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, abs)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(filepath.Clean(rel))
			if rel == "." || rel == "" || strings.HasPrefix(rel, "../") {
				return nil
			}
			key := root + "\x00" + rel
			seen[key] = struct{}{}
			st, err := os.Stat(abs)
			if err != nil {
				return err
			}
			if !st.Mode().IsRegular() {
				return nil
			}
			return s.updateFileIfStable(ctx, abs)
		})
		if err != nil {
			return err
		}
	}
	if err := cleanupMissingWorkspaceFiles(ctx, store, roots, seen, ""); err != nil {
		return err
	}
	return s.seed.CleanupOrphanSeeds(ctx)
}

func (s *service) syncDir(ctx context.Context, dir string) error {
	if s == nil {
		return fmt.Errorf("file storage service is not ready")
	}
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return s.syncAll(ctx)
	}
	absDir := filepath.Clean(dir)
	if absDir == "" {
		return s.syncAll(ctx)
	}
	store := s.host.Store()
	if store == nil {
		return fmt.Errorf("store is required")
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, 64)
	if err := filepath.WalkDir(absDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		root, rel, ok := resolveWorkspaceRelativePath(abs, roots)
		if !ok {
			return nil
		}
		key := root + "\x00" + rel
		seen[key] = struct{}{}
		return s.updateFileIfStable(ctx, abs)
	}); err != nil {
		return err
	}
	return cleanupMissingWorkspaceFiles(ctx, store, roots, seen, absDir)
}

func (s *service) updateFileIfStable(ctx context.Context, path string) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("file storage service is not ready")
	}
	store := s.host.Store()
	if store == nil {
		return fmt.Errorf("store is required")
	}
	abs := filepath.Clean(strings.TrimSpace(path))
	if abs == "" {
		return fmt.Errorf("file path is required")
	}
	st, err := os.Stat(abs)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return nil
	}
	seedRec, err := s.seed.EnsureSeedForFile(ctx, moduleapi.SeedEnsureInput{
		FilePath:            abs,
		RecommendedFileName: filepath.Base(abs),
		MimeHint:            "",
	})
	if err != nil {
		return err
	}
	root, rel, ok := resolveWorkspaceRelativePath(abs, mustWorkspaceRoots(ctx, store))
	if !ok {
		return nil
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return dbUpsertWorkspaceFileTx(ctx, tx, root, rel, seedRec.SeedHash, false)
	})
}

func (s *service) deleteFile(ctx context.Context, path string) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("file storage service is not ready")
	}
	store := s.host.Store()
	if store == nil {
		return fmt.Errorf("store is required")
	}
	abs := filepath.Clean(strings.TrimSpace(path))
	if abs == "" {
		return fmt.Errorf("file path is required")
	}
	root, rel, ok := resolveWorkspaceRelativePath(abs, mustWorkspaceRoots(ctx, store))
	if !ok {
		return nil
	}
	if err := store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return dbDeleteWorkspaceFileTx(ctx, tx, root, rel)
	}); err != nil {
		return err
	}
	return s.seed.CleanupOrphanSeeds(ctx)
}

func (s *service) registerDownloadedFile(ctx context.Context, absPath string, seedHash string, seedLocked bool, recommendedName, mimeHint string) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("file storage service is not ready")
	}
	store := s.host.Store()
	if store == nil {
		return fmt.Errorf("store is required")
	}
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return fmt.Errorf("output path is required")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	relRoot, relPath, ok := resolveWorkspaceRelativePath(absPath, mustWorkspaceRoots(ctx, store))
	if !ok {
		return fmt.Errorf("output path is outside registered biz_workspaces")
	}
	if err := store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return dbUpsertWorkspaceFileTx(ctx, tx, relRoot, relPath, seedHash, seedLocked)
	}); err != nil {
		return err
	}
	return nil
}

func (s *service) ensureWorkspace(ctx context.Context, path string, maxBytes uint64) error {
	_, err := dbUpsertWorkspace(ctx, s.host.Store(), path, maxBytes, true)
	return err
}

func (s *service) deleteWorkspace(ctx context.Context, path string) error {
	return dbDeleteWorkspace(ctx, s.host.Store(), path)
}

func (s *service) updateWorkspace(ctx context.Context, path string, maxBytes *uint64, enabled *bool) error {
	_, err := dbUpdateWorkspace(ctx, s.host.Store(), path, maxBytes, enabled)
	return err
}

func (s *service) listWorkspaces(ctx context.Context) ([]workspaceItem, error) {
	return dbListWorkspaces(ctx, s.host.Store())
}

func cleanupMissingWorkspaceFiles(ctx context.Context, store moduleapi.Store, roots []string, seen map[string]struct{}, absDir string) error {
	files, _, err := dbListWorkspaceFiles(ctx, store, -1, 0, "")
	if err != nil {
		return err
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		for _, row := range files {
			abs := filepath.Clean(filepath.Join(row.WorkspacePath, filepath.FromSlash(row.FilePath)))
			if absDir != "" {
				if rel, err := filepath.Rel(absDir, abs); err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					continue
				}
			}
			key := filepath.Clean(row.WorkspacePath) + "\x00" + filepath.ToSlash(filepath.Clean(row.FilePath))
			if _, ok := seen[key]; ok {
				continue
			}
			if err := dbDeleteWorkspaceFileTx(ctx, tx, row.WorkspacePath, row.FilePath); err != nil {
				return err
			}
		}
		return nil
	})
}

func mustWorkspaceRoots(ctx context.Context, store moduleapi.Store) []string {
	roots, _ := dbListWorkspaceRoots(ctx, store)
	return roots
}

func resolveWorkspaceRelativePath(absPath string, workspaceRoots []string) (string, string, bool) {
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return "", "", false
	}
	for _, root := range workspaceRoots {
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "" {
			continue
		}
		rel, err := filepath.Rel(root, absPath)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(filepath.Clean(rel))
		if rel == "." || rel == "" || strings.HasPrefix(rel, "../") {
			continue
		}
		return root, rel, true
	}
	return "", "", false
}

func (s *service) open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened {
		return nil
	}
	s.opened = true
	s.ensureWatcher()
	if err := s.syncOnce(ctx); err != nil {
		return err
	}
	if s.watch != nil && s.shouldScanAt(ctx) {
		if err := s.watch.start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opened {
		return nil
	}
	s.opened = false
	if s.watch != nil {
		return s.watch.close(ctx)
	}
	return nil
}

func (s *service) reloadWatcher(ctx context.Context) error {
	if s == nil || s.watch == nil {
		return nil
	}
	return s.watch.reload(ctx)
}

func (s *service) shouldScanAt(ctx context.Context) bool {
	return s != nil && s.host != nil && s.host.FSWatchEnabled()
}

func (s *service) scanInterval() time.Duration {
	if s == nil || s.host == nil {
		return 0
	}
	sec := s.host.FSRescanIntervalSeconds()
	if sec == 0 {
		sec = 30
	}
	return time.Duration(sec) * time.Second
}
