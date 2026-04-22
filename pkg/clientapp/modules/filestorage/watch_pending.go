package filestorage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type pendingQueue struct {
	mu         sync.Mutex
	dirtyFiles map[string]time.Time
	dirtyDirs  map[string]time.Time
	fileFails  map[string]int
	dirFails   map[string]int
	closing    bool
}

func newPendingQueue() *pendingQueue {
	return &pendingQueue{
		dirtyFiles: map[string]time.Time{},
		dirtyDirs:  map[string]time.Time{},
		fileFails:  map[string]int{},
		dirFails:   map[string]int{},
	}
}

func (q *pendingQueue) markFile(path string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closing {
		return
	}
	q.dirtyFiles[path] = time.Now()
}

func (q *pendingQueue) markDir(path string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closing {
		return
	}
	q.dirtyDirs[path] = time.Now()
}

func (q *pendingQueue) takeStable(window time.Duration) (files []string, dirs []string) {
	if q == nil {
		return nil, nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for path, last := range q.dirtyFiles {
		if now.Sub(last) < window {
			continue
		}
		files = append(files, path)
		delete(q.dirtyFiles, path)
	}
	for path, last := range q.dirtyDirs {
		if now.Sub(last) < window {
			continue
		}
		dirs = append(dirs, path)
		delete(q.dirtyDirs, path)
	}
	return files, dirs
}

func (q *pendingQueue) close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.closing = true
	q.dirtyFiles = map[string]time.Time{}
	q.dirtyDirs = map[string]time.Time{}
	q.fileFails = map[string]int{}
	q.dirFails = map[string]int{}
	q.mu.Unlock()
}

func (s *service) processPending(ctx context.Context, q *pendingQueue, stableWindow time.Duration) {
	if s == nil || q == nil {
		return
	}
	if stableWindow <= 0 {
		stableWindow = 3 * time.Second
	}
	tickEvery := stableWindow / 2
	if tickEvery <= 0 {
		tickEvery = time.Second
	}
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		files, dirs := q.takeStable(stableWindow)
		if len(files) == 0 && len(dirs) == 0 {
			continue
		}
		for _, path := range files {
			if err := s.updateFileIfStable(ctx, path); err != nil {
				s.handlePendingFileError(q, path, err)
			} else {
				q.clearFile(path)
			}
		}
		for _, dir := range dirs {
			if err := s.syncDir(ctx, dir); err != nil {
				s.handlePendingDirError(q, dir, err)
			} else {
				q.clearDir(dir)
			}
		}
	}
}

func (s *service) handlePendingFileError(q *pendingQueue, path string, err error) {
	if q == nil || err == nil {
		return
	}
	if moduleapi.CodeOf(err) == "FILE_NOT_STABLE" {
		q.markFile(path)
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		q.clearFile(path)
		q.markDir(filepath.Dir(path))
		obs.Error(ModuleID, "file_storage_watch_file_missing", map[string]any{
			"path":  path,
			"error": err.Error(),
		})
		return
	}
	if isTransientWatchError(err) {
		if q.nextRetry(path, true) {
			q.markFile(path)
			obs.Business(ModuleID, "file_storage_watch_file_retry", map[string]any{
				"path":      path,
				"error":     err.Error(),
				"attempts":  q.retryCount(path, true),
				"temporary": true,
			})
			return
		}
	}
	obs.Error(ModuleID, "file_storage_watch_file_dropped", map[string]any{
		"path":  path,
		"error": err.Error(),
	})
}

func (s *service) handlePendingDirError(q *pendingQueue, dir string, err error) {
	if q == nil || err == nil {
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		q.markDir(filepath.Dir(dir))
		obs.Error(ModuleID, "file_storage_watch_dir_missing", map[string]any{
			"path":  dir,
			"error": err.Error(),
		})
		return
	}
	if isTransientWatchError(err) {
		if q.nextRetry(dir, false) {
			q.markDir(dir)
			obs.Business(ModuleID, "file_storage_watch_dir_retry", map[string]any{
				"path":      dir,
				"error":     err.Error(),
				"attempts":  q.retryCount(dir, false),
				"temporary": true,
			})
			return
		}
	}
	obs.Error(ModuleID, "file_storage_watch_dir_dropped", map[string]any{
		"path":  dir,
		"error": err.Error(),
	})
}

func (q *pendingQueue) nextRetry(path string, isFile bool) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	const maxRetries = 3
	if isFile {
		q.fileFails[path]++
		return q.fileFails[path] <= maxRetries
	}
	q.dirFails[path]++
	return q.dirFails[path] <= maxRetries
}

func (q *pendingQueue) retryCount(path string, isFile bool) int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if isFile {
		return q.fileFails[path]
	}
	return q.dirFails[path]
}

func (q *pendingQueue) clearFile(path string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	delete(q.fileFails, path)
	q.mu.Unlock()
}

func (q *pendingQueue) clearDir(path string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	delete(q.dirFails, path)
	q.mu.Unlock()
}

func isTransientWatchError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrPermission) {
		return true
	}
	var errno interface{ Temporary() bool }
	if errors.As(err, &errno) && errno.Temporary() {
		return true
	}
	return false
}
