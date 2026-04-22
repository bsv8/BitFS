package filestorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type watchService struct {
	svc     *service
	watcher *fsnotify.Watcher
	pending *pendingQueue
	cancel  context.CancelFunc
	roots   []string
}

func newWatchService(svc *service) *watchService {
	return &watchService{svc: svc, pending: newPendingQueue()}
}

func (w *watchService) start(ctx context.Context) error {
	if w == nil || w.svc == nil || w.svc.host == nil {
		return fmt.Errorf("watch service is not ready")
	}
	if !w.svc.shouldScanAt(ctx) {
		return nil
	}
	if w.watcher != nil {
		return nil
	}
	w.pending = newPendingQueue()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	w.watcher = watcher
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	roots, err := dbListWorkspaceRoots(ctx, w.svc.host.Store())
	if err != nil {
		_ = watcher.Close()
		w.watcher = nil
		if cancel != nil {
			cancel()
		}
		w.cancel = nil
		if w.pending != nil {
			w.pending.close()
			w.pending = nil
		}
		w.roots = nil
		return err
	}
	w.roots = append([]string(nil), roots...)
	for _, root := range roots {
		_ = w.addDirRecursive(root)
		w.pending.markDir(root)
	}
	pending := w.pending
	go w.loop(runCtx, watcher, pending, append([]string(nil), roots...))
	go w.svc.processPending(runCtx, pending, 3*time.Second)
	return nil
}

func (w *watchService) reload(ctx context.Context) error {
	if w == nil {
		return nil
	}
	opened := w.svc != nil && w.svc.opened
	if err := w.close(ctx); err != nil {
		return err
	}
	if !opened {
		return nil
	}
	return w.start(ctx)
}

func (w *watchService) close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if w.cancel != nil {
		w.cancel()
	}
	if w.pending != nil {
		w.pending.close()
		w.pending = nil
	}
	if w.watcher != nil {
		err := w.watcher.Close()
		w.watcher = nil
		return err
	}
	return nil
}

func (w *watchService) loop(ctx context.Context, watcher *fsnotify.Watcher, pending *pendingQueue, roots []string) {
	if watcher == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}
			w.handleEvent(ev, pending, roots)
		case _, ok := <-watcher.Errors:
			if !ok {
				return
			}
			for _, root := range roots {
				if pending != nil {
					pending.markDir(root)
				}
			}
		}
	}
}

func (w *watchService) handleEvent(ev fsnotify.Event, pending *pendingQueue, roots []string) {
	if w == nil || pending == nil || w.svc == nil {
		return
	}
	name := filepath.Clean(ev.Name)
	if strings.TrimSpace(name) == "" {
		return
	}
	if ev.Op&fsnotify.Create == fsnotify.Create {
		if st, err := os.Stat(name); err == nil && st.IsDir() {
			_ = w.addDirRecursive(name)
			pending.markDir(name)
		}
	}
	root := matchWorkspaceRoot(name, roots)
	if root == "" {
		return
	}
	if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		pending.markDir(root)
		return
	}
	pending.markFile(name)
	pending.markDir(root)
}

func matchWorkspaceRoot(path string, roots []string) string {
	path = filepath.Clean(path)
	for _, root := range roots {
		root = filepath.Clean(root)
		if root == "" {
			continue
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			continue
		}
		if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		return root
	}
	return ""
}

func (w *watchService) addDirRecursive(root string) error {
	if w == nil || w.watcher == nil {
		return nil
	}
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "" {
		return nil
	}
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		return w.watcher.Add(path)
	})
}
