package filestorage

import "context"

func (s *service) ensureWatcher() {
	if s == nil || s.watch != nil {
		return
	}
	s.watch = newWatchService(s)
}

func (s *service) startWatcher(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.ensureWatcher()
	return s.open(ctx)
}
