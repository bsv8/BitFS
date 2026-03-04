package clientapp

import (
	"context"
	"database/sql"
	"sync"
)

type workspaceManager struct {
	cfg     *Config
	db      *sql.DB
	catalog *sellerCatalog
	mu      sync.Mutex
}

func (m *workspaceManager) SyncOnce(ctx context.Context) (map[string]sellerSeed, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seeds, err := scanAndSyncWorkspace(ctx, m.cfg, m.db)
	if err != nil {
		return nil, err
	}
	if m.catalog != nil {
		m.catalog.Replace(seeds)
	}
	return seeds, nil
}
