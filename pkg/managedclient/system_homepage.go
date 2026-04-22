package managedclient

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

const systemHomepageFloorPriceSatPer64K uint64 = 1
const seedBlockSizeBytes uint64 = 64 * 1024

type systemHomepageManifest struct {
	RootEntryHash string                     `json:"root_entry_hash"`
	Files         map[string]json.RawMessage `json:"files"`
}

type systemHomepageFileMeta struct {
	OriginalName string `json:"original_name"`
	MIME         string `json:"mime"`
}

type systemHomepageState struct {
	BundleDir       string
	WorkspaceDir    string
	DefaultSeedHash string
	SeedHashes      []string
	FileMetaBySeed  map[string]systemHomepageFileMeta
}

func loadSystemHomepageState(bundleDir string, workspaceDir string) (*systemHomepageState, error) {
	bundleDir = strings.TrimSpace(bundleDir)
	// 设计说明：
	// - 空值表示“本次不启用系统首页 bundle”；
	// - 不能先 filepath.Clean("")，否则会变成 "." 并误读当前目录 manifest。
	if bundleDir == "" {
		return nil, nil
	}
	bundleDir = filepath.Clean(bundleDir)
	manifestPath := filepath.Join(bundleDir, "manifest.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read system homepage manifest failed: %w", err)
	}
	var manifest systemHomepageManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("parse system homepage manifest failed: %w", err)
	}
	rootSeedHash := normalizeSeedHashHex(manifest.RootEntryHash)
	if rootSeedHash == "" {
		return nil, fmt.Errorf("invalid system homepage root_entry_hash")
	}
	seedHashes := make([]string, 0, len(manifest.Files)+1)
	seen := map[string]struct{}{}
	appendSeed := func(seedHash string) {
		if seedHash == "" {
			return
		}
		if _, ok := seen[seedHash]; ok {
			return
		}
		seen[seedHash] = struct{}{}
		seedHashes = append(seedHashes, seedHash)
	}
	appendSeed(rootSeedHash)
	for seedHash := range manifest.Files {
		appendSeed(normalizeSeedHashHex(seedHash))
	}
	if len(seedHashes) == 0 {
		return nil, fmt.Errorf("system homepage bundle has no seed files")
	}
	fileMetaBySeed := make(map[string]systemHomepageFileMeta, len(manifest.Files))
	for rawSeedHash, rawMeta := range manifest.Files {
		seedHash := normalizeSeedHashHex(rawSeedHash)
		if seedHash == "" {
			continue
		}
		var meta systemHomepageFileMeta
		if err := json.Unmarshal(rawMeta, &meta); err != nil {
			return nil, fmt.Errorf("parse system homepage file meta failed: %w", err)
		}
		fileMetaBySeed[seedHash] = systemHomepageFileMeta{
			OriginalName: sanitizeHomepageFileName(meta.OriginalName),
			MIME:         sanitizeHomepageMIMEHint(meta.MIME),
		}
	}
	state := &systemHomepageState{
		BundleDir:       bundleDir,
		DefaultSeedHash: rootSeedHash,
		SeedHashes:      seedHashes,
		FileMetaBySeed:  fileMetaBySeed,
	}
	state.BindWorkspace(workspaceDir)
	return state, nil
}

func (s *systemHomepageState) BindWorkspace(workspaceDir string) {
	if s == nil {
		return
	}
	root := filepath.Clean(strings.TrimSpace(workspaceDir))
	if root == "" || root == "." {
		s.WorkspaceDir = filepath.Join(".bitfs-system", "homepage")
		return
	}
	s.WorkspaceDir = filepath.Join(root, ".bitfs-system", "homepage")
}

func (s *systemHomepageState) InstallIntoWorkspace() error {
	if s == nil {
		return nil
	}
	if err := os.MkdirAll(s.WorkspaceDir, 0o755); err != nil {
		return err
	}
	expected := map[string]struct{}{}
	for _, seedHash := range s.SeedHashes {
		expected[seedHash] = struct{}{}
		src := filepath.Join(s.BundleDir, seedHash)
		dest := filepath.Join(s.WorkspaceDir, seedHash)
		if _, err := os.Stat(src); err != nil {
			return fmt.Errorf("system homepage seed file missing: %w", err)
		}
		if err := copyFile(src, dest); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(s.WorkspaceDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if normalizeSeedHashHex(name) == "" {
			continue
		}
		if _, ok := expected[name]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(s.WorkspaceDir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (s *systemHomepageState) EnsureSeedPrices(ctx context.Context, store moduleapi.SeedStore, resaleDiscountBPS uint64) error {
	if s == nil || store == nil {
		return nil
	}
	for _, seedHash := range s.SeedHashes {
		seed, ok, err := store.LoadSeedSnapshot(ctx, seedHash)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("system homepage seed not found after workspace sync: %s", seedHash)
		}
		if seed.FloorPriceSatPer64K > 0 {
			continue
		}
		if err := store.UpsertSeedPricingPolicy(ctx, seed.SeedHash, systemHomepageFloorPriceSatPer64K, resaleDiscountBPS, "system", time.Now().Unix()); err != nil {
			return err
		}
	}
	return nil
}

func (s *systemHomepageState) ApplySeedMetadata(ctx context.Context, store moduleapi.SeedStore) error {
	if s == nil || store == nil {
		return nil
	}
	for _, seedHash := range s.SeedHashes {
		meta := s.FileMetaBySeed[seedHash]
		snap, ok, err := store.LoadSeedSnapshot(ctx, seedHash)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		updated := snap
		if strings.TrimSpace(updated.RecommendedFileName) == "" || strings.EqualFold(strings.TrimSpace(updated.RecommendedFileName), normalizeSeedHashHex(seedHash)) {
			updated.RecommendedFileName = meta.OriginalName
		}
		if strings.TrimSpace(updated.MimeHint) == "" || strings.EqualFold(strings.TrimSpace(updated.MimeHint), "application/octet-stream") {
			updated.MimeHint = meta.MIME
		}
		if err := store.UpsertSeedRecord(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

func (s *systemHomepageState) HasBundle() bool {
	return s != nil && s.DefaultSeedHash != ""
}

func copyFile(src, dest string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dest, raw, 0o644)
}

func logSystemHomepageInstalled(state *systemHomepageState) {
	if state == nil {
		return
	}
	obs.Important(obs.ServiceBitFSClient, "system_homepage_installed", map[string]any{
		"bundle_dir":          state.BundleDir,
		"workspace_dir":       state.WorkspaceDir,
		"default_seed_hash":   state.DefaultSeedHash,
		"seed_count":          len(state.SeedHashes),
		"default_floor_price": systemHomepageFloorPriceSatPer64K,
	})
}

func normalizeSeedHashHex(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if len(value) != 64 {
		return ""
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return ""
		}
	}
	return value
}

func sanitizeHomepageFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, "\x00", "")
	return name
}

func sanitizeHomepageMIMEHint(raw string) string {
	return strings.TrimSpace(raw)
}
