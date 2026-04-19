package managedclient

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
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

func (s *systemHomepageState) EnsureSeedPrices(db *sql.DB, resaleDiscountBPS uint64) error {
	if s == nil || db == nil {
		return nil
	}
	for _, seedHash := range s.SeedHashes {
		var seedPath string
		if err := db.QueryRow(`SELECT seed_file_path FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&seedPath); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("system homepage seed not found after workspace sync: %s", seedHash)
			}
			return err
		}
		var existingFloor sql.NullInt64
		if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k FROM biz_seed_pricing_policy WHERE seed_hash=?`, seedHash).Scan(&existingFloor); err != nil && err != sql.ErrNoRows {
			return err
		}
		// 设计说明：
		// - 系统首页需要一个极低的默认售价，便于客户端开箱即用地分享首页资源；
		// - 但如果用户后来已经手动设置过价格，这里不再覆盖，避免每次启动偷偷改回去。
		if existingFloor.Valid && existingFloor.Int64 > 0 {
			continue
		}
		if _, _, err := upsertSystemHomepageSeedPricingPolicy(db, seedHash, systemHomepageFloorPriceSatPer64K, resaleDiscountBPS, seedPath); err != nil {
			return err
		}
	}
	return nil
}

func (s *systemHomepageState) ApplySeedMetadata(db *sql.DB) error {
	if s == nil || db == nil {
		return nil
	}
	for _, seedHash := range s.SeedHashes {
		meta := s.FileMetaBySeed[seedHash]
		normalizedSeedHash := normalizeSeedHashHex(seedHash)
		if _, err := db.Exec(
			`UPDATE biz_seeds
			    SET recommended_file_name=CASE
			          WHEN TRIM(COALESCE(recommended_file_name,''))='' THEN ?
			          WHEN LOWER(TRIM(COALESCE(recommended_file_name,'')))=LOWER(?) THEN ?
			          ELSE recommended_file_name
			        END,
			        mime_hint=CASE
			          WHEN TRIM(COALESCE(mime_hint,''))='' THEN ?
			          WHEN LOWER(TRIM(COALESCE(mime_hint,'')))='application/octet-stream' THEN ?
			          ELSE mime_hint
			        END
			  WHERE seed_hash=?`,
			meta.OriginalName,
			normalizedSeedHash,
			meta.OriginalName,
			meta.MIME,
			meta.MIME,
			seedHash,
		); err != nil {
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
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return name
}

func sanitizeHomepageMIMEHint(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "/") {
		return ""
	}
	return value
}

func upsertSystemHomepageSeedPricingPolicy(db *sql.DB, seedHash string, floorUnit, discountBPS uint64, seedPath string) (uint64, uint64, error) {
	if db == nil {
		return 0, 0, fmt.Errorf("db is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return 0, 0, fmt.Errorf("invalid seed hash")
	}
	var chunkCount uint64
	if err := db.QueryRow(`SELECT chunk_count FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&chunkCount); err != nil {
		return 0, 0, err
	}
	if chunkCount == 0 {
		if strings.TrimSpace(seedPath) == "" {
			return 0, 0, fmt.Errorf("seed path is empty")
		}
		stat, err := os.Stat(seedPath)
		if err != nil {
			return 0, 0, err
		}
		size := uint64(stat.Size())
		if size == 0 {
			chunkCount = 1
		} else {
			chunkCount = (size + seedBlockSizeBytes - 1) / seedBlockSizeBytes
		}
	}
	unitPrice := floorUnit
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO biz_seed_pricing_policy(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix)
		 VALUES(?,?,?,?,?)
		 ON CONFLICT(seed_hash) DO UPDATE SET
		   floor_unit_price_sat_per_64k=excluded.floor_unit_price_sat_per_64k,
		   resale_discount_bps=excluded.resale_discount_bps,
		   pricing_source=excluded.pricing_source,
		   updated_at_unix=excluded.updated_at_unix
		 WHERE COALESCE(biz_seed_pricing_policy.pricing_source,'')!='user'`,
		seedHash, floorUnit, discountBPS, "system", now,
	); err != nil {
		return 0, 0, err
	}
	return unitPrice, chunkCount, nil
}
