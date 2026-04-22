package clientapp

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

// downloadFileWorkspacePartPath 只负责把下载 part 文件放到稳定位置。
// 设计说明：
// - 路径必须稳定，重复调用不能改变位置；
// - part 文件和正式 workspace 文件分开，避免临时数据混进工作区扫描结果；
// - 这里只管路径和目录，不负责清空或写入。
func downloadFileWorkspacePartPath(ctx context.Context, store *clientDB, seedHash string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return "", fmt.Errorf("seed_hash is required")
	}
	rootDir, err := downloadFileWorkspaceDataRoot(ctx, store, seedHash)
	if err != nil {
		return "", err
	}
	if rootDir == "" {
		return "", fmt.Errorf("download root is not available")
	}
	return filepath.Join(rootDir, "downloads", seedHash+".part"), nil
}

// downloadFileWorkspaceMarkChunkStored 只把 chunk bytes 写回 part 文件对应位置。
// 设计说明：
// - chunk 布局只能来自 seed 元数据，不能靠固定大小猜；
// - 普通块用 file_size / chunk_count 推导，最后一块吃剩余长度；
// - 不做 append，不伪造 chunk 完成状态。
func downloadFileWorkspaceMarkChunkStored(ctx context.Context, store *clientDB, seedHash string, chunkIndex uint32, chunkBytes []byte) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	partPath, err := downloadFileWorkspacePartPath(ctx, store, seedHash)
	if err != nil {
		return err
	}
	meta, found, err := dbLoadSellerSeedSnapshot(ctx, store, seedHash)
	if err != nil {
		return err
	}
	if !found || meta.ChunkCount == 0 || meta.FileSize == 0 {
		return fmt.Errorf("chunk layout is required")
	}
	offset, expectedSize, ok := chunkSpanFromLayout(meta.FileSize, meta.ChunkCount, chunkIndex)
	if !ok {
		return fmt.Errorf("chunk layout is required")
	}
	if uint64(len(chunkBytes)) != expectedSize {
		return fmt.Errorf("chunk size does not match layout")
	}
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteAt(chunkBytes, offset); err != nil {
		return err
	}
	return f.Sync()
}

// downloadFileWorkspaceComplete 把 part 文件登记为正式 workspace 文件。
// 设计说明：
// - 已经登记过的完整文件直接幂等返回，不重复覆盖；
// - 不允许覆盖来源不明的现有文件；
// - 登记失败时尽量回收刚写出的正式文件，避免留下脏文件。
func downloadFileWorkspaceComplete(ctx context.Context, store *clientDB, input filedownload.CompleteFileInput) (filedownload.LocalFile, error) {
	var out filedownload.LocalFile
	if store == nil {
		return out, fmt.Errorf("client db is nil")
	}
	seedHash := normalizeSeedHashHex(input.SeedHash)
	if seedHash == "" {
		return out, fmt.Errorf("seed_hash is required")
	}
	partPath := strings.TrimSpace(input.PartFilePath)
	if partPath == "" {
		return out, fmt.Errorf("part_file_path is required")
	}
	st, err := os.Stat(partPath)
	if err != nil {
		if os.IsNotExist(err) {
			return out, fmt.Errorf("part file does not exist")
		}
		return out, err
	}
	if !st.Mode().IsRegular() {
		return out, fmt.Errorf("part file is not regular")
	}
	if localPath, size, found := findCompleteLocalFileBySeedHash(ctx, store, seedHash); found {
		return filedownload.LocalFile{
			FilePath: localPath,
			FileSize: size,
			SeedHash: seedHash,
			MimeType: sanitizeMIMEHint(input.MimeType),
		}, nil
	}
	meta, found, err := dbLoadSellerSeedSnapshot(ctx, store, seedHash)
	if err != nil {
		return out, err
	}
	if !found || meta.ChunkCount == 0 || meta.FileSize == 0 {
		return out, fmt.Errorf("chunk layout is required")
	}
	if uint64(st.Size()) != meta.FileSize {
		return out, fmt.Errorf("part file is incomplete")
	}
	recommendedName := sanitizeRecommendedFileName(input.RecommendedFileName)
	if recommendedName == "" {
		recommendedName = seedHash
	}
	mimeHint := sanitizeMIMEHint(input.MimeType)
	outPath, err := downloadFileWorkspaceSelectOutputPath(ctx, store, recommendedName, meta.FileSize)
	if err != nil {
		return out, err
	}
	if st, err := os.Stat(outPath); err == nil {
		if st.Mode().IsRegular() {
			return out, fmt.Errorf("output path already exists")
		}
		return out, fmt.Errorf("output path already exists")
	} else if !os.IsNotExist(err) {
		return out, err
	}
	if err := copyFileExclusive(partPath, outPath); err != nil {
		return out, err
	}
	copied := true
	defer func() {
		if copied {
			_ = os.Remove(outPath)
		}
	}()
	if err := dbRegisterWorkspaceFileOnly(ctx, store, outPath, seedHash, true); err != nil {
		return out, err
	}
	copied = false
	return filedownload.LocalFile{
		FilePath: outPath,
		FileSize: meta.FileSize,
		SeedHash: seedHash,
		MimeType: mimeHint,
	}, nil
}

// copyFileExclusive 只在目标不存在时复制文件，避免覆盖来源不明的现有文件。
func copyFileExclusive(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()
	if _, err := io.Copy(out, in); err != nil {
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}

// dbRegisterWorkspaceFileOnly 只登记 workspace 文件，不改 seed 元数据。
// 设计说明：
// - 这里是完成文件的最后一步，只接受已经落到 workspace 的绝对路径；
// - 文件是否属于 workspace 由 DB 已注册的 workspace roots 决定；
// - seed 元数据是另一条事实线，不能在这里顺手改写。
func dbRegisterWorkspaceFileOnly(ctx context.Context, store *clientDB, absPath string, seedHash string, seedLocked bool) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return fmt.Errorf("output path is required")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return err
	}
	resolved, ok := resolveWorkspaceRelativePath(absPath, roots)
	if !ok {
		return fmt.Errorf("output path is outside registered biz_workspaces")
	}
	lockedValue := int64(0)
	if seedLocked {
		lockedValue = 1
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertWorkspaceFileTx(ctx, tx, resolved.WorkspacePath, resolved.FilePath, seedHash, lockedValue)
	})
}

func downloadFileWorkspaceDataRoot(ctx context.Context, store *clientDB, seedHash string) (string, error) {
	dbFile, err := clientDBDatabasePath(ctx, store)
	if err == nil {
		dbFile = strings.TrimSpace(dbFile)
		if dbFile != "" {
			return filepath.Clean(filepath.Dir(dbFile)), nil
		}
	}
	seedPath, err := dbGetSeedFilePathByHash(ctx, store, seedHash)
	if err != nil {
		return "", err
	}
	seedPath = strings.TrimSpace(seedPath)
	if seedPath == "" {
		return "", nil
	}
	return filepath.Clean(filepath.Dir(filepath.Dir(seedPath))), nil
}

func clientDBDatabasePath(ctx context.Context, store *clientDB) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	var out string
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		var seq int
		var name string
		var file string
		if err := rc.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &name, &file); err != nil {
			return err
		}
		out = strings.TrimSpace(file)
		return nil
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func downloadFileWorkspaceSelectOutputPath(ctx context.Context, store *clientDB, fileName string, fileSize uint64) (string, error) {
	items, err := dbListWorkspaces(ctx, store)
	if err != nil {
		return "", err
	}
	name := sanitizeRecommendedFileName(fileName)
	if name == "" {
		return "", fmt.Errorf("invalid output file name")
	}
	for _, it := range items {
		if !it.Enabled {
			continue
		}
		free, ferr := freeBytesUnderPath(it.WorkspacePath)
		if ferr != nil {
			continue
		}
		if it.MaxBytes > 0 {
			used, uerr := dbWorkspaceUsedBytes(ctx, store, it.WorkspacePath)
			if uerr != nil {
				continue
			}
			if used+fileSize > it.MaxBytes {
				continue
			}
		}
		if free < fileSize {
			continue
		}
		outPath := filepath.Join(it.WorkspacePath, name)
		if st, err := os.Stat(outPath); err == nil && st.Mode().IsRegular() {
			continue
		} else if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return "", err
		}
		return outPath, nil
	}
	return "", fmt.Errorf("no workspace has enough capacity")
}

func chunkSpanFromLayout(fileSize uint64, chunkCount uint32, chunkIndex uint32) (int64, uint64, bool) {
	if chunkCount == 0 || chunkIndex >= chunkCount {
		return 0, 0, false
	}
	baseSize := fileSize / uint64(chunkCount)
	if chunkIndex == chunkCount-1 {
		offset := baseSize * uint64(chunkCount-1)
		return int64(offset), fileSize - offset, true
	}
	offset := baseSize * uint64(chunkIndex)
	return int64(offset), baseSize, true
}
