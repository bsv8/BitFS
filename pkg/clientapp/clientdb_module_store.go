package clientapp

import (
	"context"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage"
	"github.com/bsv8/BitFS/pkg/clientapp/seedstorage"
)

// 设计说明：
// - 这里是最薄的桥，只把 clientDB 交给各模块箱子里的本体函数；
// - 真正的业务读取和写入逻辑留在模块目录，不再堆在 root 包。

func (d *clientDB) LoadSeedSnapshot(ctx context.Context, seedHash string) (moduleapi.SeedRecord, bool, error) {
	type result struct {
		record moduleapi.SeedRecord
		ok     bool
	}
	out, err := readEntValue(ctx, d, func(root EntReadRoot) (result, error) {
		record, ok, err := seedstorage.DBLoadSeedSnapshot(ctx, root, seedHash)
		if err != nil {
			return result{}, err
		}
		return result{record: record, ok: ok}, nil
	})
	if err != nil {
		return moduleapi.SeedRecord{}, false, err
	}
	return out.record, out.ok, nil
}

func (d *clientDB) UpsertSeedRecord(ctx context.Context, record moduleapi.SeedRecord) error {
	_, err := writeEntValue(ctx, d, func(root EntWriteRoot) (struct{}, error) {
		return struct{}{}, seedstorage.DBUpsertSeedRecord(ctx, root, record)
	})
	return err
}

func (d *clientDB) UpsertSeedPricingPolicy(ctx context.Context, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	_, err := writeEntValue(ctx, d, func(root EntWriteRoot) (struct{}, error) {
		return struct{}{}, seedstorage.DBUpsertSeedPricingPolicy(ctx, root, seedHash, floorUnit, discountBPS, source, updatedAtUnix)
	})
	return err
}

func (d *clientDB) ReplaceSeedChunkSupply(ctx context.Context, seedHash string, indexes []uint32) error {
	_, err := writeEntValue(ctx, d, func(root EntWriteRoot) (struct{}, error) {
		return struct{}{}, seedstorage.DBReplaceSeedChunkSupply(ctx, root, seedHash, indexes)
	})
	return err
}

func (d *clientDB) DeleteSeedRecords(ctx context.Context, seedHashes []string) error {
	_, err := writeEntValue(ctx, d, func(root EntWriteRoot) (struct{}, error) {
		return struct{}{}, seedstorage.DBDeleteSeedRecords(ctx, root, seedHashes)
	})
	return err
}

func (d *clientDB) CleanupOrphanSeeds(ctx context.Context) error {
	return cleanupOrphanSeeds(ctx, d)
}

func (d *clientDB) ListWorkspaces(ctx context.Context) ([]moduleapi.WorkspaceItem, error) {
	var out []moduleapi.WorkspaceItem
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		rows, err := filestorage.DBListWorkspaces(ctx, root)
		if err != nil {
			return err
		}
		out = rows
		return nil
	})
	return out, err
}

func (d *clientDB) UpsertWorkspace(ctx context.Context, workspacePath string, maxBytes uint64, enabled bool) (moduleapi.WorkspaceItem, error) {
	var out moduleapi.WorkspaceItem
	err := d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		item, err := filestorage.DBUpsertWorkspace(ctx, root, workspacePath, maxBytes, enabled)
		if err != nil {
			return err
		}
		out = item
		return nil
	})
	return out, err
}

func (d *clientDB) DeleteWorkspace(ctx context.Context, workspacePath string) error {
	return d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		return filestorage.DBDeleteWorkspace(ctx, root, workspacePath)
	})
}

func (d *clientDB) UpdateWorkspace(ctx context.Context, workspacePath string, maxBytes *uint64, enabled *bool) (moduleapi.WorkspaceItem, error) {
	var out moduleapi.WorkspaceItem
	err := d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		item, err := filestorage.DBUpdateWorkspace(ctx, root, workspacePath, maxBytes, enabled)
		if err != nil {
			return err
		}
		out = item
		return nil
	})
	return out, err
}

func (d *clientDB) ListWorkspaceFiles(ctx context.Context, limit, offset int, pathLike string) (moduleapi.WorkspaceFilesPage, error) {
	var page moduleapi.WorkspaceFilesPage
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		items, total, err := filestorage.DBListWorkspaceFiles(ctx, root, limit, offset, pathLike)
		if err != nil {
			return err
		}
		page = moduleapi.WorkspaceFilesPage{Total: total, Items: items}
		return nil
	})
	return page, err
}

func (d *clientDB) UpsertWorkspaceFile(ctx context.Context, workspacePath, filePath, seedHash string, locked bool) error {
	return d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		return filestorage.DBUpsertWorkspaceFile(ctx, root, workspacePath, filePath, seedHash, locked)
	})
}

func (d *clientDB) DeleteWorkspaceFile(ctx context.Context, workspacePath, filePath string) error {
	return d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		return filestorage.DBDeleteWorkspaceFile(ctx, root, workspacePath, filePath)
	})
}

func (d *clientDB) ListWorkspaceRoots(ctx context.Context) ([]string, error) {
	var out []string
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		rows, err := filestorage.DBListWorkspaceRoots(ctx, root)
		if err != nil {
			return err
		}
		out = rows
		return nil
	})
	return out, err
}

func (d *clientDB) GetWorkspaceFileSeedHashByAbsPath(ctx context.Context, absPath string) (string, error) {
	var out string
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		seedHash, err := filestorage.DBGetWorkspaceFileSeedHashByAbsPath(ctx, root, absPath)
		if err != nil {
			return err
		}
		out = seedHash
		return nil
	})
	return out, err
}
