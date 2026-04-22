package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage"
)

func (d *clientDB) FindLatestWorkspaceFileBySeedHash(ctx context.Context, seedHash string) (string, uint64, error) {
	if d == nil {
		return "", 0, fmt.Errorf("client db is nil")
	}
	var outPath string
	var outSize uint64
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		if root == nil {
			return fmt.Errorf("filestorage read root is nil")
		}
		path, size, err := filestorage.FindLatestWorkspaceFileBySeedHash(ctx, root, seedHash)
		if err != nil {
			return err
		}
		outPath = path
		outSize = size
		return nil
	})
	return outPath, outSize, err
}

func (d *clientDB) CollectActiveSeedHashes(ctx context.Context) (map[string]struct{}, error) {
	if d == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	active := map[string]struct{}{}
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		if root == nil {
			return fmt.Errorf("filestorage read root is nil")
		}
		items, err := filestorage.CollectActiveSeedHashes(ctx, root)
		if err != nil {
			return err
		}
		active = items
		return nil
	})
	return active, err
}

func (d *clientDB) GetFileDownloadState(ctx context.Context, seedHash string, includeUpdated bool) (fileDownloadStateRow, error) {
	if d == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	var out fileDownloadStateRow
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		if root == nil {
			return fmt.Errorf("filestorage read root is nil")
		}
		state, err := filestorage.GetFileDownloadState(ctx, root, seedHash, includeUpdated)
		if err != nil {
			return err
		}
		out.FilePath = state.FilePath
		out.FileSize = state.FileSize
		out.ChunkCount = state.ChunkCount
		out.Completed = state.Completed
		out.PaidSats = state.PaidSats
		out.Status = state.Status
		out.DemandID = state.DemandID
		out.LastError = state.LastError
		out.StatusJSON = state.StatusJSON
		out.UpdatedAtUnix = state.UpdatedAtUnix
		return nil
	})
	return out, err
}

func (d *clientDB) EnsureFileDownloadQueued(ctx context.Context, seedHash string, partPath string, statusJSON string) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	return d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		if root == nil {
			return fmt.Errorf("filestorage write root is nil")
		}
		return filestorage.EnsureFileDownloadQueued(ctx, root, seedHash, partPath, statusJSON)
	})
}

func (d *clientDB) ListFileDownloadChunks(ctx context.Context, seedHash string) ([]fileDownloadChunkRow, error) {
	if d == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	var out []fileDownloadChunkRow
	err := d.Read(ctx, func(conn moduleapi.ReadConn) error {
		root := filestorage.NewDBReadRoot(conn)
		if root == nil {
			return fmt.Errorf("filestorage read root is nil")
		}
		rows, err := filestorage.ListFileDownloadChunks(ctx, root, seedHash)
		if err != nil {
			return err
		}
		out = make([]fileDownloadChunkRow, 0, len(rows))
		for _, row := range rows {
			out = append(out, fileDownloadChunkRow{
				ChunkIndex: uint32(row.ChunkIndex),
				Status:     row.Status,
			})
		}
		return nil
	})
	return out, err
}

func (d *clientDB) UpsertFileDownloadState(ctx context.Context, seedHash string, row fileDownloadStateRow) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	return d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		if root == nil {
			return fmt.Errorf("filestorage write root is nil")
		}
		return filestorage.UpsertFileDownloadState(ctx, root, seedHash, filestorage.FileDownloadState{
			FilePath:      row.FilePath,
			FileSize:      row.FileSize,
			ChunkCount:    row.ChunkCount,
			Completed:     row.Completed,
			PaidSats:      row.PaidSats,
			Status:        row.Status,
			DemandID:      row.DemandID,
			LastError:     row.LastError,
			StatusJSON:    row.StatusJSON,
			UpdatedAtUnix: row.UpdatedAtUnix,
		})
	})
}

func (d *clientDB) UpsertFileDownloadChunkDone(ctx context.Context, seedHash string, idx uint32, sellerPeerID string, price uint64) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	return d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		root := filestorage.NewDBWriteRoot(tx)
		if root == nil {
			return fmt.Errorf("filestorage write root is nil")
		}
		return filestorage.UpsertFileDownloadChunkDone(ctx, root, seedHash, idx, sellerPeerID, price)
	})
}

func (d *clientDB) DeleteLiveStreamWorkspaceRows(ctx context.Context, streamID string) (int64, error) {
	if d == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(streamID) {
		return 0, fmt.Errorf("invalid stream_id")
	}
	var before int64
	err := d.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		rows := filestorage.NewDBWriteRoot(tx)
		if rows == nil {
			return fmt.Errorf("filestorage write root is nil")
		}
		count, err := filestorage.DeleteLiveStreamWorkspaceRows(ctx, rows, streamID)
		if err != nil {
			return err
		}
		before = count
		return nil
	})
	return before, err
}
