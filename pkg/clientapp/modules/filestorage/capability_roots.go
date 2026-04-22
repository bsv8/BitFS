package filestorage

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen"
)

// 模块自己的 ent 根只放在这里。
// 设计说明：
// - root 只给能力，不直接碰模块 gen；
// - 模块内部自己把能力包成 ent client，CRUD 仍然走 ent，不走裸 SQL；
// - 读壳只给 Query 能力，写壳只给 Create/Update/Delete 能力。

type DBReadRoot interface {
	BizWorkspacesQuery() *gen.BizWorkspacesQuery
	BizWorkspaceFilesQuery() *gen.BizWorkspaceFilesQuery
	ProcFileDownloadsQuery() *gen.ProcFileDownloadsQuery
	ProcFileDownloadChunksQuery() *gen.ProcFileDownloadChunksQuery
}

type DBWriteRoot interface {
	BizWorkspacesClient() *gen.BizWorkspacesClient
	BizWorkspaceFilesClient() *gen.BizWorkspaceFilesClient
	ProcFileDownloadsClient() *gen.ProcFileDownloadsClient
	ProcFileDownloadChunksClient() *gen.ProcFileDownloadChunksClient
}

type readExecQuerier struct {
	conn moduleapi.ReadConn
}

func (c readExecQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, fmt.Errorf("read connection does not support exec")
}

func (c readExecQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

func newDBReadClient(conn moduleapi.ReadConn) *gen.Client {
	if conn == nil {
		return nil
	}
	return gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readExecQuerier{conn: conn}})))
}

func newDBWriteClient(tx moduleapi.WriteTx) *gen.Client {
	if tx == nil {
		return nil
	}
	return gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
}

type dbReadRoot struct {
	client *gen.Client
}

type dbWriteRoot struct {
	client *gen.Client
}

func NewDBReadRoot(conn moduleapi.ReadConn) DBReadRoot {
	client := newDBReadClient(conn)
	if client == nil {
		return nil
	}
	return &dbReadRoot{client: client}
}

func NewDBWriteRoot(tx moduleapi.WriteTx) DBWriteRoot {
	client := newDBWriteClient(tx)
	if client == nil {
		return nil
	}
	return &dbWriteRoot{client: client}
}

func (r *dbReadRoot) BizWorkspacesQuery() *gen.BizWorkspacesQuery {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.BizWorkspaces.Query()
}

func (r *dbReadRoot) BizWorkspaceFilesQuery() *gen.BizWorkspaceFilesQuery {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.BizWorkspaceFiles.Query()
}

func (r *dbReadRoot) ProcFileDownloadsQuery() *gen.ProcFileDownloadsQuery {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.ProcFileDownloads.Query()
}

func (r *dbReadRoot) ProcFileDownloadChunksQuery() *gen.ProcFileDownloadChunksQuery {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.ProcFileDownloadChunks.Query()
}

func (r *dbWriteRoot) BizWorkspacesClient() *gen.BizWorkspacesClient {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.BizWorkspaces
}

func (r *dbWriteRoot) BizWorkspaceFilesClient() *gen.BizWorkspaceFilesClient {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.BizWorkspaceFiles
}

func (r *dbWriteRoot) ProcFileDownloadsClient() *gen.ProcFileDownloadsClient {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.ProcFileDownloads
}

func (r *dbWriteRoot) ProcFileDownloadChunksClient() *gen.ProcFileDownloadChunksClient {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.ProcFileDownloadChunks
}
