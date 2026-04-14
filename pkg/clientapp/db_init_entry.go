package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
)

// ensureClientDBSchema 是客户端数据库结构就绪的唯一入口。
// 设计说明：
// - 这里只负责调度分层，不承载手写 DDL；
// - 统一交给 contract 的 ent schema 产物做建表和建约束。
func ensureClientDBSchema(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if store.db == nil {
		return fmt.Errorf("client db raw db is nil")
	}
	return ensureClientDBSchemaOnDB(ctx, store.db)
}

// ensureClientDBSchemaOnDB 从原始 sql.DB 初始化数据库结构。
// 设计说明：
// - 用于测试场景，测试代码直接持有 *sql.DB 而无 actor；
// - 生产代码应使用 ensureClientDBSchema(store *clientDB)；
// - 这里不再执行老的手写建表和补丁逻辑，只接受 contract 的 schema 真源。
func ensureClientDBSchemaOnDB(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	var seq int
	var name string
	var file string
	if err := db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &name, &file); err != nil {
		return fmt.Errorf("inspect sqlite database path: %w", err)
	}
	targetDB := db
	closeTarget := func() {}
	if strings.TrimSpace(file) != "" {
		schemaDB, err := sql.Open("sqlite", "file:"+file+"?_fk=1")
		if err != nil {
			return fmt.Errorf("open schema db: %w", err)
		}
		if err := applySQLitePragmas(schemaDB); err != nil {
			_ = schemaDB.Close()
			return err
		}
		targetDB = schemaDB
		closeTarget = func() { _ = schemaDB.Close() }
	}
	defer closeTarget()
	client := gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, targetDB)))
	if err := client.Schema.Create(ctx); err != nil {
		return err
	}
	compatIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_order_settlements_order ON order_settlements(order_id)`,
		`CREATE INDEX IF NOT EXISTS idx_order_settlements_status ON order_settlements(settlement_status)`,
		`CREATE INDEX IF NOT EXISTS idx_order_settlements_method ON order_settlements(settlement_method)`,
		`CREATE INDEX IF NOT EXISTS idx_order_settlements_target ON order_settlements(target_type, target_id)`,
		`CREATE INDEX IF NOT EXISTS idx_order_settlement_events_settlement ON order_settlement_events(settlement_id)`,
		`CREATE INDEX IF NOT EXISTS idx_order_settlement_events_type ON order_settlement_events(event_type)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_bsv_utxos_txid_vout ON fact_bsv_utxos(txid, vout)`,
	}
	for _, sqlText := range compatIndexes {
		if _, err := db.ExecContext(ctx, sqlText); err != nil {
			return fmt.Errorf("apply schema compatibility index: %w", err)
		}
	}
	return nil
}
