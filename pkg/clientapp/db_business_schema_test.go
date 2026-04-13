package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

// TestInitIndexDB_CreatesOrdersTable 验证 orders 表存在且结构正确
func TestInitIndexDB_CreatesOrdersTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证表存在
	hasTable, err := hasTable(db, "orders")
	if err != nil {
		t.Fatalf("check orders table failed: %v", err)
	}
	if !hasTable {
		t.Fatal("orders table should exist")
	}

	// 验证列存在
	wantCols := []string{
		"order_id", "order_type", "order_subtype", "owner_pubkey_hex",
		"target_object_type", "target_object_id", "status",
		"idempotency_key", "created_at_unix", "updated_at_unix", "note", "payload_json",
	}
	cols, err := tableColumns(db, "orders")
	if err != nil {
		t.Fatalf("inspect orders columns failed: %v", err)
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("missing column %s on orders", col)
		}
	}

	// 验证索引存在
	wantIndexes := []string{
		"idx_orders_type_status",
		"idx_orders_target",
		"idx_orders_owner",
		"idx_orders_status",
	}
	for _, idx := range wantIndexes {
		hasIndex, err := tableHasIndex(db, "orders", idx)
		if err != nil {
			t.Fatalf("check index %s failed: %v", idx, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on orders", idx)
		}
	}

	// 验证主键约束
	pks := tablePrimaryKeyColumns(t, db, "orders")
	if !containsString(pks, "order_id") {
		t.Fatal("order_id should be PRIMARY KEY")
	}
}

// TestInitIndexDB_CreatesOrderSettlementEventsTable 验证 order_settlement_events 表存在且结构正确
func TestInitIndexDB_CreatesOrderSettlementEventsTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证表存在
	hasTable, err := hasTable(db, "order_settlement_events")
	if err != nil {
		t.Fatalf("check order_settlement_events table failed: %v", err)
	}
	if !hasTable {
		t.Fatal("order_settlement_events table should exist")
	}

	// 验证列存在
	wantCols := []string{
		"id", "process_id", "settlement_id", "source_type", "source_id",
		"accounting_scene", "accounting_subtype", "event_type", "status",
		"idempotency_key", "note", "payload_json", "occurred_at_unix",
	}
	cols, err := tableColumns(db, "order_settlement_events")
	if err != nil {
		t.Fatalf("inspect order_settlement_events columns failed: %v", err)
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("missing column %s on order_settlement_events", col)
		}
	}

	// 验证索引存在
	wantIndexes := []string{
		"idx_order_settlement_events_settlement",
		"idx_order_settlement_events_type",
	}
	for _, idx := range wantIndexes {
		hasIndex, err := tableHasIndex(db, "order_settlement_events", idx)
		if err != nil {
			t.Fatalf("check index %s failed: %v", idx, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on order_settlement_events", idx)
		}
	}

	// 验证主键约束
	pks := tablePrimaryKeyColumns(t, db, "order_settlement_events")
	if !containsString(pks, "id") {
		t.Fatal("id should be PRIMARY KEY")
	}

	// 验证幂等约束：(settlement_id, event_type, idempotency_key)
	hasUnique, err := tableHasUniqueIndexOnColumns(db, "order_settlement_events", []string{"settlement_id", "event_type", "idempotency_key"})
	if err != nil {
		t.Fatalf("check unique constraint failed: %v", err)
	}
	if !hasUnique {
		t.Fatal("order_settlement_events should have unique constraint on (settlement_id, event_type, idempotency_key)")
	}
}

// TestInitIndexDB_CreatesOrderSettlementsTable 验证 order_settlements 表存在且结构正确
func TestInitIndexDB_CreatesOrderSettlementsTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证表存在
	hasTable, err := hasTable(db, "order_settlements")
	if err != nil {
		t.Fatalf("check order_settlements table failed: %v", err)
	}
	if !hasTable {
		t.Fatal("order_settlements table should exist")
	}

	// 验证列存在
	wantCols := []string{
		"settlement_id", "order_id", "settlement_no", "business_role",
		"source_type", "source_id", "accounting_scene", "accounting_subtype",
		"settlement_method", "status", "settlement_status", "amount_satoshi",
		"from_party_id", "to_party_id", "target_type", "target_id",
		"idempotency_key", "note", "error_message", "payload_json", "settlement_payload_json",
		"created_at_unix", "updated_at_unix",
	}
	cols, err := tableColumns(db, "order_settlements")
	if err != nil {
		t.Fatalf("inspect order_settlements columns failed: %v", err)
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("missing column %s on order_settlements", col)
		}
	}

	// 验证 order_id + settlement_no 唯一约束
	hasUnique, err := tableHasUniqueIndexOnColumns(db, "order_settlements", []string{"order_id", "settlement_no"})
	if err != nil {
		t.Fatalf("check unique constraint failed: %v", err)
	}
	if !hasUnique {
		t.Fatal("order_settlements should have unique constraint on (order_id, settlement_no)")
	}

	// 验证索引存在
	wantIndexes := []string{
		"idx_order_settlements_order",
		"idx_order_settlements_status",
		"idx_order_settlements_method",
		"idx_order_settlements_target",
	}
	for _, idx := range wantIndexes {
		hasIndex, err := tableHasIndex(db, "order_settlements", idx)
		if err != nil {
			t.Fatalf("check index %s failed: %v", idx, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on order_settlements", idx)
		}
	}

	// 验证主键约束
	pks := tablePrimaryKeyColumns(t, db, "order_settlements")
	if !containsString(pks, "settlement_id") {
		t.Fatal("settlement_id should be PRIMARY KEY")
	}
	if hasUnique, err := tableHasUniqueIndexOnColumns(db, "order_settlements", []string{"settlement_id"}); err != nil {
		t.Fatalf("check settlement_id unique constraint failed: %v", err)
	} else if !hasUnique {
		t.Fatal("settlement_id should be UNIQUE")
	}
}

// TestGetLatestBusinessByFrontOrderID_EmptyReturnsNoRows 验证空 front_order 不会崩溃。
func TestGetLatestBusinessByFrontOrderID_EmptyReturnsNoRows(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	_, err := GetLatestBusinessByFrontOrderID(context.Background(), &clientDB{db: db}, "fo_empty_1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

// tablePrimaryKeyColumns 获取表的主键列列表
func tablePrimaryKeyColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		t.Fatalf("PRAGMA table_info failed: %v", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("scan table_info failed: %v", err)
		}
		if pk == 1 {
			pks = append(pks, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info failed: %v", err)
	}
	return pks
}

// TestInitIndexDB_OrdersIdempotent 验证 orders 写入幂等
func TestInitIndexDB_OrdersIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 第一次插入
	if _, err := db.Exec(`INSERT INTO orders(
		order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,idempotency_key,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"ord_test_1", "domain", "register", "owner1", "domain", "example.com", "pending", "idem_ord_1", 1700000001, 1700000001, "test", `{}`,
	); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// 第二次更新同一记录
	if _, err := db.Exec(`INSERT INTO orders(
		order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,idempotency_key,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(order_id) DO UPDATE SET status=excluded.status, updated_at_unix=excluded.updated_at_unix`,
		"ord_test_1", "domain", "register", "owner1", "domain", "example.com", "completed", "idem_ord_1", 1700000001, 1700000002, "test", `{}`,
	); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}

	// 验证状态已更新
	var status string
	if err := db.QueryRow(`SELECT status FROM orders WHERE order_id=?`, "ord_test_1").Scan(&status); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected status completed, got %s", status)
	}
}

// TestInitIndexDB_OrderSettlementEventsIdempotent 验证 order_settlement_events 写入幂等
func TestInitIndexDB_OrderSettlementEventsIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO orders(
		order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,idempotency_key,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"ord_test_2", "domain", "register", "owner1", "domain", "example.com", "pending", "idem_ord_2", 1700000001, 1700000001, "test", `{}`,
	); err != nil {
		t.Fatalf("insert orders failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO order_settlement_events(
		process_id,settlement_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,idempotency_key,note,payload_json,occurred_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"proc_test_1", "set_test_1", "settlement_payment_attempt", "77", "wallet_transfer", "open", "state_change", "applied", "idem_evt_1", "test", `{}`, 1700000001,
	); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM order_settlement_events WHERE settlement_id=? AND event_type=? AND idempotency_key=?`, "set_test_1", "state_change", "idem_evt_1").Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

// TestInitIndexDB_OrderSettlementsIdempotent 验证 order_settlements 写入幂等和 order_id 唯一约束
func TestInitIndexDB_OrderSettlementsIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO order_settlements(
		settlement_id,order_id,settlement_no,business_role,source_type,source_id,accounting_scene,accounting_subtype,settlement_method,status,settlement_status,from_party_id,to_party_id,target_type,target_id,idempotency_key,note,payload_json,settlement_payload_json,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"set_test_2", "ord_test_2", 1, "formal", "settlement_payment_attempt", "77", "domain", "register", "chain", "pending", "pending", "client:self", "gateway:peer", "", "", "idem_test_2", "test", "{}", "{}", 1700000001, 1700000001,
	); err != nil {
		t.Fatalf("insert order_settlements failed: %v", err)
	}

	// 第一次更新 settlement 字段
	if err := dbUpsertBusinessSettlement(context.Background(), &clientDB{db: db}, businessSettlementEntry{
		SettlementID:     "set_test_1",
		OrderID:          "ord_test_2",
		SettlementMethod: "chain",
		Status:           "pending",
		TargetType:       "chain_quote_pay",
		TargetID:         "",
		Payload:          map[string]any{"step": 1},
	}); err != nil {
		t.Fatalf("first settlement update failed: %v", err)
	}

	// 再写一条同一 order 的新 settlement，应该追加新行而不是覆盖旧行
	if err := dbUpsertBusinessSettlement(context.Background(), &clientDB{db: db}, businessSettlementEntry{
		SettlementID:     "set_test_3",
		OrderID:          "ord_test_2",
		SettlementMethod: "chain",
		Status:           "processing",
		TargetType:       "chain_quote_pay",
		TargetID:         "",
		Payload:          map[string]any{"step": 3},
	}); err != nil {
		t.Fatalf("second settlement update failed: %v", err)
	}

	latest, err := dbGetBusinessSettlementByBusinessID(context.Background(), &clientDB{db: db}, "ord_test_2")
	if err != nil {
		t.Fatalf("latest settlement lookup failed: %v", err)
	}
	if latest.SettlementID != "set_test_3" {
		t.Fatalf("expected latest settlement set_test_3, got %s", latest.SettlementID)
	}

	// 但更新指定 settlement_id 仍然应该成功
	if err := dbUpdateBusinessSettlementStatus(context.Background(), &clientDB{db: db}, "set_test_1", "completed", ""); err != nil {
		t.Fatalf("update settlement status failed: %v", err)
	}

	// 尝试插入同一 order_id + 同一 settlement_no 仍然应该失败
	_, err = db.Exec(`INSERT INTO order_settlements(
		settlement_id,order_id,settlement_no,business_role,settlement_method,status,settlement_status,target_type,target_id,error_message,created_at_unix,updated_at_unix,idempotency_key,payload_json,from_party_id,to_party_id,source_type,source_id,accounting_scene,accounting_subtype,settlement_payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"set_test_4", "ord_test_2", 1, "formal", "chain", "completed", "completed", "chain_quote_pay", "", "", 1700000001, 1700000001, "idem_set_test_4", `{}`, "client:self", "gateway:peer", "settlement_payment_attempt", "77", "domain", "register", `{}`,
	)
	if err == nil {
		t.Fatal("expected unique constraint violation for duplicate order_id + settlement_no")
	}

	// 验证结算状态已更新
	var status string
	if err := db.QueryRow(`SELECT settlement_status FROM order_settlements WHERE settlement_id=?`, "set_test_1").Scan(&status); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected settlement_status completed, got %s", status)
	}
}
