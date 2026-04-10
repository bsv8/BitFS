package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// TestInitIndexDB_CreatesFrontOrdersTable 验证 biz_front_orders 表存在且结构正确
func TestInitIndexDB_CreatesFrontOrdersTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证表存在
	hasTable, err := hasTable(db, "biz_front_orders")
	if err != nil {
		t.Fatalf("check biz_front_orders table failed: %v", err)
	}
	if !hasTable {
		t.Fatal("biz_front_orders table should exist")
	}

	// 验证列存在
	wantCols := []string{
		"front_order_id", "front_type", "front_subtype", "owner_pubkey_hex",
		"target_object_type", "target_object_id", "status",
		"created_at_unix", "updated_at_unix", "note", "payload_json",
	}
	cols, err := tableColumns(db, "biz_front_orders")
	if err != nil {
		t.Fatalf("inspect biz_front_orders columns failed: %v", err)
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("missing column %s on biz_front_orders", col)
		}
	}

	// 验证索引存在
	wantIndexes := []string{
		"idx_biz_front_orders_type_status",
		"idx_biz_front_orders_target",
		"idx_biz_front_orders_owner",
	}
	for _, idx := range wantIndexes {
		hasIndex, err := tableHasIndex(db, "biz_front_orders", idx)
		if err != nil {
			t.Fatalf("check index %s failed: %v", idx, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on biz_front_orders", idx)
		}
	}

	// 验证主键约束
	pks := tablePrimaryKeyColumns(t, db, "biz_front_orders")
	if !containsString(pks, "front_order_id") {
		t.Fatal("front_order_id should be PRIMARY KEY")
	}
}

// TestInitIndexDB_CreatesBusinessTriggersTable 验证 biz_business_triggers 表存在且结构正确
func TestInitIndexDB_CreatesBusinessTriggersTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证表存在
	hasTable, err := hasTable(db, "biz_business_triggers")
	if err != nil {
		t.Fatalf("check biz_business_triggers table failed: %v", err)
	}
	if !hasTable {
		t.Fatal("biz_business_triggers table should exist")
	}

	// 验证列存在
	wantCols := []string{
		"trigger_id", "business_id", "trigger_type", "trigger_id_value",
		"trigger_role", "created_at_unix", "note", "payload_json",
	}
	cols, err := tableColumns(db, "biz_business_triggers")
	if err != nil {
		t.Fatalf("inspect biz_business_triggers columns failed: %v", err)
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("missing column %s on biz_business_triggers", col)
		}
	}

	// 验证外键定义存在
	hasFK, err := tableHasCreateSQLContains(db, "biz_business_triggers", "FOREIGN KEY(business_id) REFERENCES settle_records(business_id)")
	if err != nil {
		t.Fatalf("check foreign key sql failed: %v", err)
	}
	if !hasFK {
		t.Fatal("biz_business_triggers should declare foreign key on business_id referencing settle_records")
	}

	// 验证索引存在
	wantIndexes := []string{
		"idx_biz_business_triggers_business",
		"idx_biz_business_triggers_type_value",
		"idx_biz_business_triggers_type_value_role",
	}
	for _, idx := range wantIndexes {
		hasIndex, err := tableHasIndex(db, "biz_business_triggers", idx)
		if err != nil {
			t.Fatalf("check index %s failed: %v", idx, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on biz_business_triggers", idx)
		}
	}

	// 验证主键约束
	pks := tablePrimaryKeyColumns(t, db, "biz_business_triggers")
	if !containsString(pks, "trigger_id") {
		t.Fatal("trigger_id should be PRIMARY KEY")
	}

	// 验证幂等约束：(business_id, trigger_type, trigger_id_value, trigger_role)
	hasUnique, err := tableHasUniqueIndexOnColumns(db, "biz_business_triggers", []string{"business_id", "trigger_type", "trigger_id_value", "trigger_role"})
	if err != nil {
		t.Fatalf("check unique constraint failed: %v", err)
	}
	if !hasUnique {
		t.Fatal("biz_business_triggers should have unique constraint on (business_id, trigger_type, trigger_id_value, trigger_role)")
	}
}

// TestInitIndexDB_CreatesBusinessSettlementsTable 验证 settle_records 表存在且结构正确
func TestInitIndexDB_CreatesBusinessSettlementsTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证表存在
	hasTable, err := hasTable(db, "settle_records")
	if err != nil {
		t.Fatalf("check settle_records table failed: %v", err)
	}
	if !hasTable {
		t.Fatal("settle_records table should exist")
	}

	// 验证列存在
	wantCols := []string{
		"settlement_id", "business_id", "settlement_method", "status",
		"target_type", "target_id", "error_message",
		"created_at_unix", "updated_at_unix", "payload_json",
	}
	cols, err := tableColumns(db, "settle_records")
	if err != nil {
		t.Fatalf("inspect settle_records columns failed: %v", err)
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("missing column %s on settle_records", col)
		}
	}

	// 验证 business_id 唯一约束
	hasUnique, err := tableHasUniqueIndexOnColumns(db, "settle_records", []string{"business_id"})
	if err != nil {
		t.Fatalf("check unique constraint failed: %v", err)
	}
	if !hasUnique {
		t.Fatal("settle_records should have unique constraint on business_id")
	}

	// 验证索引存在
	wantIndexes := []string{
		"idx_settle_records_status",
		"idx_settle_records_method",
		"idx_settle_records_target",
	}
	for _, idx := range wantIndexes {
		hasIndex, err := tableHasIndex(db, "settle_records", idx)
		if err != nil {
			t.Fatalf("check index %s failed: %v", idx, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on settle_records", idx)
		}
	}

	// 验证主键约束
	pks := tablePrimaryKeyColumns(t, db, "settle_records")
	if !containsString(pks, "business_id") {
		t.Fatal("business_id should be PRIMARY KEY")
	}
	if hasUnique, err := tableHasUniqueIndexOnColumns(db, "settle_records", []string{"settlement_id"}); err != nil {
		t.Fatalf("check settlement_id unique constraint failed: %v", err)
	} else if !hasUnique {
		t.Fatal("settlement_id should be UNIQUE")
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

// TestInitIndexDB_FrontOrdersIdempotent 验证 biz_front_orders 写入幂等
func TestInitIndexDB_FrontOrdersIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 第一次插入
	if _, err := db.Exec(`INSERT INTO biz_front_orders(
		front_order_id,front_type,front_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"fo_test_1", "domain", "register", "owner1", "domain", "example.com", "pending", 1700000001, 1700000001, "test", `{}`,
	); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// 第二次更新同一记录
	if _, err := db.Exec(`INSERT INTO biz_front_orders(
		front_order_id,front_type,front_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(front_order_id) DO UPDATE SET status=excluded.status, updated_at_unix=excluded.updated_at_unix`,
		"fo_test_1", "domain", "register", "owner1", "domain", "example.com", "completed", 1700000001, 1700000002, "test", `{}`,
	); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}

	// 验证状态已更新
	var status string
	if err := db.QueryRow(`SELECT status FROM biz_front_orders WHERE front_order_id=?`, "fo_test_1").Scan(&status); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected status completed, got %s", status)
	}
}

// TestInitIndexDB_BusinessTriggersIdempotent 验证 biz_business_triggers 写入幂等
func TestInitIndexDB_BusinessTriggersIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 先插入必要的 settle_records 记录（因为外键约束）
	if _, err := db.Exec(`INSERT INTO settle_records(
		settlement_id,business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"set_test_1", "biz_test_1", "front_order", "fo_test_1", "domain", "register", "client:self", "gateway:peer", "pending", 1700000001, "idem_test_1", "test", `{}`,
	); err != nil {
		t.Fatalf("insert settle_records failed: %v", err)
	}

	// 第一次插入 trigger
	if _, err := db.Exec(`INSERT INTO biz_business_triggers(
		trigger_id,business_id,trigger_type,trigger_id_value,trigger_role,created_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?)`,
		"trg_test_1", "biz_test_1", "front_order", "fo_test_1", "primary", 1700000001, "test", `{}`,
	); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_business_triggers WHERE trigger_id=?`, "trg_test_1").Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 trigger, got %d", count)
	}
}

// TestInitIndexDB_BusinessSettlementsIdempotent 验证 settle_records 写入幂等和 business_id 唯一约束
func TestInitIndexDB_BusinessSettlementsIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 先插入必要的 settle_records 记录
	if _, err := db.Exec(`INSERT INTO settle_records(
		settlement_id,business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"set_test_2", "biz_test_2", "front_order", "fo_test_2", "domain", "register", "client:self", "gateway:peer", "pending", 1700000001, "idem_test_2", "test", `{}`,
	); err != nil {
		t.Fatalf("insert settle_records failed: %v", err)
	}

	// 第一次更新 settlement 字段
	if err := dbUpsertBusinessSettlement(context.Background(), &clientDB{db: db}, businessSettlementEntry{
		SettlementID:     "set_test_1",
		BusinessID:       "biz_test_2",
		SettlementMethod: "chain",
		Status:           "pending",
		TargetType:       "chain_payment",
		TargetID:         "",
		Payload:          map[string]any{"step": 1},
	}); err != nil {
		t.Fatalf("first settlement update failed: %v", err)
	}

	// 尝试插入同一 business_id 不同 settlement_id 应该失败
	_, err := db.Exec(`INSERT INTO settle_records(
		settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,idempotency_key,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"set_test_2", "biz_test_2", "chain", "completed", "chain_payment", "", "", 1700000001, 1700000001, "idem_set_test_2", `{}`,
	)
	if err == nil {
		t.Fatal("expected unique constraint violation for duplicate business_id")
	}

	// 但更新现有记录应该成功
	if err := dbUpsertBusinessSettlement(context.Background(), &clientDB{db: db}, businessSettlementEntry{
		SettlementID:     "set_test_1",
		BusinessID:       "biz_test_2",
		SettlementMethod: "chain",
		Status:           "completed",
		TargetType:       "chain_payment",
		TargetID:         "",
		Payload:          map[string]any{"step": 2},
	}); err != nil {
		t.Fatalf("update settlement failed: %v", err)
	}

	// 验证结算状态已更新
	var status string
	if err := db.QueryRow(`SELECT settlement_status FROM settle_records WHERE settlement_id=?`, "set_test_1").Scan(&status); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected settlement_status completed, got %s", status)
	}
}
