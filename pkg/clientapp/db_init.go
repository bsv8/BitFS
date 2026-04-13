package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ensureClientDBBaseSchemaCtx 只保留给测试和旧入口的薄壳。
// 设计说明：
// - 真正的建表口径已经收口到 BitFS-contract 的 ent schema；
// - 这里不再写任何手工 DDL，只转交给统一 schema 入口。
func ensureClientDBBaseSchemaCtx(ctx context.Context, db *sql.DB) error {
	return ensureClientDBSchemaOnDB(ctx, db)
}

// tableHasForeignKeyCtx 检查表是否已经有指定外键。
func tableHasForeignKeyCtx(ctx context.Context, db *sql.DB, table, fromColumn, parentTable, parentColumn string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	rows, err := QueryContext(ctx, db, fmt.Sprintf("PRAGMA foreign_key_list(%s)", strings.TrimSpace(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var seq int
		var fkTable string
		var fkFrom string
		var fkTo string
		var onUpdate string
		var onDelete string
		var match string
		if err := rows.Scan(&id, &seq, &fkTable, &fkFrom, &fkTo, &onUpdate, &onDelete, &match); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(fkFrom), strings.TrimSpace(fromColumn)) &&
			strings.EqualFold(strings.TrimSpace(fkTable), strings.TrimSpace(parentTable)) &&
			strings.EqualFold(strings.TrimSpace(fkTo), strings.TrimSpace(parentColumn)) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// tableHasCreateSQLContainsCtx 检查表定义里是否包含指定片段。
func tableHasCreateSQLContainsCtx(ctx context.Context, db *sql.DB, table, snippet string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	var sqlText sql.NullString
	err := QueryRowContext(ctx, db, `SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(table)).Scan(&sqlText)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !sqlText.Valid {
		return false, nil
	}
	return strings.Contains(normalizeSQLWhitespace(strings.ToLower(sqlText.String)), normalizeSQLWhitespace(strings.ToLower(snippet))), nil
}

// hasTable 检查表是否存在。
func hasTable(db *sql.DB, name string) (bool, error) {
	return hasSchemaObject(db, name)
}

func hasSchemaObject(db *sql.DB, name string) (bool, error) {
	var one int
	err := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1`, strings.TrimSpace(name)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// tableColumns 返回表里的所有列名。
func tableColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(`PRAGMA table_info(` + strings.TrimSpace(table) + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// tableColumnNotNull 判断列是否为 NOT NULL。
func tableColumnNotNull(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + strings.TrimSpace(table) + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column)) {
			return notNull != 0, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// tableHasIndex 判断表上是否存在指定索引。
func tableHasIndex(db *sql.DB, table, indexName string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	var one int
	err := db.QueryRow(
		`SELECT 1 FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=? LIMIT 1`,
		strings.TrimSpace(table),
		strings.TrimSpace(indexName),
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// tableHasUniqueIndexOnColumns 判断表是否存在覆盖指定列顺序的唯一索引或唯一约束。
func tableHasUniqueIndexOnColumns(db *sql.DB, table string, columns []string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	need := make([]string, 0, len(columns))
	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		need = append(need, strings.ToLower(col))
	}
	if len(need) == 0 {
		return false, fmt.Errorf("columns are required")
	}

	rows, err := db.Query(`PRAGMA index_list(` + strings.TrimSpace(table) + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return false, err
		}
		if unique == 0 {
			continue
		}
		idxCols, err := tableIndexColumns(db, name)
		if err != nil {
			return false, err
		}
		if len(idxCols) != len(need) {
			continue
		}
		matched := true
		for i := range need {
			if strings.ToLower(strings.TrimSpace(idxCols[i])) != need[i] {
				matched = false
				break
			}
		}
		if matched {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func tableIndexColumns(db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.Query(`PRAGMA index_info(` + strings.TrimSpace(indexName) + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type indexColumn struct {
		seqno int
		cid   int
		name  sql.NullString
	}
	cols := make([]indexColumn, 0)
	for rows.Next() {
		var col indexColumn
		if err := rows.Scan(&col.seqno, &col.cid, &col.name); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]string, len(cols))
	for _, col := range cols {
		if col.seqno >= 0 && col.seqno < len(out) {
			out[col.seqno] = col.name.String
		}
	}
	return out, nil
}

func normalizeSQLWhitespace(in string) string {
	return strings.NewReplacer(" ", "", "\n", "", "\t", "", "\r", "").Replace(strings.TrimSpace(in))
}
