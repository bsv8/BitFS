package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
)

type walletTokenBalanceItem struct {
	TokenStandard string `json:"token_standard"`
	AssetKey      string `json:"asset_key"`
	AssetSymbol   string `json:"asset_symbol"`
	QuantityText  string `json:"quantity_text"`
	OutputCount   int    `json:"output_count"`
	SourceName    string `json:"source_name"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

type walletTokenBalanceList struct {
	WalletAddress string                   `json:"wallet_address"`
	Total         int                      `json:"total"`
	Limit         int                      `json:"limit"`
	Offset        int                      `json:"offset"`
	Items         []walletTokenBalanceItem `json:"items"`
}

type walletTokenOutputItem struct {
	UTXOID           string          `json:"utxo_id"`
	WalletAddress    string          `json:"wallet_address"`
	TxID             string          `json:"txid"`
	Vout             uint32          `json:"vout"`
	ValueSatoshi     uint64          `json:"value_satoshi"`
	AllocationClass  string          `json:"allocation_class"`
	AllocationReason string          `json:"allocation_reason"`
	TokenStandard    string          `json:"token_standard"`
	AssetKey         string          `json:"asset_key"`
	AssetSymbol      string          `json:"asset_symbol"`
	QuantityText     string          `json:"quantity_text"`
	SourceName       string          `json:"source_name"`
	UpdatedAtUnix    int64           `json:"updated_at_unix"`
	Payload          json.RawMessage `json:"payload"`
}

type walletTokenOutputList struct {
	WalletAddress string                  `json:"wallet_address"`
	Total         int                     `json:"total"`
	Limit         int                     `json:"limit"`
	Offset        int                     `json:"offset"`
	Items         []walletTokenOutputItem `json:"items"`
}

type walletAssetEventItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	UTXOID        string          `json:"utxo_id"`
	WalletAddress string          `json:"wallet_address"`
	AssetGroup    string          `json:"asset_group"`
	AssetStandard string          `json:"asset_standard"`
	AssetKey      string          `json:"asset_key"`
	AssetSymbol   string          `json:"asset_symbol"`
	QuantityText  string          `json:"quantity_text"`
	SourceName    string          `json:"source_name"`
	EventType     string          `json:"event_type"`
	RefTxID       string          `json:"ref_txid"`
	RefBusinessID string          `json:"ref_business_id"`
	Note          string          `json:"note"`
	Payload       json.RawMessage `json:"payload"`
}

type walletAssetEventList struct {
	WalletAddress string                 `json:"wallet_address"`
	Total         int                    `json:"total"`
	Limit         int                    `json:"limit"`
	Offset        int                    `json:"offset"`
	Items         []walletAssetEventItem `json:"items"`
}

type walletOrdinalItem struct {
	UTXOID           string          `json:"utxo_id"`
	WalletAddress    string          `json:"wallet_address"`
	TxID             string          `json:"txid"`
	Vout             uint32          `json:"vout"`
	ValueSatoshi     uint64          `json:"value_satoshi"`
	AllocationClass  string          `json:"allocation_class"`
	AllocationReason string          `json:"allocation_reason"`
	AssetStandard    string          `json:"asset_standard"`
	AssetKey         string          `json:"asset_key"`
	AssetSymbol      string          `json:"asset_symbol"`
	SourceName       string          `json:"source_name"`
	UpdatedAtUnix    int64           `json:"updated_at_unix"`
	Payload          json.RawMessage `json:"payload"`
}

type walletOrdinalList struct {
	WalletAddress string              `json:"wallet_address"`
	Total         int                 `json:"total"`
	Limit         int                 `json:"limit"`
	Offset        int                 `json:"offset"`
	Items         []walletOrdinalItem `json:"items"`
}

type decimalTextValue struct {
	intValue *big.Int
	scale    int
}

type decimalTextAccumulator struct {
	sum   *big.Int
	scale int
}

// resolveWalletAddressForHTTP 统一给钱包查询面拿当前钱包地址。
// 设计说明：
// - 正式运行时优先使用当前 runtime 钱包地址，避免多钱包测试夹具把查询口径搞乱；
// - 仅在最小测试环境里，才退回到本地 wallet_utxo 投影里的最新地址，保证 handler 可以被单测直接驱动。
func resolveWalletAddressForHTTP(ctx context.Context, s *httpAPIServer) (string, error) {
	if s == nil {
		return "", fmt.Errorf("http api server is nil")
	}
	if s.rt != nil {
		addr, err := clientWalletAddress(s.rt)
		if err == nil && strings.TrimSpace(addr) != "" {
			return strings.TrimSpace(addr), nil
		}
	}
	return httpDBValue(ctx, s, func(db *sql.DB) (string, error) {
		var addr string
		err := db.QueryRow(`SELECT address FROM wallet_utxo ORDER BY updated_at_unix DESC,created_at_unix DESC,utxo_id ASC LIMIT 1`).Scan(&addr)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return strings.TrimSpace(addr), nil
	})
}

func normalizeWalletTokenStandard(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ""
	case "bsv20":
		return "bsv20"
	case "bsv21":
		return "bsv21"
	default:
		return ""
	}
}

func loadWalletTokenBalances(db *sql.DB, address string, standard string, limit int, offset int) (walletTokenBalanceList, error) {
	if db == nil {
		return walletTokenBalanceList{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletTokenBalanceList{Limit: limit, Offset: offset, Items: []walletTokenBalanceItem{}}, nil
	}
	walletID := walletIDByAddress(address)
	args := []any{walletID, address, walletAssetGroupToken}
	where := ` WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'`
	if standard != "" {
		where += ` AND a.asset_standard=?`
		args = append(args, standard)
	}
	rows, err := db.Query(
		`SELECT a.asset_standard,a.asset_key,a.asset_symbol,a.quantity_text,a.source_name,a.updated_at_unix
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id
		`+where+`
		 ORDER BY a.asset_standard ASC,a.asset_key ASC,a.updated_at_unix DESC,a.utxo_id ASC`,
		args...,
	)
	if err != nil {
		return walletTokenBalanceList{}, err
	}
	defer rows.Close()

	type groupState struct {
		item walletTokenBalanceItem
		acc  decimalTextAccumulator
	}
	orderedKeys := make([]string, 0, 16)
	grouped := map[string]*groupState{}
	for rows.Next() {
		var standard string
		var assetKey string
		var assetSymbol string
		var quantityText string
		var sourceName string
		var updatedAtUnix int64
		if err := rows.Scan(&standard, &assetKey, &assetSymbol, &quantityText, &sourceName, &updatedAtUnix); err != nil {
			return walletTokenBalanceList{}, err
		}
		key := standard + "\x00" + assetKey
		state, ok := grouped[key]
		if !ok {
			state = &groupState{
				item: walletTokenBalanceItem{
					TokenStandard: standard,
					AssetKey:      assetKey,
					AssetSymbol:   strings.TrimSpace(assetSymbol),
					QuantityText:  "0",
					OutputCount:   0,
					SourceName:    strings.TrimSpace(sourceName),
					UpdatedAtUnix: updatedAtUnix,
				},
			}
			if state.item.AssetSymbol == "" {
				state.item.AssetSymbol = assetKey
			}
			grouped[key] = state
			orderedKeys = append(orderedKeys, key)
		}
		if err := state.acc.Add(quantityText); err != nil {
			return walletTokenBalanceList{}, fmt.Errorf("token quantity_text invalid for %s: %w", assetKey, err)
		}
		state.item.OutputCount++
		if updatedAtUnix > state.item.UpdatedAtUnix {
			state.item.UpdatedAtUnix = updatedAtUnix
		}
		if state.item.SourceName == "" {
			state.item.SourceName = strings.TrimSpace(sourceName)
		}
	}
	if err := rows.Err(); err != nil {
		return walletTokenBalanceList{}, err
	}
	items := make([]walletTokenBalanceItem, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		state := grouped[key]
		state.item.QuantityText = state.acc.String()
		items = append(items, state.item)
	}
	total := len(items)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := items[offset:end]
	if page == nil {
		page = []walletTokenBalanceItem{}
	}
	return walletTokenBalanceList{
		WalletAddress: address,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
		Items:         page,
	}, nil
}

func loadWalletTokenOutputs(db *sql.DB, address string, standard string, assetKey string, limit int, offset int) (walletTokenOutputList, error) {
	if db == nil {
		return walletTokenOutputList{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletTokenOutputList{Limit: limit, Offset: offset, Items: []walletTokenOutputItem{}}, nil
	}
	walletID := walletIDByAddress(address)
	args := []any{walletID, address, walletAssetGroupToken}
	where := ` WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'`
	if standard != "" {
		where += ` AND a.asset_standard=?`
		args = append(args, standard)
	}
	if assetKey != "" {
		where += ` AND a.asset_key=?`
		args = append(args, assetKey)
	}
	var total int
	if err := db.QueryRow(
		`SELECT COUNT(1)
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id`+where,
		args...,
	).Scan(&total); err != nil {
		return walletTokenOutputList{}, err
	}
	rows, err := db.Query(
		`SELECT u.utxo_id,u.txid,u.vout,u.value_satoshi,u.allocation_class,u.allocation_reason,
		        a.asset_standard,a.asset_key,a.asset_symbol,a.quantity_text,a.source_name,a.payload_json,a.updated_at_unix
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id`+where+`
		 ORDER BY a.updated_at_unix DESC,u.created_at_unix DESC,u.utxo_id ASC
		 LIMIT ? OFFSET ?`,
		append(args, limit, offset)...,
	)
	if err != nil {
		return walletTokenOutputList{}, err
	}
	defer rows.Close()
	items := make([]walletTokenOutputItem, 0, limit)
	for rows.Next() {
		var it walletTokenOutputItem
		var payload string
		if err := rows.Scan(
			&it.UTXOID,
			&it.TxID,
			&it.Vout,
			&it.ValueSatoshi,
			&it.AllocationClass,
			&it.AllocationReason,
			&it.TokenStandard,
			&it.AssetKey,
			&it.AssetSymbol,
			&it.QuantityText,
			&it.SourceName,
			&payload,
			&it.UpdatedAtUnix,
		); err != nil {
			return walletTokenOutputList{}, err
		}
		it.WalletAddress = address
		it.Payload = json.RawMessage(payload)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return walletTokenOutputList{}, err
	}
	return walletTokenOutputList{
		WalletAddress: address,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
		Items:         items,
	}, nil
}

func loadWalletTokenOutputDetail(db *sql.DB, address string, standard string, utxoID string, assetKey string) (walletTokenOutputItem, error) {
	if db == nil {
		return walletTokenOutputItem{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletTokenOutputItem{}, sql.ErrNoRows
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	assetKey = strings.TrimSpace(assetKey)
	if utxoID == "" && assetKey == "" {
		return walletTokenOutputItem{}, fmt.Errorf("token output selector is required")
	}
	walletID := walletIDByAddress(address)
	args := []any{walletID, address, walletAssetGroupToken}
	where := ` WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'`
	if standard != "" {
		where += ` AND a.asset_standard=?`
		args = append(args, standard)
	}
	if utxoID != "" {
		where += ` AND u.utxo_id=?`
		args = append(args, utxoID)
	} else {
		where += ` AND a.asset_key=?`
		args = append(args, assetKey)
	}
	var it walletTokenOutputItem
	var payload string
	err := db.QueryRow(
		`SELECT u.utxo_id,u.txid,u.vout,u.value_satoshi,u.allocation_class,u.allocation_reason,
		        a.asset_standard,a.asset_key,a.asset_symbol,a.quantity_text,a.source_name,a.payload_json,a.updated_at_unix
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id`+where+`
		 ORDER BY a.updated_at_unix DESC,u.created_at_unix DESC,u.utxo_id ASC
		 LIMIT 1`,
		args...,
	).Scan(
		&it.UTXOID,
		&it.TxID,
		&it.Vout,
		&it.ValueSatoshi,
		&it.AllocationClass,
		&it.AllocationReason,
		&it.TokenStandard,
		&it.AssetKey,
		&it.AssetSymbol,
		&it.QuantityText,
		&it.SourceName,
		&payload,
		&it.UpdatedAtUnix,
	)
	if err != nil {
		return walletTokenOutputItem{}, err
	}
	it.WalletAddress = address
	it.Payload = json.RawMessage(payload)
	return it, nil
}

func loadWalletAssetEvents(db *sql.DB, address string, assetGroup string, standard string, assetKey string, utxoID string, limit int, offset int) (walletAssetEventList, error) {
	if db == nil {
		return walletAssetEventList{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletAssetEventList{Limit: limit, Offset: offset, Items: []walletAssetEventItem{}}, nil
	}
	walletID := walletIDByAddress(address)
	args := []any{walletID, address, strings.TrimSpace(assetGroup)}
	where := ` WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'`
	if standard != "" {
		where += ` AND a.asset_standard=?`
		args = append(args, standard)
	}
	if assetKey != "" {
		where += ` AND a.asset_key=?`
		args = append(args, assetKey)
	}
	if utxoID != "" {
		where += ` AND a.utxo_id=?`
		args = append(args, strings.ToLower(strings.TrimSpace(utxoID)))
	}
	var total int
	if err := db.QueryRow(
		`SELECT COUNT(1)
		 FROM wallet_utxo_events e
		 JOIN wallet_utxo_assets a ON a.utxo_id=e.utxo_id
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id`+where,
		args...,
	).Scan(&total); err != nil {
		return walletAssetEventList{}, err
	}
	rows, err := db.Query(
		`SELECT e.id,e.created_at_unix,e.utxo_id,
		        a.asset_group,a.asset_standard,a.asset_key,a.asset_symbol,a.quantity_text,a.source_name,
		        e.event_type,e.ref_txid,e.ref_business_id,e.note,e.payload_json
		 FROM wallet_utxo_events e
		 JOIN wallet_utxo_assets a ON a.utxo_id=e.utxo_id
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id`+where+`
		 ORDER BY e.id DESC
		 LIMIT ? OFFSET ?`,
		append(args, limit, offset)...,
	)
	if err != nil {
		return walletAssetEventList{}, err
	}
	defer rows.Close()
	items := make([]walletAssetEventItem, 0, limit)
	for rows.Next() {
		var it walletAssetEventItem
		var payload string
		if err := rows.Scan(
			&it.ID,
			&it.CreatedAtUnix,
			&it.UTXOID,
			&it.AssetGroup,
			&it.AssetStandard,
			&it.AssetKey,
			&it.AssetSymbol,
			&it.QuantityText,
			&it.SourceName,
			&it.EventType,
			&it.RefTxID,
			&it.RefBusinessID,
			&it.Note,
			&payload,
		); err != nil {
			return walletAssetEventList{}, err
		}
		it.WalletAddress = address
		it.Payload = json.RawMessage(payload)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return walletAssetEventList{}, err
	}
	return walletAssetEventList{
		WalletAddress: address,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
		Items:         items,
	}, nil
}

func loadWalletOrdinals(db *sql.DB, address string, limit int, offset int) (walletOrdinalList, error) {
	if db == nil {
		return walletOrdinalList{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletOrdinalList{Limit: limit, Offset: offset, Items: []walletOrdinalItem{}}, nil
	}
	walletID := walletIDByAddress(address)
	countArgs := []any{walletID, address, walletAssetGroupOrdinal}
	var total int
	if err := db.QueryRow(
		`SELECT COUNT(1)
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id
		 WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'`,
		countArgs...,
	).Scan(&total); err != nil {
		return walletOrdinalList{}, err
	}
	rows, err := db.Query(
		`SELECT u.utxo_id,u.txid,u.vout,u.value_satoshi,u.allocation_class,u.allocation_reason,
		        a.asset_standard,a.asset_key,a.asset_symbol,a.source_name,a.payload_json,a.updated_at_unix
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id
		 WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'
		 ORDER BY a.updated_at_unix DESC,u.created_at_unix DESC,u.utxo_id ASC
		 LIMIT ? OFFSET ?`,
		walletID, address, walletAssetGroupOrdinal, limit, offset,
	)
	if err != nil {
		return walletOrdinalList{}, err
	}
	defer rows.Close()
	items := make([]walletOrdinalItem, 0, limit)
	for rows.Next() {
		var it walletOrdinalItem
		var payload string
		if err := rows.Scan(
			&it.UTXOID,
			&it.TxID,
			&it.Vout,
			&it.ValueSatoshi,
			&it.AllocationClass,
			&it.AllocationReason,
			&it.AssetStandard,
			&it.AssetKey,
			&it.AssetSymbol,
			&it.SourceName,
			&payload,
			&it.UpdatedAtUnix,
		); err != nil {
			return walletOrdinalList{}, err
		}
		it.WalletAddress = address
		it.Payload = json.RawMessage(payload)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return walletOrdinalList{}, err
	}
	return walletOrdinalList{
		WalletAddress: address,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
		Items:         items,
	}, nil
}

func loadWalletOrdinalDetail(db *sql.DB, address string, utxoID string, assetKey string) (walletOrdinalItem, error) {
	if db == nil {
		return walletOrdinalItem{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletOrdinalItem{}, sql.ErrNoRows
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	assetKey = strings.TrimSpace(assetKey)
	if utxoID == "" && assetKey == "" {
		return walletOrdinalItem{}, fmt.Errorf("ordinal selector is required")
	}
	walletID := walletIDByAddress(address)
	args := []any{walletID, address, walletAssetGroupOrdinal}
	where := ` WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND u.state='unspent'`
	if utxoID != "" {
		where += ` AND u.utxo_id=?`
		args = append(args, utxoID)
	} else {
		where += ` AND a.asset_key=?`
		args = append(args, assetKey)
	}
	var it walletOrdinalItem
	var payload string
	err := db.QueryRow(
		`SELECT u.utxo_id,u.txid,u.vout,u.value_satoshi,u.allocation_class,u.allocation_reason,
		        a.asset_standard,a.asset_key,a.asset_symbol,a.source_name,a.payload_json,a.updated_at_unix
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id`+where+`
		 ORDER BY a.updated_at_unix DESC,u.created_at_unix DESC,u.utxo_id ASC
		 LIMIT 1`,
		args...,
	).Scan(
		&it.UTXOID,
		&it.TxID,
		&it.Vout,
		&it.ValueSatoshi,
		&it.AllocationClass,
		&it.AllocationReason,
		&it.AssetStandard,
		&it.AssetKey,
		&it.AssetSymbol,
		&it.SourceName,
		&payload,
		&it.UpdatedAtUnix,
	)
	if err != nil {
		return walletOrdinalItem{}, err
	}
	it.WalletAddress = address
	it.Payload = json.RawMessage(payload)
	return it, nil
}

func (s *httpAPIServer) handleWalletTokenBalances(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	standardRaw := strings.TrimSpace(r.URL.Query().Get("standard"))
	standard := normalizeWalletTokenStandard(standardRaw)
	if standardRaw != "" && standard == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid token standard"})
		return
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletTokenBalanceList, error) {
		return loadWalletTokenBalances(db, address, standard, limit, offset)
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletTokenOutputs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	standardRaw := strings.TrimSpace(r.URL.Query().Get("standard"))
	standard := normalizeWalletTokenStandard(standardRaw)
	if standardRaw != "" && standard == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid token standard"})
		return
	}
	assetKey := strings.TrimSpace(r.URL.Query().Get("asset_key"))
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletTokenOutputList, error) {
		return loadWalletTokenOutputs(db, address, standard, assetKey, limit, offset)
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletTokenOutputDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	assetKey := strings.TrimSpace(r.URL.Query().Get("asset_key"))
	standardRaw := strings.TrimSpace(r.URL.Query().Get("standard"))
	standard := normalizeWalletTokenStandard(standardRaw)
	if standardRaw != "" && standard == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid token standard"})
		return
	}
	if utxoID == "" && assetKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "token output selector is required"})
		return
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletTokenOutputItem, error) {
		return loadWalletTokenOutputDetail(db, address, standard, utxoID, assetKey)
	})
	if err != nil {
		if err == sql.ErrNoRows {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletTokenEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	standardRaw := strings.TrimSpace(r.URL.Query().Get("standard"))
	standard := normalizeWalletTokenStandard(standardRaw)
	if standardRaw != "" && standard == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid token standard"})
		return
	}
	assetKey := strings.TrimSpace(r.URL.Query().Get("asset_key"))
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletAssetEventList, error) {
		return loadWalletAssetEvents(db, address, walletAssetGroupToken, standard, assetKey, utxoID, limit, offset)
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletOrdinals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletOrdinalList, error) {
		return loadWalletOrdinals(db, address, limit, offset)
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletOrdinalDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	assetKey := strings.TrimSpace(r.URL.Query().Get("asset_key"))
	if utxoID == "" && assetKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "ordinal selector is required"})
		return
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletOrdinalItem, error) {
		return loadWalletOrdinalDetail(db, address, utxoID, assetKey)
	})
	if err != nil {
		if err == sql.ErrNoRows {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletOrdinalEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	assetKey := strings.TrimSpace(r.URL.Query().Get("asset_key"))
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	resp, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletAssetEventList, error) {
		return loadWalletAssetEvents(db, address, walletAssetGroupOrdinal, "ordinal", assetKey, utxoID, limit, offset)
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *decimalTextAccumulator) Add(raw string) error {
	value, err := parseDecimalText(raw)
	if err != nil {
		return err
	}
	if a.sum == nil {
		a.sum = big.NewInt(0)
	}
	if a.scale == 0 && a.sum.Sign() == 0 {
		a.scale = value.scale
		a.sum = new(big.Int).Set(value.intValue)
		return nil
	}
	if value.scale > a.scale {
		a.sum.Mul(a.sum, decimalPow10(value.scale-a.scale))
		a.scale = value.scale
	}
	term := new(big.Int).Set(value.intValue)
	if value.scale < a.scale {
		term.Mul(term, decimalPow10(a.scale-value.scale))
	}
	a.sum.Add(a.sum, term)
	return nil
}

func (a decimalTextAccumulator) String() string {
	if a.sum == nil || a.sum.Sign() == 0 {
		return "0"
	}
	return formatDecimalText(a.sum, a.scale)
}

func parseDecimalText(raw string) (decimalTextValue, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return decimalTextValue{}, fmt.Errorf("empty decimal text")
	}
	negative := false
	switch value[0] {
	case '+':
		value = strings.TrimSpace(value[1:])
	case '-':
		negative = true
		value = strings.TrimSpace(value[1:])
	}
	if value == "" {
		return decimalTextValue{}, fmt.Errorf("empty decimal text")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 {
		return decimalTextValue{}, fmt.Errorf("invalid decimal text")
	}
	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	if intPart == "" {
		intPart = "0"
	}
	if !isDecimalDigits(intPart) || !isDecimalDigits(fracPart) {
		return decimalTextValue{}, fmt.Errorf("invalid decimal text")
	}
	fracPart = strings.TrimRight(fracPart, "0")
	scale := len(fracPart)
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return decimalTextValue{intValue: big.NewInt(0), scale: 0}, nil
	}
	intValue, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return decimalTextValue{}, fmt.Errorf("invalid decimal text")
	}
	if negative {
		intValue.Neg(intValue)
	}
	return decimalTextValue{intValue: intValue, scale: scale}, nil
}

func formatDecimalText(value *big.Int, scale int) string {
	if value == nil || value.Sign() == 0 {
		return "0"
	}
	negative := value.Sign() < 0
	absValue := new(big.Int).Abs(value)
	digits := absValue.String()
	if scale == 0 {
		if negative {
			return "-" + digits
		}
		return digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	split := len(digits) - scale
	out := digits[:split] + "." + digits[split:]
	out = strings.TrimRight(out, "0")
	out = strings.TrimRight(out, ".")
	if out == "" {
		out = "0"
	}
	if negative && out != "0" {
		out = "-" + out
	}
	return out
}

func decimalPow10(exp int) *big.Int {
	if exp <= 0 {
		return big.NewInt(1)
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil)
}

func isDecimalDigits(raw string) bool {
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
