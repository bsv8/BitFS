package clientapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	WalletAssetCLIQueryTokenBalances     = "wallet_token_balances"
	WalletAssetCLIQueryTokenOutputs      = "wallet_token_outputs"
	WalletAssetCLIQueryTokenOutputDetail = "wallet_token_output_detail"
	WalletAssetCLIQueryTokenEvents       = "wallet_token_events"
	WalletAssetCLIQueryOrdinals          = "wallet_ordinals"
	WalletAssetCLIQueryOrdinalDetail     = "wallet_ordinal_detail"
	WalletAssetCLIQueryOrdinalEvents     = "wallet_ordinal_events"
)

type WalletAssetCLIQuery struct {
	Kind     string
	Standard string
	AssetKey string
	UTXOID   string
	Limit    int
	Offset   int
}

// RunWalletAssetCLIQuery 让纯 Go CLI 直接复用本地资产投影查询。
// 设计说明：
// - CLI 只负责收参数和打印结果，不重复实现 token / ordinal 查询逻辑；
// - 这里直接读本地 index sqlite，避免为了只读查询还必须先把 daemon 跑起来；
// - 返回值直接是格式化好的 JSON，方便 cmd/client 保持极薄。
func RunWalletAssetCLIQuery(indexDBPath string, req WalletAssetCLIQuery) ([]byte, error) {
	indexDBPath = strings.TrimSpace(indexDBPath)
	if indexDBPath == "" {
		return nil, fmt.Errorf("index db path is empty")
	}
	db, err := sql.Open("sqlite", indexDBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	if err := applySQLitePragmas(db); err != nil {
		return nil, err
	}
	address, err := loadCLIWalletQueryAddress(db)
	if err != nil {
		return nil, err
	}
	standard := normalizeWalletTokenStandard(req.Standard)
	if strings.TrimSpace(req.Standard) != "" && standard == "" {
		return nil, fmt.Errorf("invalid token standard")
	}
	limit := cliWalletAssetBoundInt(req.Limit, 50, 1, 500)
	offset := cliWalletAssetBoundInt(req.Offset, 0, 0, 1_000_000)
	var payload any
	switch strings.TrimSpace(req.Kind) {
	case WalletAssetCLIQueryTokenBalances:
		payload, err = loadWalletTokenBalances(db, address, standard, limit, offset)
	case WalletAssetCLIQueryTokenOutputs:
		payload, err = loadWalletTokenOutputs(db, address, standard, strings.TrimSpace(req.AssetKey), limit, offset)
	case WalletAssetCLIQueryTokenOutputDetail:
		payload, err = loadWalletTokenOutputDetail(db, address, standard, req.UTXOID, req.AssetKey)
	case WalletAssetCLIQueryTokenEvents:
		payload, err = loadWalletAssetEvents(db, address, walletAssetGroupToken, standard, req.AssetKey, req.UTXOID, limit, offset)
	case WalletAssetCLIQueryOrdinals:
		payload, err = loadWalletOrdinals(db, address, limit, offset)
	case WalletAssetCLIQueryOrdinalDetail:
		payload, err = loadWalletOrdinalDetail(db, address, req.UTXOID, req.AssetKey)
	case WalletAssetCLIQueryOrdinalEvents:
		payload, err = loadWalletAssetEvents(db, address, walletAssetGroupOrdinal, "ordinal", req.AssetKey, req.UTXOID, limit, offset)
	default:
		return nil, fmt.Errorf("unsupported wallet asset cli query kind")
	}
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(payload, "", "  ")
}

func loadCLIWalletQueryAddress(db *sql.DB) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	var address string
	err := db.QueryRow(`SELECT address FROM wallet_utxo ORDER BY updated_at_unix DESC,created_at_unix DESC,utxo_id ASC LIMIT 1`).Scan(&address)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(address), nil
}

func cliWalletAssetBoundInt(value int, def, minV, maxV int) int {
	raw := ""
	if value != 0 {
		raw = strconv.Itoa(value)
	}
	return parseBoundInt(raw, def, minV, maxV)
}
