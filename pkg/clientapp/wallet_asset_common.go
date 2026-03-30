package clientapp

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// 这些 helper 只服务于当前保留下来的 bsv21 create/send 主链路。
// 旧的 token/ordinal 查询面已经删除，因此这里不再承载任何历史资产影子逻辑。

func loadWalletUTXOsByID(db *sql.DB, address string, utxoIDs []string) ([]poolcore.UTXO, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" || len(utxoIDs) == 0 {
		return []poolcore.UTXO{}, nil
	}
	walletID := walletIDByAddress(address)
	out := make([]poolcore.UTXO, 0, len(utxoIDs))
	for _, rawID := range utxoIDs {
		utxoID := strings.ToLower(strings.TrimSpace(rawID))
		if utxoID == "" {
			continue
		}
		var item poolcore.UTXO
		err := db.QueryRow(
			`SELECT txid,vout,value_satoshi
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND utxo_id=? AND state='unspent'`,
			walletID,
			address,
			utxoID,
		).Scan(&item.TxID, &item.Vout, &item.Value)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, fmt.Errorf("wallet utxo not found: %s", utxoID)
			}
			return nil, err
		}
		item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
		out = append(out, item)
	}
	return out, nil
}

func mustDecodeHex(raw string) []byte {
	data, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		panic(err)
	}
	return data
}

func firstNonEmptyStringField(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[strings.TrimSpace(key)]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if trimmed := strings.TrimSpace(typed); trimmed != "" {
				return trimmed
			}
		case fmt.Stringer:
			if trimmed := strings.TrimSpace(typed.String()); trimmed != "" {
				return trimmed
			}
		case []byte:
			if trimmed := strings.TrimSpace(string(typed)); trimmed != "" {
				return trimmed
			}
		default:
			text := strings.TrimSpace(fmt.Sprint(typed))
			if text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}
