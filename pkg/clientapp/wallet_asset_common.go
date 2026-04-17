package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// 这些 helper 只服务于当前保留下来的 bsv21 create/send 主链路。
// 旧的 token/ordinal 查询面已经删除，因此这里不再承载任何历史资产影子逻辑。

func loadWalletUTXOsByID(ctx context.Context, store *clientDB, address string, utxoIDs []string) ([]poolcore.UTXO, error) {
	return dbLoadWalletUTXOsByID(ctx, store, address, utxoIDs)
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
