package payflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

const proofStateVersion = "bsv8-feepool-proof-state-v1"

func normalizeHex(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func marshalArray(items []any) ([]byte, error) {
	raw, err := json.Marshal(items)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func hashRawHex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
