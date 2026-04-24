package poolcore

import (
	"encoding/json"
	"fmt"
)

// 这些 payload 只负责生成稳定 hash，不承载业务处理逻辑。
// 设计约束：
// - 使用定长数组编码，避免 map/json key 顺序把报价 hash 搞乱；
// - payload 本体由 client/gateway 两侧共同持有，共享层只关注 hash。

func MarshalListenCycleQuotePayload(requestedDurationSeconds uint32, requestedUntilUnix int64, proposedPaymentSatoshi uint64) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-listen-cycle-offer-payload-v2",
		requestedDurationSeconds,
		requestedUntilUnix,
		proposedPaymentSatoshi,
	})
}

type ListenCycleServicePayload struct {
	RequestedDurationSeconds uint32
	RequestedUntilUnix       int64
	ProposedPaymentSatoshi   uint64
}

func UnmarshalListenCycleQuotePayload(raw []byte) (ListenCycleServicePayload, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return ListenCycleServicePayload{}, err
	}
	if len(parts) != 4 {
		return ListenCycleServicePayload{}, fmt.Errorf("listen cycle payload fields mismatch")
	}
	var version string
	var out ListenCycleServicePayload
	if err := json.Unmarshal(parts[0], &version); err != nil {
		return ListenCycleServicePayload{}, err
	}
	if version != "bsv8-listen-cycle-offer-payload-v2" {
		return ListenCycleServicePayload{}, fmt.Errorf("listen cycle payload version mismatch")
	}
	if err := json.Unmarshal(parts[1], &out.RequestedDurationSeconds); err != nil {
		return ListenCycleServicePayload{}, err
	}
	if err := json.Unmarshal(parts[2], &out.RequestedUntilUnix); err != nil {
		return ListenCycleServicePayload{}, err
	}
	if err := json.Unmarshal(parts[3], &out.ProposedPaymentSatoshi); err != nil {
		return ListenCycleServicePayload{}, err
	}
	return out, nil
}
