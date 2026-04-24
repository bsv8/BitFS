package broadcast

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
)

// 这些 payload 只负责生成稳定 hash，不承载业务处理逻辑。
// 设计约束：
// - 使用定长数组编码，避免 map/json key 顺序把报价 hash 搞乱；
// - payload 本体由 client/gateway 两侧共同持有，共享层只关注 hash。

func MarshalDemandPublishQuotePayload(seedHash string, chunkCount uint32, buyerAddrs []string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-demand-publish-offer-payload-v1",
		strings.ToLower(strings.TrimSpace(seedHash)),
		chunkCount,
		normalizeStringList(buyerAddrs),
	})
}

func MarshalDemandPublishBatchQuotePayload(items []*DemandPublishBatchPaidItem, buyerAddrs []string) ([]byte, error) {
	type item struct {
		SeedHash   string
		ChunkCount uint32
	}
	normalized := make([]item, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		normalized = append(normalized, item{
			SeedHash:   strings.ToLower(strings.TrimSpace(it.SeedHash)),
			ChunkCount: it.ChunkCount,
		})
	}
	return json.Marshal([]any{
		"bsv8-demand-publish-batch-offer-payload-v1",
		normalized,
		normalizeStringList(buyerAddrs),
	})
}

func MarshalLiveDemandPublishQuotePayload(streamID string, haveSegmentIndex int64, window uint32, buyerAddrs []string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-live-demand-publish-offer-payload-v1",
		strings.ToLower(strings.TrimSpace(streamID)),
		haveSegmentIndex,
		window,
		normalizeStringList(buyerAddrs),
	})
}

func MarshalNodeReachabilityAnnounceQuotePayload(signedAnnouncement []byte) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-node-reachability-announce-offer-payload-v1",
		append([]byte(nil), signedAnnouncement...),
	})
}

func MarshalNodeReachabilityQueryQuotePayload(targetNodePubkeyHex string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-node-reachability-query-offer-payload-v1",
		poolcore.NormalizeClientIDLoose(targetNodePubkeyHex),
	})
}

func normalizeStringList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, raw := range values {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

type DemandPublishServicePayload struct {
	SeedHash   string
	ChunkCount uint32
	BuyerAddrs []string
}

func UnmarshalDemandPublishQuotePayload(raw []byte) (DemandPublishServicePayload, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return DemandPublishServicePayload{}, err
	}
	if len(parts) != 4 {
		return DemandPublishServicePayload{}, fmt.Errorf("demand publish payload fields mismatch")
	}
	var version string
	var out DemandPublishServicePayload
	if err := json.Unmarshal(parts[0], &version); err != nil {
		return DemandPublishServicePayload{}, err
	}
	if version != "bsv8-demand-publish-offer-payload-v1" {
		return DemandPublishServicePayload{}, fmt.Errorf("demand publish payload version mismatch")
	}
	if err := json.Unmarshal(parts[1], &out.SeedHash); err != nil {
		return DemandPublishServicePayload{}, err
	}
	if err := json.Unmarshal(parts[2], &out.ChunkCount); err != nil {
		return DemandPublishServicePayload{}, err
	}
	if err := json.Unmarshal(parts[3], &out.BuyerAddrs); err != nil {
		return DemandPublishServicePayload{}, err
	}
	out.SeedHash = strings.ToLower(strings.TrimSpace(out.SeedHash))
	out.BuyerAddrs = normalizeStringList(out.BuyerAddrs)
	return out, nil
}

type DemandPublishBatchServicePayload struct {
	Items      []*DemandPublishBatchPaidItem
	BuyerAddrs []string
}

func UnmarshalDemandPublishBatchQuotePayload(raw []byte) (DemandPublishBatchServicePayload, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return DemandPublishBatchServicePayload{}, err
	}
	if len(parts) != 3 {
		return DemandPublishBatchServicePayload{}, fmt.Errorf("demand publish batch payload fields mismatch")
	}
	var version string
	var items []struct {
		SeedHash   string `json:"SeedHash"`
		ChunkCount uint32 `json:"ChunkCount"`
	}
	var out DemandPublishBatchServicePayload
	if err := json.Unmarshal(parts[0], &version); err != nil {
		return DemandPublishBatchServicePayload{}, err
	}
	if version != "bsv8-demand-publish-batch-offer-payload-v1" {
		return DemandPublishBatchServicePayload{}, fmt.Errorf("demand publish batch payload version mismatch")
	}
	if err := json.Unmarshal(parts[1], &items); err != nil {
		return DemandPublishBatchServicePayload{}, err
	}
	if err := json.Unmarshal(parts[2], &out.BuyerAddrs); err != nil {
		return DemandPublishBatchServicePayload{}, err
	}
	out.Items = make([]*DemandPublishBatchPaidItem, 0, len(items))
	for _, it := range items {
		out.Items = append(out.Items, &DemandPublishBatchPaidItem{
			SeedHash:   strings.ToLower(strings.TrimSpace(it.SeedHash)),
			ChunkCount: it.ChunkCount,
		})
	}
	out.BuyerAddrs = normalizeStringList(out.BuyerAddrs)
	return out, nil
}

type LiveDemandPublishServicePayload struct {
	StreamID         string
	HaveSegmentIndex int64
	Window           uint32
	BuyerAddrs       []string
}

func UnmarshalLiveDemandPublishQuotePayload(raw []byte) (LiveDemandPublishServicePayload, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return LiveDemandPublishServicePayload{}, err
	}
	if len(parts) != 5 {
		return LiveDemandPublishServicePayload{}, fmt.Errorf("live demand publish payload fields mismatch")
	}
	var version string
	var out LiveDemandPublishServicePayload
	if err := json.Unmarshal(parts[0], &version); err != nil {
		return LiveDemandPublishServicePayload{}, err
	}
	if version != "bsv8-live-demand-publish-offer-payload-v1" {
		return LiveDemandPublishServicePayload{}, fmt.Errorf("live demand publish payload version mismatch")
	}
	if err := json.Unmarshal(parts[1], &out.StreamID); err != nil {
		return LiveDemandPublishServicePayload{}, err
	}
	if err := json.Unmarshal(parts[2], &out.HaveSegmentIndex); err != nil {
		return LiveDemandPublishServicePayload{}, err
	}
	if err := json.Unmarshal(parts[3], &out.Window); err != nil {
		return LiveDemandPublishServicePayload{}, err
	}
	if err := json.Unmarshal(parts[4], &out.BuyerAddrs); err != nil {
		return LiveDemandPublishServicePayload{}, err
	}
	out.StreamID = strings.ToLower(strings.TrimSpace(out.StreamID))
	out.BuyerAddrs = normalizeStringList(out.BuyerAddrs)
	return out, nil
}

type NodeReachabilityAnnounceServicePayload struct {
	SignedAnnouncement []byte
}

func UnmarshalNodeReachabilityAnnounceQuotePayload(raw []byte) (NodeReachabilityAnnounceServicePayload, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return NodeReachabilityAnnounceServicePayload{}, err
	}
	if len(parts) != 2 {
		return NodeReachabilityAnnounceServicePayload{}, fmt.Errorf("node reachability announce payload fields mismatch")
	}
	var version string
	var out NodeReachabilityAnnounceServicePayload
	if err := json.Unmarshal(parts[0], &version); err != nil {
		return NodeReachabilityAnnounceServicePayload{}, err
	}
	if version != "bsv8-node-reachability-announce-offer-payload-v1" {
		return NodeReachabilityAnnounceServicePayload{}, fmt.Errorf("node reachability announce payload version mismatch")
	}
	if err := json.Unmarshal(parts[1], &out.SignedAnnouncement); err != nil {
		return NodeReachabilityAnnounceServicePayload{}, err
	}
	return out, nil
}

type NodeReachabilityQueryServicePayload struct {
	TargetNodePubkeyHex string
}

func UnmarshalNodeReachabilityQueryQuotePayload(raw []byte) (NodeReachabilityQueryServicePayload, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return NodeReachabilityQueryServicePayload{}, err
	}
	if len(parts) != 2 {
		return NodeReachabilityQueryServicePayload{}, fmt.Errorf("node reachability query payload fields mismatch")
	}
	var version string
	var out NodeReachabilityQueryServicePayload
	if err := json.Unmarshal(parts[0], &version); err != nil {
		return NodeReachabilityQueryServicePayload{}, err
	}
	if version != "bsv8-node-reachability-query-offer-payload-v1" {
		return NodeReachabilityQueryServicePayload{}, fmt.Errorf("node reachability query payload version mismatch")
	}
	if err := json.Unmarshal(parts[1], &out.TargetNodePubkeyHex); err != nil {
		return NodeReachabilityQueryServicePayload{}, err
	}
	out.TargetNodePubkeyHex = poolcore.NormalizeClientIDLoose(out.TargetNodePubkeyHex)
	return out, nil
}
