package gatewayclient

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
)

func marshalDemandPublishQuotePayload(seedHash string, chunkCount uint32, buyerAddrs []string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-demand-publish-offer-payload-v1",
		strings.ToLower(strings.TrimSpace(seedHash)),
		chunkCount,
		normalizeStringList(buyerAddrs),
	})
}

func marshalDemandPublishBatchQuotePayload(items []*batchPaidItem, buyerAddrs []string) ([]byte, error) {
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

func marshalLiveDemandPublishQuotePayload(streamID string, haveSegmentIndex int64, window uint32, buyerAddrs []string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-live-demand-publish-offer-payload-v1",
		strings.ToLower(strings.TrimSpace(streamID)),
		haveSegmentIndex,
		window,
		normalizeStringList(buyerAddrs),
	})
}

func marshalNodeReachabilityAnnounceQuotePayload(signedAnnouncement []byte) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-node-reachability-announce-offer-payload-v1",
		append([]byte(nil), signedAnnouncement...),
	})
}

func marshalNodeReachabilityQueryQuotePayload(targetNodePubkeyHex string) ([]byte, error) {
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

func unmarshalDemandPublishQuotePayload(raw []byte) (DemandPublishServicePayload, error) {
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