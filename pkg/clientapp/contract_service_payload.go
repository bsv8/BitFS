package clientapp

import (
	"encoding/json"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
)

const (
	// 广播业务报价类型（与网关侧保持一致，避免 client 依赖 runtime 模块常量）。
	quoteServiceTypeDemandPublish            = "demand_publish_fee"
	quoteServiceTypeDemandPublishBatch       = "demand_publish_batch_fee"
	quoteServiceTypeLiveDemandPublish        = "live_demand_publish_fee"
	quoteServiceTypeNodeReachabilityAnnounce = "node_reachability_announce_fee"
	quoteServiceTypeNodeReachabilityQuery    = "node_reachability_query_fee"
)

func marshalDemandPublishQuotePayload(seedHash string, chunkCount uint32, buyerAddrs []string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-demand-publish-offer-payload-v1",
		strings.ToLower(strings.TrimSpace(seedHash)),
		chunkCount,
		normalizeQuoteStringList(buyerAddrs),
	})
}

func marshalDemandPublishBatchQuotePayload(items []*contractmessage.DemandPublishBatchPaidItem, buyerAddrs []string) ([]byte, error) {
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
		normalizeQuoteStringList(buyerAddrs),
	})
}

func marshalLiveDemandPublishQuotePayload(streamID string, haveSegmentIndex int64, window uint32, buyerAddrs []string) ([]byte, error) {
	return json.Marshal([]any{
		"bsv8-live-demand-publish-offer-payload-v1",
		strings.ToLower(strings.TrimSpace(streamID)),
		haveSegmentIndex,
		window,
		normalizeQuoteStringList(buyerAddrs),
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

func normalizeQuoteStringList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

const (
	// 服务回执类型（与网关签名服务回执时使用的 service_type 保持一致）。
	serviceTypeListenCycle              = "bitcast-gateway/listen_cycle_paid"
	serviceTypeDemandPublish            = "bitcast-gateway/demand_publish_paid"
	serviceTypeDemandPublishBatch       = "bitcast-gateway/demand_publish_batch_paid"
	serviceTypeLiveDemandPublish        = "bitcast-gateway/live_demand_publish_paid"
	serviceTypeNodeReachabilityAnnounce = "bitcast-gateway/node_reachability_announce_paid"
	serviceTypeNodeReachabilityQuery    = "bitcast-gateway/node_reachability_query_paid"
	serviceTypeResolveName              = string(contractroute.RouteDomainV1Resolve)
	serviceTypeQueryName                = string(contractroute.RouteDomainV1Query)
	serviceTypeRegisterLock             = string(contractroute.RouteDomainV1Lock)
	serviceTypeSetTarget                = string(contractroute.RouteDomainV1SetTarget)
)

func marshalListenCycleServicePayload(resp contractmessage.ListenCyclePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.GrantedDurationSeconds,
		resp.GrantedUntilUnix,
		resp.Error,
	})
}

func marshalDemandPublishServicePayload(resp contractmessage.DemandPublishPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.DemandID,
		resp.Published,
		resp.Error,
	})
}

func marshalDemandPublishBatchServicePayload(resp contractmessage.DemandPublishBatchPaidResp) ([]byte, error) {
	type item struct {
		SeedHash   string `json:"seed_hash"`
		ChunkCount uint32 `json:"chunk_count"`
		DemandID   string `json:"demand_id"`
		Status     string `json:"status"`
	}
	items := make([]item, 0, len(resp.Items))
	for _, it := range resp.Items {
		if it == nil {
			continue
		}
		items = append(items, item{
			SeedHash:   it.SeedHash,
			ChunkCount: it.ChunkCount,
			DemandID:   it.DemandID,
			Status:     it.Status,
		})
	}
	return json.Marshal([]any{
		resp.Status,
		items,
		resp.PublishedCount,
		resp.Error,
	})
}

func marshalLiveDemandPublishServicePayload(resp contractmessage.LiveDemandPublishPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.DemandID,
		resp.Published,
		resp.Error,
	})
}

func marshalNodeReachabilityAnnounceServicePayload(resp contractmessage.NodeReachabilityAnnouncePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Published,
		resp.Error,
	})
}

func marshalNodeReachabilityQueryServicePayload(resp contractmessage.NodeReachabilityQueryPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Found,
		resp.TargetNodePubkeyHex,
		resp.SignedAnnouncement,
		resp.Error,
	})
}

func marshalResolveNameServicePayload(resp contractmessage.ResolveNamePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.OwnerPubkeyHex,
		resp.TargetPubkeyHex,
		resp.ExpireAtUnix,
		resp.SignedRecordJSON,
		resp.Error,
	})
}

func marshalQueryNameServicePayload(resp contractmessage.QueryNamePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.Available,
		resp.Locked,
		resp.Registered,
		resp.OwnerPubkeyHex,
		resp.TargetPubkeyHex,
		resp.ExpireAtUnix,
		resp.LockExpiresAtUnix,
		resp.RegisterPriceSatoshi,
		resp.RegisterSubmitFeeSatoshi,
		resp.RegisterLockFeeSatoshi,
		resp.SetTargetFeeSatoshi,
		resp.ResolveFeeSatoshi,
		resp.QueryFeeSatoshi,
		resp.SignedRecordJSON,
		resp.Error,
	})
}

func marshalRegisterLockServicePayload(resp contractmessage.RegisterLockPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.TargetPubkeyHex,
		resp.LockExpiresAtUnix,
		resp.SignedQuoteJSON,
		resp.Error,
	})
}

func marshalSetTargetServicePayload(resp contractmessage.SetTargetPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.OwnerPubkeyHex,
		resp.TargetPubkeyHex,
		resp.ExpireAtUnix,
		resp.SignedRecordJSON,
		resp.Error,
	})
}
