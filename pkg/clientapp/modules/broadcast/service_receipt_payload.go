package broadcast

import "encoding/json"

const (
	ServiceTypeListenCycle              = "bitcast-gateway/listen_cycle_paid"
	ServiceTypeDemandPublish            = "bitcast-gateway/demand_publish_paid"
	ServiceTypeDemandPublishBatch       = "bitcast-gateway/demand_publish_batch_paid"
	ServiceTypeLiveDemandPublish        = "bitcast-gateway/live_demand_publish_paid"
	ServiceTypeNodeReachabilityAnnounce = "bitcast-gateway/node_reachability_announce_paid"
	ServiceTypeNodeReachabilityQuery    = "bitcast-gateway/node_reachability_query_paid"
)

func MarshalListenCycleServicePayload(resp ListenCyclePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.GrantedDurationSeconds,
		resp.GrantedUntilUnix,
		resp.Error,
	})
}

func MarshalDemandPublishServicePayload(resp DemandPublishPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.DemandID,
		resp.Published,
		resp.Error,
	})
}

func MarshalDemandPublishBatchServicePayload(resp DemandPublishBatchPaidResp) ([]byte, error) {
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

func MarshalLiveDemandPublishServicePayload(resp LiveDemandPublishPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.DemandID,
		resp.Published,
		resp.Error,
	})
}

func MarshalNodeReachabilityAnnounceServicePayload(resp NodeReachabilityAnnouncePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Published,
		resp.Error,
	})
}

func MarshalNodeReachabilityQueryServicePayload(resp NodeReachabilityQueryPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Found,
		resp.TargetNodePubkeyHex,
		resp.SignedAnnouncement,
		resp.Error,
	})
}
