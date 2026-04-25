package gatewayclient

// batchPaidItem 是 batch 批量中的单个需求项。
type batchPaidItem struct {
	SeedHash   string
	ChunkCount uint32
}

// DemandPublishPaidItem 是 batch 批量中的单个需求项。
type DemandPublishPaidItem struct {
	SeedHash   string
	ChunkCount uint32
}

// DemandPublishBatchPaidItem 是批量支付的单个需求项（带索引）。
type DemandPublishBatchPaidItem struct {
	SeedHash   string
	ChunkCount uint32
	Index      uint32
}

// MarshalDemandPublishReceipt marshal demand publish 回执 payload。
func MarshalDemandPublishReceipt(seedHash string, chunkCount uint32, demandID string, published bool) ([]byte, error) {
	return nil, nil
}

// MarshalDemandPublishBatchReceipt marshal demand publish batch 回执 payload。
func MarshalDemandPublishBatchReceipt(items []*DemandPublishBatchPaidItem, demandIDs []string, published []bool) ([]byte, error) {
	return nil, nil
}