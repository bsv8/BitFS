package gatewayclient

const (
	// QuoteServiceType* 是 gatewayclient 模块对费用池报价层暴露的业务类型。
	QuoteServiceTypeDemandPublish            = "demand_publish_fee"
	QuoteServiceTypeDemandPublishBatch       = "demand_publish_batch_fee"
	QuoteServiceTypeLiveDemandPublish        = "live_demand_publish_fee"
	QuoteServiceTypeNodeReachabilityAnnounce = "node_reachability_announce_fee"
	QuoteServiceTypeNodeReachabilityQuery    = "node_reachability_query_fee"
)