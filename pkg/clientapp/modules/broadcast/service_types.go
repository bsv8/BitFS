package broadcast

const (
	// QuoteServiceType* 是 broadcast 模块对费用池报价层暴露的业务类型。
	// 设计说明：
	// - 这些 service_type 属于公告模块自己的公开合同，不属于费用池底座；
	// - poolcore 只验证“是否有绑定 quote”，不再枚举 broadcast 业务名；
	// - client / gateway 共用这些常量，避免两侧各写一份字符串再慢慢漂移。
	QuoteServiceTypeDemandPublish            = "demand_publish_fee"
	QuoteServiceTypeDemandPublishBatch       = "demand_publish_batch_fee"
	QuoteServiceTypeLiveDemandPublish        = "live_demand_publish_fee"
	QuoteServiceTypeNodeReachabilityAnnounce = "node_reachability_announce_fee"
	QuoteServiceTypeNodeReachabilityQuery    = "node_reachability_query_fee"
)

// BoundQuoteChargeReasonSet 返回 broadcast 模块需要“quote 强绑定”校验的扣费原因集合。
// 说明：gateway 在装配阶段把它注入 poolcore，避免底座直接依赖具体业务模块。
func BoundQuoteChargeReasonSet() map[string]struct{} {
	return map[string]struct{}{
		QuoteServiceTypeDemandPublish:            {},
		QuoteServiceTypeDemandPublishBatch:       {},
		QuoteServiceTypeLiveDemandPublish:        {},
		QuoteServiceTypeNodeReachabilityAnnounce: {},
		QuoteServiceTypeNodeReachabilityQuery:    {},
	}
}
