package poolcore

import "strings"

// BoundQuoteChargeReasons 是运行时注入的“必须绑定 service quote 才允许 pay”的业务集合。
// 设计说明：
// - listen_cycle_fee 属于费用池底座自有能力，因此仍由 poolcore 内建；
// - 其它公开业务能力在装配阶段自行注入，避免 poolcore 认识具体模块名字；
// - 这里故意只做字符串集合，不把业务逻辑反向拉回底座。
type BoundQuoteChargeReasons map[string]struct{}

func NewBoundQuoteChargeReasons(reasons ...string) BoundQuoteChargeReasons {
	out := make(BoundQuoteChargeReasons, len(reasons))
	for _, raw := range reasons {
		reason := strings.TrimSpace(raw)
		if reason == "" {
			continue
		}
		out[reason] = struct{}{}
	}
	return out
}

func (s *GatewayService) RequiresBoundServiceQuote(chargeReason string) bool {
	reason := strings.TrimSpace(chargeReason)
	if reason == QuoteServiceTypeListenCycle {
		return true
	}
	if s == nil || len(s.BoundQuoteChargeReasons) == 0 {
		return false
	}
	_, ok := s.BoundQuoteChargeReasons[reason]
	return ok
}
