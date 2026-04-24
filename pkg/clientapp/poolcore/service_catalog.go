package poolcore

import (
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
)

// BillableServiceDecision 是单个收费服务在报价阶段给费用池底座的决策结果。
// 设计说明：
// - 业务模块只声明“这个 service_type 怎么验、怎么报价”；
// - poolcore 继续统一处理 offer 绑定校验、签名报价和公共状态推进；
// - 这样业务目录能正式收口，但底座仍然不反向依赖具体业务实现。
type BillableServiceDecision struct {
	Status                     string
	Error                      string
	ChargeReason               string
	ChargeAmountSatoshi        uint64
	QuoteTTLSeconds            uint32
}

type BillableServiceContext struct {
	Request              ServiceQuoteReq
	Offer                payflow.ServiceOffer
	ServiceParamsPayload []byte
	Gateway              *GatewayService
}

type BillableServiceQuoteFunc func(BillableServiceContext) (BillableServiceDecision, error)

type BillableServiceDecl struct {
	ServiceType  string
	ChargeReason string
	Quote        BillableServiceQuoteFunc
}

type ServiceCatalog struct {
	declsByServiceType map[string]BillableServiceDecl
}

func NewServiceCatalog(decls ...BillableServiceDecl) (ServiceCatalog, error) {
	out := ServiceCatalog{
		declsByServiceType: make(map[string]BillableServiceDecl, len(decls)),
	}
	for _, raw := range decls {
		decl, key, err := normalizeBillableServiceDecl(raw)
		if err != nil {
			return ServiceCatalog{}, err
		}
		if _, exists := out.declsByServiceType[key]; exists {
			return ServiceCatalog{}, fmt.Errorf("billable service conflict: %q", decl.ServiceType)
		}
		out.declsByServiceType[key] = decl
	}
	return out, nil
}

func MustNewServiceCatalog(decls ...BillableServiceDecl) ServiceCatalog {
	catalog, err := NewServiceCatalog(decls...)
	if err != nil {
		panic(err)
	}
	return catalog
}

func (c ServiceCatalog) BoundQuoteChargeReasons() BoundQuoteChargeReasons {
	out := make(BoundQuoteChargeReasons, len(c.declsByServiceType))
	for _, decl := range c.declsByServiceType {
		reason := strings.TrimSpace(decl.ChargeReason)
		if reason == "" {
			continue
		}
		out[reason] = struct{}{}
	}
	return out
}

func (c ServiceCatalog) Quote(service *GatewayService, req ServiceQuoteReq) (ServiceQuoteResp, error) {
	offer, err := payflow.UnmarshalServiceOffer(req.ServiceOffer)
	if err != nil {
		return ServiceQuoteResp{}, err
	}
	if !strings.EqualFold(strings.TrimSpace(offer.ClientPubkeyHex), strings.TrimSpace(req.ClientID)) {
		return ServiceQuoteResp{Success: false, Status: "rejected", Error: "service offer client mismatch"}, nil
	}
	if string(offer.RequestParams) != string(req.ServiceParamsPayload) {
		return ServiceQuoteResp{Success: false, Status: "rejected", Error: "service request_params mismatch"}, nil
	}
	decl, ok := c.lookup(strings.TrimSpace(offer.ServiceType))
	if !ok {
		return ServiceQuoteResp{Success: false, Status: "rejected", Error: "unsupported service_type"}, nil
	}
	if decl.Quote == nil {
		return ServiceQuoteResp{}, fmt.Errorf("billable service %q quote handler missing", decl.ServiceType)
	}
	decision, err := decl.Quote(BillableServiceContext{
		Request:              req,
		Offer:                offer,
		ServiceParamsPayload: append([]byte(nil), req.ServiceParamsPayload...),
		Gateway:              service,
	})
	if err != nil {
		return ServiceQuoteResp{}, err
	}
	if strings.TrimSpace(decision.Status) == "" {
		decision.Status = "accepted"
	}
	if strings.TrimSpace(decision.Error) != "" {
		if strings.TrimSpace(decision.Status) == "" {
			decision.Status = "rejected"
		}
		return ServiceQuoteResp{
			Success: false,
			Status:  strings.TrimSpace(decision.Status),
			Error:   strings.TrimSpace(decision.Error),
		}, nil
	}
	if strings.TrimSpace(decision.ChargeReason) == "" && strings.TrimSpace(decl.ChargeReason) == "" {
		return ServiceQuoteResp{}, fmt.Errorf("billable service %q charge_reason required", decl.ServiceType)
	}
	if decision.ChargeAmountSatoshi == 0 {
		return ServiceQuoteResp{}, fmt.Errorf("billable service %q charge_amount_satoshi required", decl.ServiceType)
	}
	_, raw, status, err := service.BuildServiceQuote(ServiceQuoteBuildInput{
		Offer:               offer,
		ChargeAmountSatoshi: decision.ChargeAmountSatoshi,
		QuoteTTLSeconds:     decision.QuoteTTLSeconds,
	})
	if err != nil {
		return ServiceQuoteResp{}, err
	}
	return ServiceQuoteResp{
		Success:      true,
		Status:       status,
		ServiceQuote: raw,
	}, nil
}

func (c ServiceCatalog) lookup(serviceType string) (BillableServiceDecl, bool) {
	decl, ok := c.declsByServiceType[normalizeBillableServiceKey(serviceType)]
	return decl, ok
}

func normalizeBillableServiceDecl(raw BillableServiceDecl) (BillableServiceDecl, string, error) {
	raw.ServiceType = strings.TrimSpace(raw.ServiceType)
	raw.ChargeReason = strings.TrimSpace(raw.ChargeReason)
	if raw.ServiceType == "" {
		return BillableServiceDecl{}, "", fmt.Errorf("billable service service_type required")
	}
	if raw.Quote == nil {
		return BillableServiceDecl{}, "", fmt.Errorf("billable service %q quote handler required", raw.ServiceType)
	}
	return raw, normalizeBillableServiceKey(raw.ServiceType), nil
}

func normalizeBillableServiceKey(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}
