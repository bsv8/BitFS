package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
)

// WalletOrderRequest 是页面交给钱包的签名订单原文。
// 设计说明：
// - 页面只负责搬运远端给出的 [[字段...],签名]，不自己解释业务；
// - 钱包本地根据模板 ID 决定能否验证、能否组交易、如何给用户解释；
// - signer_pubkey_hex 明确告诉钱包“这份业务原文是谁签的”，避免页面口头说明和验签目标脱节。
type WalletOrderRequest struct {
	SignerPubkeyHex     string          `json:"signer_pubkey_hex"`
	SignedEnvelope      json.RawMessage `json:"signed_envelope"`
	ExpectedPreviewHash string          `json:"expected_preview_hash,omitempty"`
}

type WalletOrderPreview struct {
	Recognized            bool     `json:"recognized"`
	CanSign               bool     `json:"can_sign"`
	TemplateID            string   `json:"template_id,omitempty"`
	BusinessTitle         string   `json:"business_title,omitempty"`
	Summary               string   `json:"summary"`
	DetailLines           []string `json:"detail_lines,omitempty"`
	WarningLevel          string   `json:"warning_level,omitempty"`
	ServicePubkeyHex      string   `json:"service_pubkey_hex,omitempty"`
	PayToAddress          string   `json:"pay_to_address,omitempty"`
	BusinessAmountSatoshi uint64   `json:"business_amount_satoshi,omitempty"`
	MinerFeeSatoshi       uint64   `json:"miner_fee_satoshi,omitempty"`
	TotalSpendSatoshi     uint64   `json:"total_spend_satoshi,omitempty"`
	ChangeAmountSatoshi   uint64   `json:"change_amount_satoshi,omitempty"`
	TxID                  string   `json:"txid,omitempty"`
	PreviewHash           string   `json:"preview_hash,omitempty"`
}

type WalletOrderPreviewResp struct {
	Ok      bool               `json:"ok"`
	Code    string             `json:"code"`
	Message string             `json:"message,omitempty"`
	Preview WalletOrderPreview `json:"preview"`
}

type WalletOrderSignResp struct {
	Ok          bool               `json:"ok"`
	Code        string             `json:"code"`
	Message     string             `json:"message,omitempty"`
	Preview     WalletOrderPreview `json:"preview"`
	SignedTxHex string             `json:"signed_tx_hex,omitempty"`
	TxID        string             `json:"txid,omitempty"`
}

// BizOrderPayBSVRequest 是新的 BSV 业务下单输入。
// 设计说明：
// - order_id 是业务主单号，也是重复提交去重键；为空时由 BitFS 自动创建；
// - 这里只描述“要付什么”，真正的链上支付留给 settle 层执行。
type BizOrderPayBSVRequest struct {
	OrderID       string `json:"order_id"`
	ToAddress     string `json:"to_address"`
	AmountSatoshi uint64 `json:"amount_satoshi"`
}

// BizOrderPayBSVResponse 是新的 BSV 业务下单结果。
// 设计说明：
// - ok=false 不一定是技术错误，很多时候只是业务上还没付成；
// - status 直接给前台看，方便按业务主表聚合展示；
// - summary 里会带出这张订单下所有 payment 单的汇总。
type BizOrderPayBSVResponse struct {
	Ok                   bool                         `json:"ok"`
	Code                 string                       `json:"code"`
	Message              string                       `json:"message,omitempty"`
	OrderID              string                       `json:"order_id,omitempty"`
	SettlementID         string                       `json:"settlement_id,omitempty"`
	Status               string                       `json:"status,omitempty"`
	TxID                 string                       `json:"txid,omitempty"`
	TargetAmountSatoshi  uint64                       `json:"target_amount_satoshi,omitempty"`
	SettledAmountSatoshi uint64                       `json:"settled_amount_satoshi,omitempty"`
	FrontOrderSummary    *FrontOrderSettlementSummary `json:"summary,omitempty"`
}

type walletOrderPreparedTx struct {
	Preview     WalletOrderPreview
	SignedTxHex string
	TxID        string
}

type walletOrderTemplate interface {
	TemplateID() string
	Prepare(context.Context, *clientDB, *Runtime, WalletOrderRequest) (walletOrderPreparedTx, error)
}

func TriggerWalletOrderPreview(ctx context.Context, store *clientDB, rt *Runtime, req WalletOrderRequest) (WalletOrderPreviewResp, error) {
	prepared, err := prepareWalletOrderTx(ctx, store, rt, req)
	if err != nil {
		return WalletOrderPreviewResp{}, err
	}
	return WalletOrderPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: prepared.Preview,
	}, nil
}

func TriggerWalletOrderSign(ctx context.Context, store *clientDB, rt *Runtime, req WalletOrderRequest) (WalletOrderSignResp, error) {
	prepared, err := prepareWalletOrderTx(ctx, store, rt, req)
	if err != nil {
		return WalletOrderSignResp{}, err
	}
	if !prepared.Preview.CanSign {
		return WalletOrderSignResp{
			Ok:      false,
			Code:    "UNKNOWN_TEMPLATE",
			Message: "wallet order template not found",
			Preview: prepared.Preview,
		}, nil
	}
	expected := strings.ToLower(strings.TrimSpace(req.ExpectedPreviewHash))
	if expected == "" {
		return WalletOrderSignResp{
			Ok:      false,
			Code:    "PREVIEW_REQUIRED",
			Message: "expected preview hash is required",
			Preview: prepared.Preview,
		}, nil
	}
	if !strings.EqualFold(expected, prepared.Preview.PreviewHash) {
		return WalletOrderSignResp{
			Ok:      false,
			Code:    "PREVIEW_CHANGED",
			Message: "wallet preview changed; retry",
			Preview: prepared.Preview,
		}, nil
	}
	return WalletOrderSignResp{
		Ok:          true,
		Code:        "OK",
		Message:     "",
		Preview:     prepared.Preview,
		SignedTxHex: prepared.SignedTxHex,
		TxID:        prepared.TxID,
	}, nil
}

func prepareWalletOrderTx(ctx context.Context, store *clientDB, rt *Runtime, req WalletOrderRequest) (walletOrderPreparedTx, error) {
	_ = ctx
	if rt == nil || store == nil || rt.ActionChain == nil {
		return walletOrderPreparedTx{}, fmt.Errorf("runtime not initialized")
	}
	signerPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(req.SignerPubkeyHex))
	if err != nil {
		return walletOrderPreparedTx{}, fmt.Errorf("signer_pubkey_hex invalid: %w", err)
	}
	envelopeRaw := append([]byte(nil), req.SignedEnvelope...)
	if len(strings.TrimSpace(string(envelopeRaw))) == 0 {
		return walletOrderPreparedTx{}, fmt.Errorf("signed_envelope is required")
	}
	templateID, err := walletOrderTemplateIDFromEnvelope(envelopeRaw)
	if err != nil {
		return walletOrderPreparedTx{}, err
	}
	tpl, ok := walletOrderTemplateRegistry()[templateID]
	if !ok {
		preview := buildUnknownWalletOrderPreview(signerPubkeyHex, templateID)
		return walletOrderPreparedTx{Preview: preview}, nil
	}
	prepared, err := tpl.Prepare(ctx, store, rt, WalletOrderRequest{
		SignerPubkeyHex:     signerPubkeyHex,
		SignedEnvelope:      envelopeRaw,
		ExpectedPreviewHash: strings.TrimSpace(req.ExpectedPreviewHash),
	})
	if err != nil {
		return walletOrderPreparedTx{}, err
	}
	return prepared, nil
}

func walletOrderTemplateRegistry() map[string]walletOrderTemplate {
	return map[string]walletOrderTemplate{
		domainQuotePayloadVersion: walletOrderTemplateDomainRegister{},
	}
}

func walletOrderTemplateIDFromEnvelope(raw []byte) (string, error) {
	var env domainwire.SignedEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("decode signed envelope: %w", err)
	}
	if len(env.Fields) == 0 {
		return "", fmt.Errorf("signed envelope fields missing")
	}
	templateID := strings.TrimSpace(asStringField(env.Fields[0]))
	if templateID == "" {
		return "", fmt.Errorf("signed envelope template id missing")
	}
	return templateID, nil
}

func buildUnknownWalletOrderPreview(signerPubkeyHex string, templateID string) WalletOrderPreview {
	lines := []string{
		fmt.Sprintf("签发节点公钥: %s", strings.ToLower(strings.TrimSpace(signerPubkeyHex))),
	}
	if strings.TrimSpace(templateID) != "" {
		lines = append(lines, fmt.Sprintf("模板标识: %s", strings.TrimSpace(templateID)))
	}
	lines = append(lines,
		"说明: 当前钱包没有这个业务模板，无法安全解释交易意图，也不会替页面组装交易。",
		"建议: 只在你完全确认来源和业务语义时，再继续接入对应模板。",
	)
	return WalletOrderPreview{
		Recognized:       false,
		CanSign:          false,
		TemplateID:       strings.TrimSpace(templateID),
		BusinessTitle:    "未知交易请求",
		Summary:          "未知交易请求，高度注意",
		DetailLines:      lines,
		WarningLevel:     "high",
		ServicePubkeyHex: strings.ToLower(strings.TrimSpace(signerPubkeyHex)),
	}
}

func walletOrderPreviewHash(rawTx []byte) string {
	sum := sha256.Sum256(rawTx)
	return hex.EncodeToString(sum[:])
}

// bizOrderPayBSVIdentity 生成业务下单、结算单、触发器使用的稳定主键。
// 设计说明：
// - 同一个 order_id 必须落到同一组主键；
// - 这样重复提交只会回到同一条业务线，不会再生一张新单。
func bizOrderPayBSVIdentity(orderID string) (frontOrderID string, businessID string, settlementID string, triggerID string) {
	orderID = strings.ToLower(strings.TrimSpace(orderID))
	sum := sha256.Sum256([]byte(orderID))
	suffix := hex.EncodeToString(sum[:12])
	frontOrderID = orderID
	businessID = "biz_order_pay_bsv_" + suffix
	settlementID = "set_order_pay_bsv_" + suffix
	triggerID = "trg_order_pay_bsv_" + suffix
	return
}
