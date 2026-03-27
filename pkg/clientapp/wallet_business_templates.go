package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/modules/domain"
)

// WalletBusinessRequest 是页面交给钱包的签名业务原文。
// 设计说明：
// - 页面只负责搬运远端给出的 [[字段...],签名]，不自己解释业务；
// - 钱包本地根据模板 ID 决定能否验证、能否组交易、如何给用户解释；
// - signer_pubkey_hex 明确告诉钱包“这份业务原文是谁签的”，避免页面口头说明和验签目标脱节。
type WalletBusinessRequest struct {
	SignerPubkeyHex     string          `json:"signer_pubkey_hex"`
	SignedEnvelope      json.RawMessage `json:"signed_envelope"`
	ExpectedPreviewHash string          `json:"expected_preview_hash,omitempty"`
}

type WalletBusinessPreview struct {
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

type WalletBusinessPreviewResp struct {
	Ok      bool                  `json:"ok"`
	Code    string                `json:"code"`
	Message string                `json:"message,omitempty"`
	Preview WalletBusinessPreview `json:"preview"`
}

type WalletBusinessSignResp struct {
	Ok          bool                  `json:"ok"`
	Code        string                `json:"code"`
	Message     string                `json:"message,omitempty"`
	Preview     WalletBusinessPreview `json:"preview"`
	SignedTxHex string                `json:"signed_tx_hex,omitempty"`
	TxID        string                `json:"txid,omitempty"`
}

type walletBusinessPreparedTx struct {
	Preview     WalletBusinessPreview
	SignedTxHex string
	TxID        string
}

type walletBusinessTemplate interface {
	TemplateID() string
	Prepare(context.Context, *Runtime, WalletBusinessRequest) (walletBusinessPreparedTx, error)
}

func TriggerWalletBusinessPreview(ctx context.Context, rt *Runtime, req WalletBusinessRequest) (WalletBusinessPreviewResp, error) {
	prepared, err := prepareWalletBusinessTx(ctx, rt, req)
	if err != nil {
		return WalletBusinessPreviewResp{}, err
	}
	return WalletBusinessPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: prepared.Preview,
	}, nil
}

func TriggerWalletBusinessSign(ctx context.Context, rt *Runtime, req WalletBusinessRequest) (WalletBusinessSignResp, error) {
	prepared, err := prepareWalletBusinessTx(ctx, rt, req)
	if err != nil {
		return WalletBusinessSignResp{}, err
	}
	if !prepared.Preview.CanSign {
		return WalletBusinessSignResp{
			Ok:      false,
			Code:    "UNKNOWN_TEMPLATE",
			Message: "wallet business template not found",
			Preview: prepared.Preview,
		}, nil
	}
	expected := strings.ToLower(strings.TrimSpace(req.ExpectedPreviewHash))
	if expected == "" {
		return WalletBusinessSignResp{
			Ok:      false,
			Code:    "PREVIEW_REQUIRED",
			Message: "expected preview hash is required",
			Preview: prepared.Preview,
		}, nil
	}
	if !strings.EqualFold(expected, prepared.Preview.PreviewHash) {
		return WalletBusinessSignResp{
			Ok:      false,
			Code:    "PREVIEW_CHANGED",
			Message: "wallet preview changed; retry",
			Preview: prepared.Preview,
		}, nil
	}
	return WalletBusinessSignResp{
		Ok:          true,
		Code:        "OK",
		Message:     "",
		Preview:     prepared.Preview,
		SignedTxHex: prepared.SignedTxHex,
		TxID:        prepared.TxID,
	}, nil
}

func prepareWalletBusinessTx(ctx context.Context, rt *Runtime, req WalletBusinessRequest) (walletBusinessPreparedTx, error) {
	_ = ctx
	if rt == nil || rt.DB == nil || rt.ActionChain == nil {
		return walletBusinessPreparedTx{}, fmt.Errorf("runtime not initialized")
	}
	signerPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(req.SignerPubkeyHex))
	if err != nil {
		return walletBusinessPreparedTx{}, fmt.Errorf("signer_pubkey_hex invalid: %w", err)
	}
	envelopeRaw := append([]byte(nil), req.SignedEnvelope...)
	if len(strings.TrimSpace(string(envelopeRaw))) == 0 {
		return walletBusinessPreparedTx{}, fmt.Errorf("signed_envelope is required")
	}
	templateID, err := walletBusinessTemplateIDFromEnvelope(envelopeRaw)
	if err != nil {
		return walletBusinessPreparedTx{}, err
	}
	tpl, ok := walletBusinessTemplateRegistry()[templateID]
	if !ok {
		preview := buildUnknownWalletBusinessPreview(signerPubkeyHex, templateID)
		return walletBusinessPreparedTx{Preview: preview}, nil
	}
	prepared, err := tpl.Prepare(ctx, rt, WalletBusinessRequest{
		SignerPubkeyHex:     signerPubkeyHex,
		SignedEnvelope:      envelopeRaw,
		ExpectedPreviewHash: strings.TrimSpace(req.ExpectedPreviewHash),
	})
	if err != nil {
		return walletBusinessPreparedTx{}, err
	}
	return prepared, nil
}

func walletBusinessTemplateRegistry() map[string]walletBusinessTemplate {
	return map[string]walletBusinessTemplate{
		domainQuotePayloadVersion: walletBusinessTemplateDomainRegister{},
	}
}

func walletBusinessTemplateIDFromEnvelope(raw []byte) (string, error) {
	var env domainmodule.SignedEnvelope
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

func buildUnknownWalletBusinessPreview(signerPubkeyHex string, templateID string) WalletBusinessPreview {
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
	return WalletBusinessPreview{
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

func walletBusinessPreviewHash(rawTx []byte) string {
	sum := sha256.Sum256(rawTx)
	return hex.EncodeToString(sum[:])
}
