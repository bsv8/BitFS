package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domainclient"
)

type walletOrderTemplateDomainRegister struct{}

func (walletOrderTemplateDomainRegister) TemplateID() string {
	return domainQuotePayloadVersion
}

func (walletOrderTemplateDomainRegister) Prepare(ctx context.Context, store *clientDB, rt *Runtime, req WalletOrderRequest) (walletOrderPreparedTx, error) {
	backend := domainClientBackend{rt: rt, store: store}
	quote, err := backend.VerifyRegisterQuote(req.SignerPubkeyHex, req.SignedEnvelope)
	if err != nil {
		return walletOrderPreparedTx{}, err
	}
	built, err := backend.BuildDomainRegisterTx(ctx, req.SignedEnvelope, quote)
	if err != nil {
		return walletOrderPreparedTx{}, err
	}
	preview := WalletOrderPreview{
		Recognized:            true,
		CanSign:               true,
		TemplateID:            domainQuotePayloadVersion,
		BusinessTitle:         "注册域名",
		Summary:               fmt.Sprintf("将为注册域名 %s 生成一笔待提交交易。", quote.Name),
		DetailLines:           buildDomainRegisterPreviewLines(req.SignerPubkeyHex, quote, built),
		WarningLevel:          "normal",
		ServicePubkeyHex:      strings.ToLower(strings.TrimSpace(req.SignerPubkeyHex)),
		PayToAddress:          quote.PayToAddress,
		BusinessAmountSatoshi: quote.TotalPaySatoshi,
		MinerFeeSatoshi:       built.MinerFeeSatoshi,
		TotalSpendSatoshi:     quote.TotalPaySatoshi + built.MinerFeeSatoshi,
		ChangeAmountSatoshi:   built.ChangeSatoshi,
		TxID:                  built.TxID,
		PreviewHash:           walletOrderPreviewHash(built.RawTx),
	}
	return walletOrderPreparedTx{
		Preview:     preview,
		SignedTxHex: hex.EncodeToString(built.RawTx),
		TxID:        built.TxID,
	}, nil
}

func buildDomainRegisterPreviewLines(signerPubkeyHex string, quote domainbiz.DomainRegisterQuote, built domainbiz.BuiltDomainRegisterTx) []string {
	return []string{
		fmt.Sprintf("业务: 注册域名 %s", quote.Name),
		fmt.Sprintf("服务节点公钥: %s", strings.ToLower(strings.TrimSpace(signerPubkeyHex))),
		fmt.Sprintf("收款地址: %s", strings.TrimSpace(quote.PayToAddress)),
		fmt.Sprintf("目标公钥: %s", strings.ToLower(strings.TrimSpace(quote.TargetPubkeyHex))),
		fmt.Sprintf("业务金额: %d sat", quote.TotalPaySatoshi),
		fmt.Sprintf("矿工费: %d sat", built.MinerFeeSatoshi),
		fmt.Sprintf("总支出: %d sat", quote.TotalPaySatoshi+built.MinerFeeSatoshi),
		fmt.Sprintf("找零回钱包: %d sat", built.ChangeSatoshi),
		"说明: 钱包只负责验证业务原文并生成签名交易，不会替页面广播。",
		"风险: 页面后续若把这笔交易提交给 domain 并被接受，链上支付将不可撤销。",
	}
}
