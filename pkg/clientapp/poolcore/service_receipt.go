package poolcore

import (
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
)

// BuildSignedServiceReceipt 基于一次已受理扣费结果生成链下业务回执。
// 设计说明：
// - 回执必须绑定 offer_hash，避免把别的业务结果嫁接到本次成交；
// - payloadBytes 由业务层自己定义，只要求 client/server 使用同一份规范化编码。
func BuildSignedServiceReceipt(serverPrivHex string, isMainnet bool, offerHash string, serviceType string, payloadBytes []byte) ([]byte, error) {
	if strings.TrimSpace(serverPrivHex) == "" {
		return nil, fmt.Errorf("server private key required")
	}
	if strings.TrimSpace(offerHash) == "" {
		return nil, fmt.Errorf("offer_hash required")
	}
	actor, err := BuildActor("service_receipt", strings.TrimSpace(serverPrivHex), isMainnet)
	if err != nil {
		return nil, err
	}
	receipt, err := payflow.SignServiceReceipt(payflow.ServiceReceipt{
		OfferHash:    strings.ToLower(strings.TrimSpace(offerHash)),
		ServiceType:  strings.TrimSpace(serviceType),
		ResultHash:   payflow.HashPayloadBytes(payloadBytes),
		IssuedAtUnix: time.Now().Unix(),
	}, actor.PrivKey)
	if err != nil {
		return nil, err
	}
	return payflow.MarshalServiceReceipt(receipt)
}
