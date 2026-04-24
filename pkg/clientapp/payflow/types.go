package payflow

import contractpayflow "github.com/bsv8/BFTP-contract/pkg/v1/payflow"

// 设计说明：
// - payflow 协议契约已迁移到 BFTP-contract/pkg/v1/payflow；
// - 本包保留同名 type alias 与函数转发，避免上层调用路径变化。

type QuoteRef = contractpayflow.QuoteRef
type PaymentRef = contractpayflow.PaymentRef
type ServiceOffer = contractpayflow.ServiceOffer
type ServiceQuote = contractpayflow.ServiceQuote
type ChargeIntent = contractpayflow.ChargeIntent
type ClientCommit = contractpayflow.ClientCommit
type AcceptedCharge = contractpayflow.AcceptedCharge
type ProofState = contractpayflow.ProofState
type ServiceReceipt = contractpayflow.ServiceReceipt

func HashIntent(v ChargeIntent) (string, error) {
	return contractpayflow.HashIntent(v)
}

func HashServiceOffer(v ServiceOffer) (string, error) {
	return contractpayflow.HashServiceOffer(v)
}

func HashServiceQuote(v ServiceQuote) (string, error) {
	return contractpayflow.HashServiceQuote(v)
}

func HashClientCommit(v ClientCommit) (string, error) {
	return contractpayflow.HashClientCommit(v)
}

func HashAcceptedCharge(v AcceptedCharge) (string, error) {
	return contractpayflow.HashAcceptedCharge(v)
}

func HashPayloadBytes(raw []byte) string {
	return contractpayflow.HashPayloadBytes(raw)
}

func MarshalIntent(v ChargeIntent) ([]byte, error) {
	return contractpayflow.MarshalIntent(v)
}

func MarshalServiceOffer(v ServiceOffer) ([]byte, error) {
	return contractpayflow.MarshalServiceOffer(v)
}

func MarshalServiceQuote(v ServiceQuote) ([]byte, error) {
	return contractpayflow.MarshalServiceQuote(v)
}

func MarshalClientCommit(v ClientCommit) ([]byte, error) {
	return contractpayflow.MarshalClientCommit(v)
}

func MarshalSignedClientCommit(v ClientCommit, sig []byte) ([]byte, error) {
	return contractpayflow.MarshalSignedClientCommit(v, sig)
}

func MarshalAcceptedCharge(v AcceptedCharge) ([]byte, error) {
	return contractpayflow.MarshalAcceptedCharge(v)
}

func MarshalProofState(v ProofState) ([]byte, error) {
	return contractpayflow.MarshalProofState(v)
}

func MarshalServiceReceipt(v ServiceReceipt) ([]byte, error) {
	return contractpayflow.MarshalServiceReceipt(v)
}

func UnmarshalServiceOffer(raw []byte) (ServiceOffer, error) {
	return contractpayflow.UnmarshalServiceOffer(raw)
}

func UnmarshalServiceQuote(raw []byte) (ServiceQuote, error) {
	return contractpayflow.UnmarshalServiceQuote(raw)
}

func UnmarshalIntent(raw []byte) (ChargeIntent, error) {
	return contractpayflow.UnmarshalIntent(raw)
}

func UnmarshalClientCommit(raw []byte) (ClientCommit, error) {
	return contractpayflow.UnmarshalClientCommit(raw)
}

func UnmarshalSignedClientCommit(raw []byte) (ClientCommit, []byte, error) {
	return contractpayflow.UnmarshalSignedClientCommit(raw)
}

func UnmarshalAcceptedCharge(raw []byte) (AcceptedCharge, error) {
	return contractpayflow.UnmarshalAcceptedCharge(raw)
}

func UnmarshalProofState(raw []byte) (ProofState, error) {
	return contractpayflow.UnmarshalProofState(raw)
}

func UnmarshalServiceReceipt(raw []byte) (ServiceReceipt, error) {
	return contractpayflow.UnmarshalServiceReceipt(raw)
}
