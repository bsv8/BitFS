package ncall

import contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"

const (
	ContentTypeProto = contractmessage.ContentTypeProto
)

type CallReq = contractmessage.CallReq
type PaymentOption = contractmessage.PaymentOption
type CallResp = contractmessage.CallResp
type ResolveReq = contractmessage.ResolveReq
type ResolveResp = contractmessage.ResolveResp
type FeePool2of2Payment = contractmessage.FeePool2of2Payment
type FeePool2of2Receipt = contractmessage.FeePool2of2Receipt
type ChainTxV1Payment = contractmessage.ChainTxV1Payment
type ChainTxV1Receipt = contractmessage.ChainTxV1Receipt
type CapabilityItem = contractmessage.CapabilityItem
type CapabilitiesShowBody = contractmessage.CapabilitiesShowBody
