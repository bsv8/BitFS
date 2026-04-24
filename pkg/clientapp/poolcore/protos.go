package poolcore

import "github.com/libp2p/go-libp2p/core/protocol"

const (
	// 费用池参数查询（握手参数下发）。
	ProtoFeePoolInfo protocol.ID = "/bsv-transfer/fee_pool/info/1.0.0"

	// Open 阶段：Create + BaseTx。
	ProtoFeePoolCreate protocol.ID = "/bsv-transfer/fee_pool/create/1.0.0"
	ProtoFeePoolBaseTx protocol.ID = "/bsv-transfer/fee_pool/base_tx/1.0.0"

	// Pay 阶段：更新 spend tx（不上链）。
	ProtoFeePoolPayConfirm protocol.ID = "/bsv-transfer/fee_pool/pay_confirm/1.0.0"

	// Close 阶段：最终结算并广播 final tx。
	ProtoFeePoolClose protocol.ID = "/bsv-transfer/fee_pool/close/1.0.0"

	// 状态查询（用于观测/e2e）。
	ProtoFeePoolSessionState protocol.ID = "/bsv-transfer/fee_pool/state/1.0.0"
)
