package ncall

import (
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
)

const ProtoCapabilitiesShow = contractprotoid.ProtoCapabilitiesShow

const (
	ProtoBroadcastListenCycle      = contractprotoid.ProtoBroadcastV1ListenCycle
	ProtoBroadcastDemandPublish    = contractprotoid.ProtoBroadcastV1DemandPublish
	ProtoBroadcastDemandPublishBatch = contractprotoid.ProtoBroadcastV1DemandPublishBatch
	ProtoBroadcastLiveDemandPublish = contractprotoid.ProtoBroadcastV1LiveDemandPublish
)

const (
	ProtoDomainPricing        = contractprotoid.ProtoDomainPricing
	ProtoDomainListOwned     = contractprotoid.ProtoDomainListOwned
	ProtoDomainQueryNamePaid   = contractprotoid.ProtoDomainQueryNamePaid
	ProtoDomainResolveNamePaid  = contractprotoid.ProtoDomainResolveNamePaid
	ProtoDomainRegisterLock     = contractprotoid.ProtoDomainRegisterLock
	ProtoDomainRegisterSubmit   = contractprotoid.ProtoDomainRegisterSubmit
	ProtoDomainSetTargetPaid    = contractprotoid.ProtoDomainSetTargetPaid
)

const (
	PaymentSchemePool2of2V1 = contractprotoid.PaymentSchemePool2of2V1
	PaymentSchemeChainTxV1  = contractprotoid.PaymentSchemeChainTxV1
)

const (
	// RoutePoolV1* 是兼容层导出，不作为新业务常量。
	// 2026-04 硬切后主链路已迁移到 contractprotoid.ProtoPoolV1*。
	RoutePoolV1Info         = string(contractroute.RoutePoolV1Info)
	RoutePoolV1Create       = string(contractroute.RoutePoolV1Create)
	RoutePoolV1BaseTx       = string(contractroute.RoutePoolV1BaseTx)
	RoutePoolV1PayConfirm   = string(contractroute.RoutePoolV1PayConfirm)
	RoutePoolV1Close        = string(contractroute.RoutePoolV1Close)
	RoutePoolV1SessionState = string(contractroute.RoutePoolV1SessionState)
)

const (
	ProtoBroadcastV1NodeReachabilityAnnounce = contractprotoid.ProtoBroadcastV1NodeReachabilityAnnounce
	ProtoBroadcastV1NodeReachabilityQuery    = contractprotoid.ProtoBroadcastV1NodeReachabilityQuery
)
