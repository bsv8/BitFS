package broadcast

import contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"

const (
	RouteBroadcastV1ListenCycle              = string(contractroute.RouteBroadcastV1ListenCycle)
	RouteBroadcastV1DemandPublish            = string(contractroute.RouteBroadcastV1DemandPublish)
	RouteBroadcastV1DemandPublishBatch       = string(contractroute.RouteBroadcastV1DemandPublishBatch)
	RouteBroadcastV1LiveDemandPublish        = string(contractroute.RouteBroadcastV1LiveDemandPublish)
	RouteBroadcastV1NodeReachabilityAnnounce = string(contractroute.RouteBroadcastV1NodeReachabilityAnnounce)
	RouteBroadcastV1NodeReachabilityQuery    = string(contractroute.RouteBroadcastV1NodeReachabilityQuery)
)
