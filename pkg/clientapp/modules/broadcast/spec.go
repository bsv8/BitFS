package broadcast

import (
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/caps"
)

const (
	InternalAbilityID = "bftp.broadcast@1"
	PublicCapabilityID = "broadcast"
	Version            = uint32(1)
)

func Spec() caps.ModuleSpec {
	return caps.ModuleSpec{
		InternalAbility: InternalAbilityID,
		Capabilities: []caps.PublicCapability{
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoBroadcastV1ListenCycle)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoBroadcastV1DemandPublish)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoBroadcastV1DemandPublishBatch)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoBroadcastV1LiveDemandPublish)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoBroadcastV1NodeReachabilityAnnounce)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoBroadcastV1NodeReachabilityQuery)},
		},
		Protos: []string{
			string(contractprotoid.ProtoBroadcastV1ListenCycle),
			string(contractprotoid.ProtoBroadcastV1DemandPublish),
			string(contractprotoid.ProtoBroadcastV1DemandPublishBatch),
			string(contractprotoid.ProtoBroadcastV1LiveDemandPublish),
			string(contractprotoid.ProtoBroadcastV1NodeReachabilityAnnounce),
			string(contractprotoid.ProtoBroadcastV1NodeReachabilityQuery),
		},
	}
}
