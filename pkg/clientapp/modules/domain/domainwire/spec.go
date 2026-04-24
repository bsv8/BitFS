package domainwire

import (
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/caps"
)

const (
	InternalAbilityID = "bftp.domain@1"
	PublicCapabilityID = "domain"
	Version            = uint32(1)
)

func Spec() caps.ModuleSpec {
	return caps.ModuleSpec{
		InternalAbility: InternalAbilityID,
		Capabilities: []caps.PublicCapability{
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainPricing)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainResolveNamePaid)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainQueryNamePaid)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainRegisterLock)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainListOwned)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainSetTargetPaid)},
			{ID: PublicCapabilityID, Version: Version, ProtocolID: string(contractprotoid.ProtoDomainRegisterSubmit)},
		},
		Protos: []string{
			string(contractprotoid.ProtoDomainPricing),
			string(contractprotoid.ProtoDomainResolveNamePaid),
			string(contractprotoid.ProtoDomainQueryNamePaid),
			string(contractprotoid.ProtoDomainRegisterLock),
			string(contractprotoid.ProtoDomainListOwned),
			string(contractprotoid.ProtoDomainSetTargetPaid),
			string(contractprotoid.ProtoDomainRegisterSubmit),
		},
	}
}
