package indexresolve

import (
	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
)

const (
	ModuleID          = "index_resolve"
	ModuleIdentity    = "indexresolve"
	ModuleVersion     = "v1"
	CapabilityVersion = 1
	DefaultRouteKey   = "index"
)

// Spec 描述 index resolve 模块的固定身份。
// 这里把能力名和模块版本绑死，避免后面再出现分叉口径。
type Spec struct {
	ID                string
	Version           string
	CapabilityID      string
	CapabilityVersion int
}

var ModuleSpec = Spec{
	ID:                ModuleID,
	Version:           ModuleVersion,
	CapabilityID:      ModuleID,
	CapabilityVersion: CapabilityVersion,
}

func CapabilityItem() *contractmessage.CapabilityItem {
	return &contractmessage.CapabilityItem{
		ID:         ModuleSpec.CapabilityID,
		Version:    uint32(ModuleSpec.CapabilityVersion),
		ProtocolID: string(contractprotoid.ProtoIndexResolve),
	}
}
