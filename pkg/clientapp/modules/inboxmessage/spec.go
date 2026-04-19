package inboxmessage

import contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"

const (
	ModuleID          = "inbox_message"
	ModuleIdentity    = "inboxmessage"
	ModuleVersion     = "v1"
	CapabilityVersion = 1
	InboxMessageRoute = "inbox.message"
)

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
		ID:      ModuleSpec.CapabilityID,
		Version: uint32(ModuleSpec.CapabilityVersion),
	}
}
