package inboxmessage

import (
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	inboxmessagedb "github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage/storedb"
)

func Descriptor() moduleapi.ModuleDescriptor {
	return moduleapi.ModuleDescriptor{
		Name:         ModuleIdentity,
		SchemaOwner:   ModuleIdentity,
		EnsureSchema:  inboxmessagedb.EnsureSchema,
		Install:       Install,
	}
}
