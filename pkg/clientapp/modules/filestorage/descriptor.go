package filestorage

import (
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	filestoragedb "github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb"
)

func Descriptor() moduleapi.ModuleDescriptor {
	return moduleapi.ModuleDescriptor{
		Name:         ModuleIdentity,
		SchemaOwner:   ModuleIdentity,
		EnsureSchema:  filestoragedb.EnsureSchema,
		Install:       Install,
	}
}
