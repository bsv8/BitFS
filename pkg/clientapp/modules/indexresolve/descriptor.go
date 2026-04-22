package indexresolve

import (
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	indexresolvedb "github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve/storedb"
)

func Descriptor() moduleapi.ModuleDescriptor {
	return moduleapi.ModuleDescriptor{
		Name:               ModuleIdentity,
		SchemaOwner:        ModuleIdentity,
		EnsureSchema:       indexresolvedb.EnsureSchema,
		Install:            Install,
		ModuleLockName:     ModuleIdentity,
		ModuleLockProvider: FunctionLocks,
	}
}
