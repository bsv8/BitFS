package filestorage

import "github.com/bsv8/BitFS/pkg/clientapp/moduleapi"

func Descriptor() moduleapi.ModuleDescriptor {
	return moduleapi.ModuleDescriptor{
		Name:    ModuleIdentity,
		Install: Install,
	}
}
