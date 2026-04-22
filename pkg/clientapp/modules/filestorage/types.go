package filestorage

import "github.com/bsv8/BitFS/pkg/clientapp/moduleapi"

const (
	ModuleIdentity    = "filestorage"
	ModuleID          = "filestorage"
	CapabilityVersion = 1
)

type storeAdapter struct {
	store moduleapi.Store
}
