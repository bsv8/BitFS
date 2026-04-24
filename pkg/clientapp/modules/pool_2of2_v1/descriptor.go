package pool_2of2_v1

import (
	"context"
	"database/sql"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of2_v1/storedb"
)

const ModuleIdentity = "pool_2of2_v1"
const ModuleCapabilityVersion = 1

func Descriptor() moduleapi.ModuleDescriptor {
	return moduleapi.ModuleDescriptor{
		Name:         ModuleIdentity,
		SchemaOwner:  ModuleIdentity,
		EnsureSchema: ensureSchema,
		Install:      Install,
		StoreFactory: newStore,
	}
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	return storedb.EnsurePool2of2V1Schema(ctx, db)
}

func newStore(store moduleapi.Store) any {
	return newPoolStore(store)
}
