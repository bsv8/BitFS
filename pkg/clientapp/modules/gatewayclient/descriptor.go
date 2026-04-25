package gatewayclient

import (
	"context"
	"database/sql"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient/storedb"
)

const ModuleIdentity = "gatewayclient"
const ModuleCapabilityVersion = 1

func Descriptor() moduleapi.ModuleDescriptor {
	return moduleapi.ModuleDescriptor{
		Name:         ModuleIdentity,
		SchemaOwner:  ModuleIdentity,
		EnsureSchema: ensureSchema,
		Install:      Install,
		StoreFactory: NewGatewayClientStore,
	}
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	return storedb.EnsureGatewayClientSchema(ctx, db)
}
