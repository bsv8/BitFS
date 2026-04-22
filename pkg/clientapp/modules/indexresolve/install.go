package indexresolve

import (
	"context"
	"fmt"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type indexResolveStoreAdapter struct {
	store moduleapi.Store
}

func (a indexResolveStoreAdapter) Read(ctx context.Context, fn func(moduleapi.ReadConn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is nil")
	}
	return a.store.Read(ctx, fn)
}

func (a indexResolveStoreAdapter) WriteTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	if a.store == nil {
		return fmt.Errorf("store is nil")
	}
	return a.store.WriteTx(ctx, fn)
}

func openIndexResolveStore(ctx context.Context, host moduleapi.Host) (*dbStore, error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}
	store := host.Store()
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}

	db := indexResolveStoreAdapter{store: store}

	return BootstrapStore(ctx, db)
}

func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	store, err := openIndexResolveStore(ctx, host)
	if err != nil {
		return nil, err
	}

	return host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleID,
		Version: CapabilityVersion,
		Capabilities: []moduleapi.Capability{
			{ID: ModuleID, Version: uint32(CapabilityVersion), ProtocolID: contractprotoid.ProtoIndexResolve},
		},
		LibP2P: []moduleapi.LibP2PRoute{
			{ProtocolID: contractprotoid.ProtoIndexResolve, Handler: handleIndexResolve(store)},
		},
		Settings: indexResolveSettingsActions(store),
		OBS:      indexResolveOBSActions(store),
		HTTP: []moduleapi.HTTPRoute{
			{Path: "/v1/settings/index-resolve", Handler: handleIndexResolveSettings(store)},
		},
	})
}