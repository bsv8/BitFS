package gatewayclient

import (
	"context"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}

	storeGetter, ok := host.(interface{ GetModuleStore(name string) any })
	if !ok {
		return nil, fmt.Errorf("host does not expose module store registry")
	}
	storeAny := storeGetter.GetModuleStore(ModuleIdentity)
	storeInterface, ok := storeAny.(Store)
	if !ok || storeInterface == nil {
		return nil, fmt.Errorf("module store %s is not available", ModuleIdentity)
	}

	svc := NewService(host, storeInterface)

	cleanup, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleIdentity,
		Version: ModuleCapabilityVersion,
		HTTP:    svc.httpRoutes(),
		Settings: svc.settingsActions(),
		OBS:     svc.obsActions(),
		OpenHooks: svc.openHooks(),
	})
	if err != nil {
		return nil, err
	}

	return func() {
		cleanup()
	}, nil
}
