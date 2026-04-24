package pool_2of3_v1

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

	storeInterface, ok := host.(interface{ GetModuleStore(name string) any }).GetModuleStore(ModuleIdentity).(Store)
	if !ok || storeInterface == nil {
		return nil, fmt.Errorf("module store %s is not available", ModuleIdentity)
	}

	svc := newService(host, storeInterface)

	cleanup, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleIdentity,
		Version: ModuleCapabilityVersion,
	})
	if err != nil {
		return nil, err
	}

	paymentHost, ok := host.(interface {
		RegisterTradeSession(scheme string, executor moduleapi.TradeSession) (func(), error)
	})
	if !ok {
		cleanup()
		return nil, fmt.Errorf("host does not implement RegisterTradeSession")
	}

	tsCleanup, err := paymentHost.RegisterTradeSession(ModuleIdentity, svc)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to register TradeSession: %w", err)
	}

	return func() {
		tsCleanup()
		cleanup()
	}, nil
}
