package chain_tx_v1

import (
	"context"
	"fmt"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
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

	registry, ok := host.(interface {
		RegisterQuotedServicePayer(scheme string, executor moduleapi.QuotedServicePayer) (func(), error)
	})
	if !ok {
		cleanup()
		return nil, fmt.Errorf("host does not implement RegisterQuotedServicePayer")
	}

	paymentCleanup, err := registry.RegisterQuotedServicePayer(contractprotoid.PaymentSchemeChainTxV1, svc)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to register QuotedServicePayer: %w", err)
	}

	return func() {
		paymentCleanup()
		cleanup()
	}, nil
}
