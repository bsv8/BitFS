package pool_2of2_v1

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

	paymentHost, ok := host.(interface {
		RegisterQuotedServicePayer(scheme string, executor moduleapi.QuotedServicePayer) (func(), error)
		RegisterServiceCoverageSession(scheme string, executor moduleapi.ServiceCoverageSession) (func(), error)
	})
	if !ok {
		cleanup()
		return nil, fmt.Errorf("host does not implement payment registry interface")
	}

	cleanups := []func(){cleanup}

	qpCleanup, err := paymentHost.RegisterQuotedServicePayer(contractprotoid.PaymentSchemePool2of2V1, svc)
	if err != nil {
		for _, c := range cleanups {
			c()
		}
		return nil, fmt.Errorf("failed to register QuotedServicePayer: %w", err)
	}
	cleanups = append(cleanups, qpCleanup)

	scCleanup, err := paymentHost.RegisterServiceCoverageSession(contractprotoid.PaymentSchemePool2of2V1, svc)
	if err != nil {
		for _, c := range cleanups {
			c()
		}
		return nil, fmt.Errorf("failed to register ServiceCoverageSession: %w", err)
	}
	cleanups = append(cleanups, scCleanup)

	return func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			if cleanups[i] != nil {
				cleanups[i]()
			}
		}
	}, nil
}