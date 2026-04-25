package domainclient

import (
	"context"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type backendProvider interface {
	DomainClientBackend() Backend
}

func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}

	provider, ok := host.(backendProvider)
	if !ok {
		return nil, fmt.Errorf("host does not provide domainclient backend")
	}
	backend := provider.DomainClientBackend()
	if backend == nil {
		return nil, fmt.Errorf("domainclient backend is required")
	}

	svc := newService(backend)
	return host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleID,
		Version: CapabilityVersion,
		DomainResolvers: []moduleapi.DomainResolver{
			{
				Name: ResolveProviderName,
				Handler: func(ctx context.Context, rawDomain string) (string, error) {
					return svc.resolveDomainRemote(ctx, rawDomain)
				},
			},
		},
		HTTP: svc.httpRoutes(),
		Settings: svc.settingsActions(),
		OBS: svc.obsActions(),
	})
}
