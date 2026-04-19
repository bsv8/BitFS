package indexresolve

import (
	"context"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func resolveIndexRoute(store indexResolveModuleStore) moduleapi.LibP2PHook {
	return func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		manifest, err := BizResolve(ctx, store, ev.Request.Route)
		if err != nil {
			return moduleapi.LibP2PResult{}, toModuleAPIError(err)
		}
		return moduleapi.LibP2PResult{
			ResolveManifest: toModuleResolveManifest(manifest),
		}, nil
	}
}

func toModuleResolveManifest(m Manifest) moduleapi.ResolveManifest {
	return moduleapi.ResolveManifest{
		Route:               m.Route,
		SeedHash:            m.SeedHash,
		RecommendedFileName: m.RecommendedFileName,
		MIMEHint:            m.MIMEHint,
		FileSize:            m.FileSize,
		UpdatedAtUnix:       m.UpdatedAtUnix,
	}
}
