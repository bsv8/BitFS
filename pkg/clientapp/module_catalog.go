package clientapp

import (
	"context"
	"fmt"
	"database/sql"
	"sort"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb"
	domainmodule "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
	filestorage "github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage"
	inboxmessage "github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage"
	indexresolve "github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

func builtinModuleCatalog() []moduleapi.ModuleDescriptor {
	return []moduleapi.ModuleDescriptor{
		domainmodule.Descriptor(),
		indexresolve.Descriptor(),
		filestorage.Descriptor(),
		inboxmessage.Descriptor(),
	}
}

func ensureCoreSchema(ctx context.Context, db *sql.DB) error {
	return coredb.EnsureSchema(ctx, db)
}

func ensureBuiltinModuleSchemas(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	for _, entry := range builtinModuleCatalog() {
		if entry.EnsureSchema == nil {
			if entry.SchemaOwner != "" {
				return fmt.Errorf("module %s schema owner %q has no ensure hook", entry.Name, entry.SchemaOwner)
			}
			continue
		}
		if strings.TrimSpace(entry.SchemaOwner) == "" {
			return fmt.Errorf("module %s schema owner is required", entry.Name)
		}
		if err := entry.EnsureSchema(ctx, db); err != nil {
			return fmt.Errorf("module %s schema ensure failed: %w", entry.Name, err)
		}
	}
	return nil
}

// EnsureCoreSchema 只给外层装配层和测试用。
func EnsureCoreSchema(ctx context.Context, db *sql.DB) error {
	return ensureCoreSchema(ctx, db)
}

// EnsureBuiltinModuleSchemas 只给外层装配层和测试用。
func EnsureBuiltinModuleSchemas(ctx context.Context, db *sql.DB) error {
	return ensureBuiltinModuleSchemas(ctx, db)
}

func BuiltinModuleLockModules() []string {
	var out []string
	for _, entry := range builtinModuleCatalog() {
		if entry.ModuleLockProvider != nil {
			out = append(out, entry.Name)
		}
	}
	sort.Strings(out)
	return out
}

func BuiltinModuleLockItems(modules ...string) ([]moduleapi.LockedFunction, []string, error) {
	if err := ValidateBuiltinModuleLockDescriptors(); err != nil {
		return nil, nil, err
	}

	var all []moduleapi.LockedFunction
	var missing []string

	hasFilter := len(modules) > 0
	moduleSet := make(map[string]struct{})
	for _, m := range modules {
		moduleSet[m] = struct{}{}
	}

	for _, entry := range builtinModuleCatalog() {
		if hasFilter {
			if _, ok := moduleSet[entry.Name]; !ok {
				continue
			}
		}
		if entry.ModuleLockProvider == nil {
			if hasFilter {
				missing = append(missing, entry.Name)
			}
			continue
		}
		lockName := entry.ModuleLockName
		if lockName == "" {
			lockName = entry.Name
		}
		items := entry.ModuleLockProvider()
		for _, item := range items {
			itemModule := strings.TrimSpace(item.Module)
			if itemModule != lockName {
				return nil, nil, fmt.Errorf("descriptor %s: item.Module=%q != lockName=%q", entry.Name, itemModule, lockName)
			}
			all = append(all, item)
		}
	}

	if hasFilter {
		for _, m := range modules {
			found := false
			for _, entry := range builtinModuleCatalog() {
				if entry.Name == m {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, m)
			}
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Module != all[j].Module {
			return all[i].Module < all[j].Module
		}
		return all[i].ID < all[j].ID
	})

	return all, missing, nil
}

func ValidateBuiltinModuleLockDescriptors() error {
	for _, entry := range builtinModuleCatalog() {
		if entry.ModuleLockProvider == nil {
			continue
		}
		lockName := entry.ModuleLockName
		if lockName == "" {
			lockName = entry.Name
		}
		items := entry.ModuleLockProvider()
		for _, item := range items {
			itemModule := strings.TrimSpace(item.Module)
			if itemModule != lockName {
				return fmt.Errorf("descriptor %s: item.Module=%q != lockName=%q", entry.Name, itemModule, lockName)
			}
		}
	}
	return nil
}

func toModuleLockProvider(provider func() []moduleapi.LockedFunction) modulelock.Provider {
	return func() []modulelock.LockedFunction {
		items := provider()
		out := make([]modulelock.LockedFunction, 0, len(items))
		for _, item := range items {
			out = append(out, modulelock.LockedFunction{
				ID:               strings.TrimSpace(item.ID),
				Module:           strings.TrimSpace(item.Module),
				Package:          strings.TrimSpace(item.Package),
				Symbol:           strings.TrimSpace(item.Symbol),
				Signature:        strings.TrimSpace(item.Signature),
				ObsControlAction: strings.TrimSpace(item.ObsControlAction),
				Note:             strings.TrimSpace(item.Note),
			})
		}
		return out
	}
}

func installBuiltinModules(ctx context.Context, rt *Runtime, store moduleBootstrapStore) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}

	host := newModuleHost(rt, store)
	catalog := builtinModuleCatalog()
	cleanups := make([]func(), 0, len(catalog))

	for _, entry := range catalog {
		if entry.Install == nil {
			closeInstalledModules(cleanups)
			return nil, fmt.Errorf("module %s installer is required", entry.Name)
		}

		if entry.ModuleLockProvider != nil {
			lockName := entry.ModuleLockName
			if lockName == "" {
				lockName = entry.Name
			}
			cleanup, err := rt.modules.RegisterModuleLockProvider(lockName, toModuleLockProvider(entry.ModuleLockProvider))
			if err != nil {
				closeInstalledModules(cleanups)
				return nil, err
			}
			cleanups = append(cleanups, cleanup)
		}

		cleanup, err := entry.Install(ctx, host)
		if err != nil {
			closeInstalledModules(cleanups)
			return nil, err
		}
		if cleanup != nil {
			cleanups = append(cleanups, cleanup)
		}
	}

	return func() {
		closeInstalledModules(cleanups)
	}, nil
}

func closeInstalledModules(cleanups []func()) {
	for i := len(cleanups) - 1; i >= 0; i-- {
		if cleanups[i] != nil {
			cleanups[i]()
		}
	}
}
