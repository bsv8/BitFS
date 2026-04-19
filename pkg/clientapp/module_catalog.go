package clientapp

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	domainmodule "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
	inboxmessage "github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage"
	indexresolve "github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

// builtinModuleEntry 描述一个内置模块的元信息。
type builtinModuleEntry struct {
	Name               string
	Install            moduleapi.Installer
	ModuleLockName     string
	ModuleLockProvider func() []moduleapi.LockedFunction
}

// builtinModuleCatalog 返回当前发行版默认启用的模块顺序。
//
// 设计说明：
// - 这是唯一的模块插拔清单，也是唯一的模块锁项挂载清单；
// - 需要增删模块时，只改这里，不再用 tag 分叉；
// - 顺序就是依赖顺序，依赖关系不做自动拓扑排序；
// - 有 ModuleLockProvider 的模块会提供锁项，供工具静态检查使用。
func builtinModuleCatalog() []builtinModuleEntry {
	return []builtinModuleEntry{
		{
			Name:    domainmodule.ModuleIdentity,
			Install: domainmodule.Install,
		},
		{
			Name:               indexresolve.ModuleIdentity,
			Install:            indexresolve.Install,
			ModuleLockName:     indexresolve.ModuleIdentity,
			ModuleLockProvider: indexresolve.FunctionLocks,
		},
		{
			Name:    inboxmessage.ModuleIdentity,
			Install: inboxmessage.Install,
		},
	}
}

// BuiltinModuleLockModules 返回当前 catalog 里有锁项的模块名，排序后返回。
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

// BuiltinModuleLockItems 返回指定模块的锁项。
//
// 参数：
//   - modules 为空时返回所有启用模块的锁项；
//   - 指定模块不存在或无锁项时放进 missing；
//   - 返回项按 Module + ID 排序，保证检查输出稳定。
func BuiltinModuleLockItems(modules ...string) ([]moduleapi.LockedFunction, []string) {
	var all []moduleapi.LockedFunction
	var missing []string

	hasFilter := len(modules) > 0
	moduleSet := make(map[string]struct{})
	for _, m := range modules {
		moduleSet[m] = struct{}{}
	}

	moduleToLockName := make(map[string]string)
	for _, entry := range builtinModuleCatalog() {
		if entry.ModuleLockProvider != nil {
			moduleToLockName[entry.Name] = entry.ModuleLockName
		}
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
		items := entry.ModuleLockProvider()
		all = append(all, items...)
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

	return all, missing
}

// installBuiltinModules 统一安装默认模块，并在失败时倒序回收。
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
