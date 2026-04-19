package clientapp

import (
	"context"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	domainmodule "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
	inboxmessage "github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage"
	indexresolve "github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

// builtinModules 返回当前发行版默认启用的模块顺序。
//
// 设计说明：
// - 这是唯一的模块插拔清单；
// - 需要增删模块时，只改这里，不再用 tag 分叉；
// - 顺序就是依赖顺序，依赖关系不做自动拓扑排序。
func builtinModules() []moduleapi.Installer {
	return []moduleapi.Installer{
		domainmodule.Install,
		indexresolve.Install,
		inboxmessage.Install,
	}
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
	modules := builtinModules()
	cleanups := make([]func(), 0, len(modules))
	for _, installer := range modules {
		cleanup, err := installer(ctx, host)
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
