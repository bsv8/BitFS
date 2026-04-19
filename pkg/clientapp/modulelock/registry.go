package modulelock

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Registry 收集模块白名单提供者。
//
// 设计说明：
// - 只登记“模块 -> 提供者”这一层，不把白名单拼成大结构体；
// - 需要关闭时返回 cleanup，避免测试里污染后续注册；
// - 冻结项按模块分批拿，便于按模块过滤检查。
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

func NewRegistry() *Registry {
	return &Registry{
		providers: map[string]Provider{},
	}
}

func (r *Registry) Register(module string, provider Provider) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	module = strings.TrimSpace(module)
	if module == "" {
		return nil, fmt.Errorf("module is required")
	}
	if provider == nil {
		return nil, fmt.Errorf("provider is required")
	}
	r.mu.Lock()
	if r.providers == nil {
		r.providers = map[string]Provider{}
	}
	if _, ok := r.providers[module]; ok {
		r.mu.Unlock()
		return nil, fmt.Errorf("module is already registered: %s", module)
	}
	r.providers[module] = provider
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			delete(r.providers, module)
			r.mu.Unlock()
		})
	}, nil
}

func (r *Registry) Modules() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.providers))
	for module := range r.providers {
		out = append(out, module)
	}
	sort.Strings(out)
	return out
}

func (r *Registry) Items(modules ...string) ([]LockedFunction, []string) {
	if r == nil {
		return nil, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	selected := map[string]struct{}{}
	for _, module := range modules {
		module = strings.TrimSpace(module)
		if module == "" {
			continue
		}
		selected[module] = struct{}{}
	}
	if len(selected) == 0 {
		for module := range r.providers {
			selected[module] = struct{}{}
		}
	}

	var out []LockedFunction
	var missing []string
	keys := make([]string, 0, len(selected))
	for module := range selected {
		keys = append(keys, module)
	}
	sort.Strings(keys)
	for _, module := range keys {
		provider, ok := r.providers[module]
		if !ok {
			missing = append(missing, module)
			continue
		}
		items := provider()
		for _, item := range items {
			out = append(out, item)
		}
	}
	sort.Strings(missing)
	return out, missing
}
