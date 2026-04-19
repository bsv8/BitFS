package clientapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

type moduleRegistry struct {
	mu sync.RWMutex

	indexResolveCapabilityHook func() *contractmessage.CapabilityItem
	indexResolveResolveHook    func(context.Context, string) (routeIndexManifest, error)
	indexResolveSettingsHook   func(*httpAPIServer, *http.ServeMux, string)
	indexResolveCloseHook      func()
	moduleLocks                *modulelock.Registry
}

func newModuleRegistry() *moduleRegistry {
	return &moduleRegistry{
		moduleLocks: modulelock.NewRegistry(),
	}
}

func (r *moduleRegistry) registerIndexResolve(
	capabilityHook func() *contractmessage.CapabilityItem,
	resolveHook func(context.Context, string) (routeIndexManifest, error),
	settingsHook func(*httpAPIServer, *http.ServeMux, string),
	closeHook func(),
) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	r.mu.Lock()
	if r.indexResolveCapabilityHook != nil || r.indexResolveResolveHook != nil || r.indexResolveSettingsHook != nil || r.indexResolveCloseHook != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("index_resolve module is already registered")
	}
	r.indexResolveCapabilityHook = capabilityHook
	r.indexResolveResolveHook = resolveHook
	r.indexResolveSettingsHook = settingsHook
	r.indexResolveCloseHook = closeHook
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			if r.indexResolveCloseHook != nil {
				r.indexResolveCloseHook()
			}
			r.indexResolveCapabilityHook = nil
			r.indexResolveResolveHook = nil
			r.indexResolveSettingsHook = nil
			r.indexResolveCloseHook = nil
			r.mu.Unlock()
		})
	}, nil
}

func (r *moduleRegistry) capabilityItems() []*contractmessage.CapabilityItem {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.indexResolveCapabilityHook == nil {
		return nil
	}
	item := r.indexResolveCapabilityHook()
	if item != nil {
		return []*contractmessage.CapabilityItem{item}
	}
	return nil
}

func (r *moduleRegistry) registerModuleLockProvider(module string, provider modulelock.Provider) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	if r.moduleLocks == nil {
		r.moduleLocks = modulelock.NewRegistry()
	}
	return r.moduleLocks.Register(module, provider)
}

func (r *moduleRegistry) moduleLockItems(modules ...string) ([]modulelock.LockedFunction, []string) {
	if r == nil || r.moduleLocks == nil {
		return nil, nil
	}
	return r.moduleLocks.Items(modules...)
}

func ensureModuleRegistry(rt *Runtime) *moduleRegistry {
	if rt == nil {
		return nil
	}
	if rt.modules == nil {
		rt.modules = newModuleRegistry()
	}
	return rt.modules
}

func (r *Runtime) NodePubkeyHex() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.ClientID())
}

func clientCapabilitiesShowBody(rt *Runtime) contractmessage.CapabilitiesShowBody {
	caps := []*contractmessage.CapabilityItem{
		{ID: "wallet", Version: 1},
		{ID: "bitfs", Version: 1},
	}
	if rt != nil && rt.modules != nil {
		caps = append(caps, rt.modules.capabilityItems()...)
	}
	return contractmessage.CapabilitiesShowBody{
		NodePubkeyHex: runtimeNodePubkeyHex(rt),
		Capabilities:  caps,
	}
}

func runtimeNodePubkeyHex(rt *Runtime) string {
	if rt == nil {
		return ""
	}
	return strings.TrimSpace(rt.ClientID())
}

func (r *moduleRegistry) resolveIndex(ctx context.Context, route string) (routeIndexManifest, error) {
	if r == nil {
		return routeIndexManifest{}, newModuleHookError("MODULE_DISABLED", "index_resolve module is disabled")
	}
	r.mu.RLock()
	hook := r.indexResolveResolveHook
	r.mu.RUnlock()
	if hook == nil {
		return routeIndexManifest{}, newModuleHookError("MODULE_DISABLED", "index_resolve module is disabled")
	}
	// 只读边界：resolve 只能走模块查询钩子，不能顺手碰 settings 挂载。
	return hook(ctx, route)
}

func (r *moduleRegistry) mountIndexResolveSettingsRoutes(s *httpAPIServer, mux *http.ServeMux, prefix string) {
	if r == nil || s == nil || mux == nil {
		return
	}
	r.mu.RLock()
	hook := r.indexResolveSettingsHook
	r.mu.RUnlock()
	if hook == nil {
		return
	}
	hook(s, mux, prefix)
}

type moduleHookError struct {
	code    string
	message string
}

func (e *moduleHookError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.message)
}

func newModuleHookError(code, message string) error {
	return &moduleHookError{
		code:    strings.TrimSpace(code),
		message: strings.TrimSpace(message),
	}
}

func moduleHookCode(err error) string {
	var typed *moduleHookError
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.code)
	}
	return ""
}

func moduleHookMessage(err error) string {
	var typed *moduleHookError
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.message)
	}
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}
