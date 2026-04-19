package clientapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

// OBSActionResponse 是通用 obs 动作的返回壳。
//
// 设计说明：
// - 渠道层只认这一层，不直接碰模块实现细节；
// - 成功时写 OK/Result/Payload，失败时由上层统一转成 command result；
// - Payload 只放业务结果，不塞控制面状态，避免把两层搅在一起。
type OBSActionResponse struct {
	OK      bool
	Result  string
	Error   string
	Payload map[string]any
}

type LibP2PProtocol string
type LibP2PHook func(context.Context, LibP2PEvent) (LibP2PResult, error)

const (
	LibP2PProtocolNodeCall    LibP2PProtocol = "node.call"
	LibP2PProtocolNodeResolve LibP2PProtocol = "node.resolve"
)

type LibP2PEvent struct {
	Protocol    LibP2PProtocol
	Route       string
	Meta        ncall.CallContext
	Req         ncall.CallReq
	ContentType string
}

type LibP2PResult struct {
	CallResp        ncall.CallResp
	ResolveManifest routeIndexManifest
}
type HTTPAPIHook func(*httpAPIServer, *http.ServeMux, string)
type SettingsHook func(context.Context, string, map[string]any) (map[string]any, error)
type OBSControlHook func(context.Context, string, map[string]any) (OBSActionResponse, error)
type OpenHook func(context.Context) error
type CloseHook func(context.Context) error

// ModuleHooks 暴露统一的模块钩子能力。
//
// 设计说明：
// - 外层只拿分发口和注册口，不拿模块内部状态；
// - 生产代码只走 Dispatch* / Run*，注册只在单点装配里出现；
// - 测试仍然可以用注册口挂入假钩子，验证分发是否只靠注册表。
type ModuleHooks interface {
	RegisterLibP2PHook(LibP2PProtocol, string, LibP2PHook) (func(), error)
	RegisterHTTPAPIHook(HTTPAPIHook) (func(), error)
	RegisterSettingsHook(SettingsHook) (func(), error)
	RegisterOBSControlHook(OBSControlHook) (func(), error)
	RegisterOpenHook(OpenHook) (func(), error)
	RegisterCloseHook(CloseHook) (func(), error)
	RegisterModuleLockProvider(string, modulelock.Provider) (func(), error)

	DispatchLibP2P(context.Context, LibP2PEvent) (LibP2PResult, error)
	MountHTTPAPI(*httpAPIServer, *http.ServeMux, string)
	DispatchSettings(context.Context, string, map[string]any) (map[string]any, error)
	DispatchOBSControl(context.Context, string, map[string]any) (OBSActionResponse, error)
	RunOpenHooks(context.Context) error
	RunCloseHooks(context.Context) error
}

type lifecycleHookEntry struct {
	id   uint64
	hook func(context.Context) error
}

type capabilityHookEntry struct {
	id   uint64
	hook func() *contractmessage.CapabilityItem
}

type httpAPIHookEntry struct {
	id   uint64
	hook HTTPAPIHook
}

type moduleRegistry struct {
	mu sync.RWMutex

	capabilityHooks []capabilityHookEntry

	libP2PHooks  map[string]LibP2PHook
	httpAPIHooks []httpAPIHookEntry
	settingsHook SettingsHook
	obsHook      OBSControlHook
	openHooks    []lifecycleHookEntry
	closeHooks   []lifecycleHookEntry
	nextHookID   uint64
	moduleLocks  *modulelock.Registry
}

func newModuleRegistry() *moduleRegistry {
	return &moduleRegistry{
		moduleLocks: modulelock.NewRegistry(),
	}
}

func (r *moduleRegistry) registerCapabilityHook(hook func() *contractmessage.CapabilityItem) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	r.mu.Lock()
	r.nextHookID++
	id := r.nextHookID
	r.capabilityHooks = append(r.capabilityHooks, capabilityHookEntry{id: id, hook: hook})
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			for i, entry := range r.capabilityHooks {
				if entry.id == id {
					r.capabilityHooks = append(r.capabilityHooks[:i], r.capabilityHooks[i+1:]...)
					break
				}
			}
			r.mu.Unlock()
		})
	}, nil
}

func (r *moduleRegistry) RegisterLibP2PHook(protocol LibP2PProtocol, route string, hook LibP2PHook) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	protocol = normalizeLibP2PProtocol(protocol)
	if protocol == "" {
		return nil, fmt.Errorf("protocol is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	route = strings.TrimSpace(route)
	if protocol == LibP2PProtocolNodeCall && strings.TrimSpace(route) == "" {
		return nil, fmt.Errorf("route is required")
	}
	key := libP2PHookKey(protocol, route)
	r.mu.Lock()
	if r.libP2PHooks == nil {
		r.libP2PHooks = make(map[string]LibP2PHook)
	}
	if _, exists := r.libP2PHooks[key]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("libp2p hook already registered for protocol=%s route=%q", protocol, route)
	}
	r.libP2PHooks[key] = hook
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			delete(r.libP2PHooks, key)
			r.mu.Unlock()
		})
	}, nil
}

func (r *moduleRegistry) RegisterHTTPAPIHook(hook HTTPAPIHook) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	r.mu.Lock()
	r.nextHookID++
	id := r.nextHookID
	r.httpAPIHooks = append(r.httpAPIHooks, httpAPIHookEntry{id: id, hook: hook})
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			for i, entry := range r.httpAPIHooks {
				if entry.id == id {
					r.httpAPIHooks = append(r.httpAPIHooks[:i], r.httpAPIHooks[i+1:]...)
					break
				}
			}
			r.mu.Unlock()
		})
	}, nil
}

func (r *moduleRegistry) RegisterSettingsHook(hook SettingsHook) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	r.mu.Lock()
	if r.settingsHook != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("settings hook is already registered")
	}
	r.settingsHook = hook
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			r.settingsHook = nil
			r.mu.Unlock()
		})
	}, nil
}

func (r *moduleRegistry) RegisterOBSControlHook(hook OBSControlHook) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	r.mu.Lock()
	if r.obsHook != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("obs control hook is already registered")
	}
	r.obsHook = hook
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			r.obsHook = nil
			r.mu.Unlock()
		})
	}, nil
}

func (r *moduleRegistry) RegisterOpenHook(hook OpenHook) (func(), error) {
	return r.registerLifecycleHook(&r.openHooks, hook)
}

func (r *moduleRegistry) RegisterCloseHook(hook CloseHook) (func(), error) {
	return r.registerLifecycleHook(&r.closeHooks, hook)
}

func (r *moduleRegistry) RegisterModuleLockProvider(module string, provider modulelock.Provider) (func(), error) {
	return r.registerModuleLockProvider(module, provider)
}

func (r *moduleRegistry) registerLifecycleHook(dst *[]lifecycleHookEntry, hook func(context.Context) error) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	r.mu.Lock()
	r.nextHookID++
	entry := lifecycleHookEntry{id: r.nextHookID, hook: hook}
	*dst = append(*dst, entry)
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			items := (*dst)[:0]
			for _, item := range *dst {
				if item.id != entry.id {
					items = append(items, item)
				}
			}
			*dst = items
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
	if len(r.capabilityHooks) == 0 {
		return nil
	}
	out := make([]*contractmessage.CapabilityItem, 0, len(r.capabilityHooks))
	for _, entry := range r.capabilityHooks {
		if entry.hook != nil {
			if item := entry.hook(); item != nil {
				out = append(out, item)
			}
		}
	}
	return out
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

func (r *moduleRegistry) DispatchLibP2P(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
	if ctx == nil {
		return LibP2PResult{}, newModuleHookError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return LibP2PResult{}, newModuleHookError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if r == nil {
		return LibP2PResult{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	protocol := normalizeLibP2PProtocol(ev.Protocol)
	if protocol == "" {
		return LibP2PResult{}, newModuleHookError("BAD_REQUEST", "protocol is required")
	}
	route := ev.Route
	if protocol == LibP2PProtocolNodeCall {
		route = strings.TrimSpace(route)
		if route == "" {
			return LibP2PResult{}, newModuleHookError("BAD_REQUEST", "route is required")
		}
	}
	route = strings.TrimSpace(route)
	key := libP2PHookKey(protocol, route)
	r.mu.RLock()
	hook := r.libP2PHooks[key]
	r.mu.RUnlock()
	if hook == nil {
		return LibP2PResult{}, newModuleHookError("ROUTE_NOT_FOUND", "route not found")
	}
	out, err := hook(ctx, ev)
	if err != nil {
		return LibP2PResult{}, err
	}
	switch protocol {
	case LibP2PProtocolNodeCall:
		if !isValidLibP2PCallResult(out.CallResp) {
			return LibP2PResult{}, newModuleHookError("INTERNAL_ERROR", "call response is required")
		}
	case LibP2PProtocolNodeResolve:
		if !isValidLibP2PResolveResult(out.ResolveManifest) {
			return LibP2PResult{}, newModuleHookError("INTERNAL_ERROR", "resolve manifest is required")
		}
	}
	return out, nil
}

func (r *moduleRegistry) MountHTTPAPI(s *httpAPIServer, mux *http.ServeMux, prefix string) {
	if r == nil || s == nil || mux == nil {
		return
	}
	r.mu.RLock()
	hooks := r.httpAPIHooks
	r.mu.RUnlock()
	for _, entry := range hooks {
		if entry.hook != nil {
			entry.hook(s, mux, prefix)
		}
	}
}

func (r *moduleRegistry) DispatchSettings(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
	action = strings.TrimSpace(action)
	if ctx == nil {
		return nil, newModuleHookError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return nil, newModuleHookError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if action == "" {
		return nil, newModuleHookError("BAD_REQUEST", "action is required")
	}
	if r == nil {
		return nil, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	r.mu.RLock()
	hook := r.settingsHook
	r.mu.RUnlock()
	if hook == nil {
		return nil, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	return hook(ctx, action, payload)
}

func (r *moduleRegistry) DispatchOBSControl(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
	action = strings.TrimSpace(action)
	if ctx == nil {
		return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return OBSActionResponse{}, newModuleHookError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if action == "" {
		return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "action is required")
	}
	if r == nil {
		return OBSActionResponse{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	r.mu.RLock()
	hook := r.obsHook
	r.mu.RUnlock()
	if hook == nil {
		if r.hasRegisteredOBSControlAction(action) {
			return OBSActionResponse{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
		}
		return OBSActionResponse{}, newModuleHookError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
	}
	return hook(ctx, action, payload)
}

func (r *moduleRegistry) hasRegisteredOBSControlAction(action string) bool {
	if r == nil || r.moduleLocks == nil {
		return false
	}
	items, _ := r.moduleLocks.Items()
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.ObsControlAction) == action {
			return true
		}
	}
	return false
}

func (r *moduleRegistry) RunOpenHooks(ctx context.Context) error {
	return r.runLifecycleHooks(ctx, r.snapshotLifecycleHooks(true))
}

func (r *moduleRegistry) RunCloseHooks(ctx context.Context) error {
	return r.runLifecycleHooks(ctx, r.snapshotLifecycleHooks(false))
}

func (r *moduleRegistry) snapshotLifecycleHooks(open bool) []lifecycleHookEntry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var src []lifecycleHookEntry
	if open {
		src = r.openHooks
	} else {
		src = r.closeHooks
	}
	if len(src) == 0 {
		return nil
	}
	out := make([]lifecycleHookEntry, len(src))
	copy(out, src)
	return out
}

func (r *moduleRegistry) runLifecycleHooks(ctx context.Context, hooks []lifecycleHookEntry) error {
	if ctx == nil {
		return newModuleHookError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return newModuleHookError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if r == nil {
		return newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	if len(hooks) == 0 {
		return nil
	}
	for _, item := range hooks {
		if item.hook == nil {
			continue
		}
		if err := item.hook(ctx); err != nil {
			return err
		}
	}
	return nil
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

// Modules 只返回可转发的模块能力，不暴露内部注册表实现。
func (rt *Runtime) Modules() ModuleHooks {
	if rt == nil {
		return nil
	}
	if rt.modules == nil {
		return nil
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

// NewModuleHookError 只给外层测试和桥接层创建统一模块错误壳用。
func NewModuleHookError(code, message string) error {
	return newModuleHookError(code, message)
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

// ModuleHookCodeOf 只给外层读取通用模块错误码用。
func ModuleHookCodeOf(err error) string {
	return moduleHookCode(err)
}

// ModuleHookMessageOf 只给外层读取通用模块错误信息用。
func ModuleHookMessageOf(err error) string {
	return moduleHookMessage(err)
}

func normalizeLibP2PProtocol(protocol LibP2PProtocol) LibP2PProtocol {
	switch strings.TrimSpace(string(protocol)) {
	case string(LibP2PProtocolNodeCall):
		return LibP2PProtocolNodeCall
	case string(LibP2PProtocolNodeResolve):
		return LibP2PProtocolNodeResolve
	default:
		return ""
	}
}

func libP2PHookKey(protocol LibP2PProtocol, route string) string {
	return string(protocol) + "\n" + route
}

func isValidLibP2PCallResult(resp ncall.CallResp) bool {
	return strings.TrimSpace(resp.Code) != ""
}

func isValidLibP2PResolveResult(manifest routeIndexManifest) bool {
	return strings.TrimSpace(manifest.Route) != ""
}
