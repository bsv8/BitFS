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

type OBSActionHandler func(context.Context, map[string]any) (OBSActionResponse, error)

type obsActionHook struct {
	action  string
	handler OBSActionHandler
}

// OBSActionCaller 只暴露 obs 渠道需要转发的能力。
//
// 设计说明：
// - 外层只能拿能力，不拿内部注册表；
// - 这样托管进程只会拿到 obs 动作分发口，不会摸到实现细节；
// - HTTP 和 libp2p 仍然在包内走更细的解析/挂载钩子。
type OBSActionCaller interface {
	CallOBSAction(context.Context, string, map[string]any) (OBSActionResponse, error)
}

type moduleRegistry struct {
	mu sync.RWMutex

	capabilityHook func() *contractmessage.CapabilityItem
	resolveHook    func(context.Context, string) (routeIndexManifest, error)
	resolveClose   func()
	settingsHook   func(*httpAPIServer, *http.ServeMux, string)
	settingsClose  func()
	obsHooks       map[string]obsActionHook
	obsPrefixCount map[string]int
	moduleLocks    *modulelock.Registry
}

func newModuleRegistry() *moduleRegistry {
	return &moduleRegistry{
		moduleLocks: modulelock.NewRegistry(),
	}
}

// RegisterResolveHook 绑定节点路由解析能力。
//
// 设计说明：
// - 只放解析入口，不把 settings 写入口混进来；
// - capabilityHook 由这里统一接到能力列表，避免单独再拼一次；
// - closeHook 只负责解绑，不负责业务清理。
func (r *moduleRegistry) RegisterResolveHook(
	capabilityHook func() *contractmessage.CapabilityItem,
	resolveHook func(context.Context, string) (routeIndexManifest, error),
	closeHook func(),
) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	r.mu.Lock()
	if r.capabilityHook != nil || r.resolveHook != nil || r.resolveClose != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("resolve hook is already registered")
	}
	r.capabilityHook = capabilityHook
	r.resolveHook = resolveHook
	r.resolveClose = closeHook
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			if r.resolveClose != nil {
				r.resolveClose()
			}
			r.capabilityHook = nil
			r.resolveHook = nil
			r.resolveClose = nil
			r.mu.Unlock()
		})
	}, nil
}

// RegisterSettingsHooks 绑定 settings 管理面挂载口。
//
// 设计说明：
// - 这里只登记挂载动作，不登记业务实现；
// - HTTP 层只负责把路由转给这里，避免再写模块分支；
// - 该钩子和解析钩子分开，避免一处变动牵连另一处。
func (r *moduleRegistry) RegisterSettingsHooks(settingsHook func(*httpAPIServer, *http.ServeMux, string), closeHook func()) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	r.mu.Lock()
	if r.settingsHook != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("settings hook is already registered")
	}
	r.settingsHook = settingsHook
	r.settingsClose = closeHook
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			if r.settingsClose != nil {
				r.settingsClose()
			}
			r.settingsHook = nil
			r.settingsClose = nil
			r.mu.Unlock()
		})
	}, nil
}

// RegisterOBSAction 把一个 obs 动作接到通用动作表。
//
// 设计说明：
// - 这里只记录“动作 -> handler”，不写模块分支；
// - 前缀只用来区分“模块没启用”和“动作漏接线”；
// - 这里不依赖 contract 白名单，避免把模块动作错放到契约层。
func (r *moduleRegistry) RegisterOBSAction(action string, handler OBSActionHandler) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	action = strings.TrimSpace(action)
	if action == "" {
		return nil, fmt.Errorf("action is required")
	}
	r.mu.Lock()
	if r.obsHooks == nil {
		r.obsHooks = map[string]obsActionHook{}
	}
	if r.obsPrefixCount == nil {
		r.obsPrefixCount = map[string]int{}
	}
	if _, exists := r.obsHooks[action]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("obs action is already registered: %s", action)
	}
	prefix := obsActionPrefix(action)
	r.obsHooks[action] = obsActionHook{
		action:  action,
		handler: handler,
	}
	r.obsPrefixCount[prefix]++
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			if r == nil {
				return
			}
			r.mu.Lock()
			entry, ok := r.obsHooks[action]
			if ok {
				delete(r.obsHooks, action)
				prefix := obsActionPrefix(entry.action)
				if count := r.obsPrefixCount[prefix]; count <= 1 {
					delete(r.obsPrefixCount, prefix)
				} else {
					r.obsPrefixCount[prefix] = count - 1
				}
			}
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
	if r.capabilityHook == nil {
		return nil
	}
	item := r.capabilityHook()
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

// ResolveRoute 走通用解析钩子。
func (r *moduleRegistry) ResolveRoute(ctx context.Context, route string) (routeIndexManifest, error) {
	if r == nil {
		return routeIndexManifest{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	r.mu.RLock()
	hook := r.resolveHook
	r.mu.RUnlock()
	if hook == nil {
		return routeIndexManifest{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}
	return hook(ctx, route)
}

// MountSettingsRoutes 只做 settings 路由挂载，不碰业务分支。
func (r *moduleRegistry) MountSettingsRoutes(s *httpAPIServer, mux *http.ServeMux, prefix string) {
	if r == nil || s == nil || mux == nil {
		return
	}
	r.mu.RLock()
	hook := r.settingsHook
	r.mu.RUnlock()
	if hook == nil {
		return
	}
	hook(s, mux, prefix)
}

// CallOBSAction 走通用 obs 动作表。
//
// 设计说明：
// - 同一前缀下只要挂过一个动作，就说明模块已启用；
// - 同前缀动作缺 handler 时，直接报 handler not registered，尽早暴露漏接线；
// - 这里不回查 contract，避免把模块白名单放错层。
func (r *moduleRegistry) CallOBSAction(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
	action = strings.TrimSpace(action)
	if action == "" {
		return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "action is required")
	}
	if ctx == nil {
		return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "ctx is required")
	}
	if r == nil {
		return OBSActionResponse{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
	}

	r.mu.RLock()
	entry, exists := r.obsHooks[action]
	enabled := r.obsPrefixCount[obsActionPrefix(action)] > 0
	r.mu.RUnlock()

	if !exists || entry.handler == nil {
		if !enabled {
			return OBSActionResponse{}, newModuleHookError("MODULE_DISABLED", "module is disabled")
		}
		return OBSActionResponse{}, newModuleHookError("HANDLER_NOT_REGISTERED", "handler not registered")
	}
	return entry.handler(ctx, payload)
}

func obsActionPrefix(action string) string {
	action = strings.TrimSpace(action)
	if action == "" {
		return ""
	}
	if idx := strings.LastIndex(action, "."); idx > 0 {
		return action[:idx]
	}
	return action
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
func (rt *Runtime) Modules() OBSActionCaller {
	if rt == nil {
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
