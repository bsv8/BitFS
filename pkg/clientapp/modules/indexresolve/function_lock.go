//go:build !indexresolve_disabled

package indexresolve

import (
	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

// FunctionLocks 返回 indexresolve 模块的冻结白名单。
//
// 设计说明：
// - 这里只冻结模块对外稳定能力；
// - 业务实现内部可重构，但一旦改签名就必须同步更新这里和测试；
// - 不把模块项塞回 contract/fnlock，避免把模块私有规则抬成全局规则。
func FunctionLocks() []modulelock.LockedFunction {
	return []modulelock.LockedFunction{
		{
			ID:        "bitfs.indexresolve.function_locks",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "FunctionLocks",
			Signature: "func FunctionLocks() []modulelock.LockedFunction",
			Note:      "模块白名单提供入口，只返回本模块的冻结项。",
		},
		{
			ID:        "bitfs.indexresolve.error_new",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NewError",
			Signature: "func NewError(code, message string) error",
			Note:      "模块统一错误出口，HTTP/服务层必须共享同一套错误码。",
		},
		{
			ID:        "bitfs.indexresolve.error_code_of",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "CodeOf",
			Signature: "func CodeOf(err error) string",
			Note:      "错误码提取函数，供门禁和 HTTP 响应统一使用。",
		},
		{
			ID:        "bitfs.indexresolve.error_message_of",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "MessageOf",
			Signature: "func MessageOf(err error) string",
			Note:      "错误消息提取函数，供门禁和 HTTP 响应统一使用。",
		},
		{
			ID:        "bitfs.indexresolve.normalize_route",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NormalizeRoute",
			Signature: "func NormalizeRoute(raw string) (string, error)",
			Note:      "路由归一化入口，保持 index resolve 的路由规则唯一。",
		},
		{
			ID:        "bitfs.indexresolve.normalize_seed_hash_hex",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NormalizeSeedHashHex",
			Signature: "func NormalizeSeedHashHex(raw string) (string, error)",
			Note:      "seed_hash 归一化入口，避免上层各写一份校验规则。",
		},
		{
			ID:        "bitfs.indexresolve.new_service",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NewService",
			Signature: "func NewService(store Store, runtime RuntimeReader) *Service",
			Note:      "模块服务构造入口，负责把 store 和运行时能力接起来。",
		},
		{
			ID:        "bitfs.indexresolve.service_enabled",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Enabled",
			Signature: "func (s *Service) Enabled() bool",
			Note:      "模块可用性判断入口，外层路由挂载必须先看这个开关。",
		},
		{
			ID:        "bitfs.indexresolve.service_node_pubkey_hex",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.NodePubkeyHex",
			Signature: "func (s *Service) NodePubkeyHex() string",
			Note:      "返回当前节点公钥 hex，作为模块展示和能力绑定依据。",
		},
		{
			ID:        "bitfs.indexresolve.service_capability",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Capability",
			Signature: "func (s *Service) Capability() *contractmessage.CapabilityItem",
			Note:      "模块能力展示入口，供上层 capability 表示使用。",
		},
		{
			ID:        "bitfs.indexresolve.service_close",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Close",
			Signature: "func (s *Service) Close()",
			Note:      "模块关闭入口，避免关闭后继续接受调用。",
		},
		{
			ID:        "bitfs.indexresolve.service_resolve",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Resolve",
			Signature: "func (s *Service) Resolve(ctx context.Context, rawRoute string) (Manifest, error)",
			Note:      "路由解析主入口，决定一个 route 最终指向哪份 manifest。",
		},
		{
			ID:        "bitfs.indexresolve.service_list",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.List",
			Signature: "func (s *Service) List(ctx context.Context) ([]RouteItem, error)",
			Note:      "路由列表入口，供 settings 管理面展示当前绑定关系。",
		},
		{
			ID:        "bitfs.indexresolve.service_upsert",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Upsert",
			Signature: "func (s *Service) Upsert(ctx context.Context, rawRoute string, rawSeedHash string, nowUnix int64) (RouteItem, error)",
			Note:      "路由绑定写入口，更新 route 和 seed 的映射关系。",
		},
		{
			ID:        "bitfs.indexresolve.service_delete",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Delete",
			Signature: "func (s *Service) Delete(ctx context.Context, rawRoute string) error",
			Note:      "路由解绑写入口，只负责删除映射，不做别的副作用。",
		},
		{
			ID:        "bitfs.indexresolve.manifest_marshal_pb",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "MarshalManifestPB",
			Signature: "func MarshalManifestPB(m Manifest) ([]byte, error)",
			Note:      "manifest protobuf 序列化入口，供协议层复用。",
		},
		{
			ID:        "bitfs.indexresolve.manifest_unmarshal_pb",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "UnmarshalManifestPB",
			Signature: "func UnmarshalManifestPB(raw []byte) (Manifest, error)",
			Note:      "manifest protobuf 反序列化入口，供协议层复用。",
		},
	}
}
