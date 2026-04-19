//go:build with_indexresolve

package indexresolve

import "github.com/bsv8/BitFS/pkg/clientapp/modulelock"

// FunctionLocks 返回 indexresolve 模块的冻结白名单。
//
// 设计说明：
// - 白名单跟着模块一起编译，避免出现两份真相；
// - 这里直接描述模块实现包的稳定入口，检查器只做比对，不再拼接规则；
// - ModuleIdentity 复用 spec.go 里的模块身份，保持口径统一。
func FunctionLocks() []modulelock.LockedFunction {
	return []modulelock.LockedFunction{
		{
			ID:        "bitfs.indexresolve.function_locks",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "FunctionLocks",
			Signature: "func FunctionLocks() []modulelock.LockedFunction",
			Note:      "冻结 indexresolve 白名单入口，白名单和模块实现同生共死。",
		},
		{
			ID:        "bitfs.indexresolve.error_new",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NewError",
			Signature: "func NewError(code, message string) error",
			Note:      "冻结 indexresolve 错误构造入口。",
		},
		{
			ID:        "bitfs.indexresolve.error_code_of",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "CodeOf",
			Signature: "func CodeOf(err error) string",
			Note:      "冻结 indexresolve 错误码提取入口。",
		},
		{
			ID:        "bitfs.indexresolve.error_message_of",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "MessageOf",
			Signature: "func MessageOf(err error) string",
			Note:      "冻结 indexresolve 错误消息提取入口。",
		},
		{
			ID:        "bitfs.indexresolve.normalize_route",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NormalizeRoute",
			Signature: "func NormalizeRoute(raw string) (string, error)",
			Note:      "冻结路由归一化入口。",
		},
		{
			ID:        "bitfs.indexresolve.normalize_seed_hash_hex",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NormalizeSeedHashHex",
			Signature: "func NormalizeSeedHashHex(raw string) (string, error)",
			Note:      "冻结种子哈希归一化入口。",
		},
		{
			ID:        "bitfs.indexresolve.new_service",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NewService",
			Signature: "func NewService(store Store, runtime RuntimeReader) *Service",
			Note:      "冻结服务构造入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_enabled",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Enabled",
			Signature: "func (s *Service) Enabled() bool",
			Note:      "冻结服务启用态查询入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_node_pubkey_hex",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.NodePubkeyHex",
			Signature: "func (s *Service) NodePubkeyHex() string",
			Note:      "冻结节点公钥查询入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_capability",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Capability",
			Signature: "func (s *Service) Capability() *contractmessage.CapabilityItem",
			Note:      "冻结能力描述入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_close",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Close",
			Signature: "func (s *Service) Close()",
			Note:      "冻结服务关闭入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_resolve",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Resolve",
			Signature: "func (s *Service) Resolve(ctx context.Context, rawRoute string) (Manifest, error)",
			Note:      "冻结解析入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_list",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.List",
			Signature: "func (s *Service) List(ctx context.Context) ([]RouteItem, error)",
			Note:      "冻结列表入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_upsert",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Upsert",
			Signature: "func (s *Service) Upsert(ctx context.Context, rawRoute string, rawSeedHash string, nowUnix int64) (RouteItem, error)",
			Note:      "冻结写入入口。",
		},
		{
			ID:        "bitfs.indexresolve.service_delete",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Delete",
			Signature: "func (s *Service) Delete(ctx context.Context, rawRoute string) error",
			Note:      "冻结删除入口。",
		},
		{
			ID:        "bitfs.indexresolve.manifest_marshal_pb",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "MarshalManifestPB",
			Signature: "func MarshalManifestPB(m Manifest) ([]byte, error)",
			Note:      "冻结 manifest 的 protobuf 输出入口。",
		},
		{
			ID:        "bitfs.indexresolve.manifest_unmarshal_pb",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "UnmarshalManifestPB",
			Signature: "func UnmarshalManifestPB(raw []byte) (Manifest, error)",
			Note:      "冻结 manifest 的 protobuf 输入入口。",
		},
	}
}
