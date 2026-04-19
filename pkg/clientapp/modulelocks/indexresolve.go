package modulelocks

import "github.com/bsv8/BitFS/pkg/clientapp/modulelock"

const ModuleIdentity = "indexresolve"

// FunctionLocks 返回 indexresolve 模块的冻结白名单。
//
// 设计说明：
// - 白名单放到独立包里，避免跟模块实现包绑死；
// - 这里只保留模块身份和冻结签名，不参与模块注册；
// - 新增白名单项时，只能在这里补，不能再分叉出第二份真相。
func FunctionLocks() []modulelock.LockedFunction {
	return []modulelock.LockedFunction{
		{
			ID:        "bitfs.indexresolve.function_locks",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modulelocks",
			Symbol:    "FunctionLocks",
			Signature: "func FunctionLocks() []modulelock.LockedFunction",
			Note:      "冻结 indexresolve 白名单入口，防止白名单定义被拆散到模块实现包里。",
		},
		{
			ID:        "bitfs.indexresolve.error_new",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NewError",
			Signature: "func NewError(code, message string) error",
			Note:      "错误对象必须保持稳定入口，避免上层靠字符串拼接误判。",
		},
		{
			ID:        "bitfs.indexresolve.error_code_of",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "CodeOf",
			Signature: "func CodeOf(err error) string",
			Note:      "错误码提取要保持固定接口，供 HTTP 和调用链统一判断。",
		},
		{
			ID:        "bitfs.indexresolve.error_message_of",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "MessageOf",
			Signature: "func MessageOf(err error) string",
			Note:      "错误信息提取要保持固定接口，避免上层重复解析错误文本。",
		},
		{
			ID:        "bitfs.indexresolve.normalize_route",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NormalizeRoute",
			Signature: "func NormalizeRoute(raw string) (string, error)",
			Note:      "路由归一化必须稳定，不能让外层自己猜规则。",
		},
		{
			ID:        "bitfs.indexresolve.normalize_seed_hash_hex",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NormalizeSeedHashHex",
			Signature: "func NormalizeSeedHashHex(raw string) (string, error)",
			Note:      "种子 hash 归一化必须稳定，避免上层各写一套校验。",
		},
		{
			ID:        "bitfs.indexresolve.new_service",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "NewService",
			Signature: "func NewService(store Store, runtime RuntimeReader) *Service",
			Note:      "服务构造入口必须稳定，模块接线要按这个入口拼装。",
		},
		{
			ID:        "bitfs.indexresolve.service_enabled",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Enabled",
			Signature: "func (s *Service) Enabled() bool",
			Note:      "启用状态是模块对外可见边界的一部分。",
		},
		{
			ID:        "bitfs.indexresolve.service_node_pubkey_hex",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.NodePubkeyHex",
			Signature: "func (s *Service) NodePubkeyHex() string",
			Note:      "节点公钥输出要走固定接口，避免外层直接碰内部字段。",
		},
		{
			ID:        "bitfs.indexresolve.service_capability",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Capability",
			Signature: "func (s *Service) Capability() *contractmessage.CapabilityItem",
			Note:      "能力项输出入口要稳定，主框架只认这个结果。",
		},
		{
			ID:        "bitfs.indexresolve.service_close",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Close",
			Signature: "func (s *Service) Close()",
			Note:      "关闭入口必须稳定，注册和解绑都依赖它收口。",
		},
		{
			ID:        "bitfs.indexresolve.service_resolve",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Resolve",
			Signature: "func (s *Service) Resolve(ctx context.Context, rawRoute string) (Manifest, error)",
			Note:      "解析入口必须稳定，调用链只允许通过这里查路由。",
		},
		{
			ID:        "bitfs.indexresolve.service_list",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.List",
			Signature: "func (s *Service) List(ctx context.Context) ([]RouteItem, error)",
			Note:      "列表入口必须稳定，settings 页面依赖它拿数据。",
		},
		{
			ID:        "bitfs.indexresolve.service_upsert",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Upsert",
			Signature: "func (s *Service) Upsert(ctx context.Context, rawRoute string, rawSeedHash string, nowUnix int64) (RouteItem, error)",
			Note:      "写入入口必须稳定，避免上层直接拼 SQL。",
		},
		{
			ID:        "bitfs.indexresolve.service_delete",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "Service.Delete",
			Signature: "func (s *Service) Delete(ctx context.Context, rawRoute string) error",
			Note:      "删除入口必须稳定，settings 路由只允许走这里。",
		},
		{
			ID:        "bitfs.indexresolve.manifest_marshal_pb",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "MarshalManifestPB",
			Signature: "func MarshalManifestPB(m Manifest) ([]byte, error)",
			Note:      "manifest 编码入口必须稳定，协议层不能自己拼字节。",
		},
		{
			ID:        "bitfs.indexresolve.manifest_unmarshal_pb",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "UnmarshalManifestPB",
			Signature: "func UnmarshalManifestPB(raw []byte) (Manifest, error)",
			Note:      "manifest 解码入口必须稳定，协议层不能自己猜字段。",
		},
	}
}
