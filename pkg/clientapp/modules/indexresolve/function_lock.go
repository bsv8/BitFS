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
			ID:        "bitfs.indexresolve.biz_resolve",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "BizResolve",
			Signature: "func BizResolve(ctx context.Context, state ModuleState, resolver ResolveReader, emitter ObsEmitter, rawRoute string) (Manifest, error)",
			Note:      "冻结解析业务入口，只允许这里对外接线。",
		},
		{
			ID:        "bitfs.indexresolve.biz_settings_list",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "BizSettingsList",
			Signature: "func BizSettingsList(ctx context.Context, state ModuleState, lister SettingsLister, emitter ObsEmitter) ([]RouteItem, error)",
			Note:      "冻结 settings 列表业务入口。",
		},
		{
			ID:        "bitfs.indexresolve.biz_settings_upsert",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "BizSettingsUpsert",
			Signature: "func BizSettingsUpsert(ctx context.Context, state ModuleState, upserter SettingsUpserter, emitter ObsEmitter, rawRoute string, rawSeedHash string) (RouteItem, error)",
			Note:      "冻结 settings 写入业务入口。",
		},
		{
			ID:        "bitfs.indexresolve.biz_settings_delete",
			Module:    ModuleIdentity,
			Package:   "./pkg/clientapp/modules/indexresolve",
			Symbol:    "BizSettingsDelete",
			Signature: "func BizSettingsDelete(ctx context.Context, state ModuleState, deleter SettingsDeleter, emitter ObsEmitter, rawRoute string) (string, error)",
			Note:      "冻结 settings 删除业务入口。",
		},
	}
}
