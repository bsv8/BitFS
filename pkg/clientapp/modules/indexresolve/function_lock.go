//go:build with_indexresolve

package indexresolve

import "github.com/bsv8/BitFS/pkg/clientapp/modulelock"

// FunctionLocks 返回 indexresolve 模块本地的 obs 动作白名单。
//
// 设计说明：
// - 这里只保留模块本地动作锁，不落到 contract；
// - 4 个动作共享同一套业务入口，只是在 obs 层分成 4 个动作名；
// - ModuleIdentity 复用 spec.go 里的模块身份，保持口径统一。
func FunctionLocks() []modulelock.LockedFunction {
	return []modulelock.LockedFunction{
		{
			ID:               "bitfs.indexresolve.obs_resolve",
			Module:           ModuleIdentity,
			Package:          "./pkg/clientapp/modules/indexresolve",
			Symbol:           "BizResolve",
			Signature:        "func BizResolve(ctx context.Context, resolver ResolveReader, rawRoute string) (Manifest, error)",
			ObsControlAction: "settings.index_resolve.resolve",
			Note:             "解析动作只允许走模块本地业务入口。",
		},
		{
			ID:               "bitfs.indexresolve.obs_settings_list",
			Module:           ModuleIdentity,
			Package:          "./pkg/clientapp/modules/indexresolve",
			Symbol:           "BizSettingsList",
			Signature:        "func BizSettingsList(ctx context.Context, lister SettingsLister) ([]RouteItem, error)",
			ObsControlAction: "settings.index_resolve.list",
			Note:             "settings 列表动作只允许走模块本地业务入口。",
		},
		{
			ID:               "bitfs.indexresolve.obs_settings_upsert",
			Module:           ModuleIdentity,
			Package:          "./pkg/clientapp/modules/indexresolve",
			Symbol:           "BizSettingsUpsert",
			Signature:        "func BizSettingsUpsert(ctx context.Context, upserter SettingsUpserter, rawRoute string, rawSeedHash string) (RouteItem, error)",
			ObsControlAction: "settings.index_resolve.upsert",
			Note:             "settings 写入动作只允许走模块本地业务入口。",
		},
		{
			ID:               "bitfs.indexresolve.obs_settings_delete",
			Module:           ModuleIdentity,
			Package:          "./pkg/clientapp/modules/indexresolve",
			Symbol:           "BizSettingsDelete",
			Signature:        "func BizSettingsDelete(ctx context.Context, deleter SettingsDeleter, rawRoute string) error",
			ObsControlAction: "settings.index_resolve.delete",
			Note:             "settings 删除动作只允许走模块本地业务入口。",
		},
	}
}
