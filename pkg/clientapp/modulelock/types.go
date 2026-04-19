package modulelock

// LockedFunction 是模块本地白名单项。
//
// 设计说明：
// - 这里只保存模块自己的冻结规则，不回写 contract/fnlock；
// - ID 要稳定且全局唯一，用来让检查器一眼定位改动；
// - Signature 由 go doc 直接比对，不能靠人工口头约定。
type LockedFunction struct {
	ID               string
	Module           string
	Package          string
	Symbol           string
	Signature        string
	ObsControlAction string
	Note             string
}

// Provider 是模块白名单提供者。
//
// 设计说明：
// - 只返回当前模块的冻结项；
// - 不传聚合结构体，不让调用方顺手拿到别的模块信息。
type Provider func() []LockedFunction
