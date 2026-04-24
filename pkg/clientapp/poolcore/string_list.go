package poolcore

import "strings"

// normalizeStringList 统一修剪字符串列表里的空白和空项。
// 说明：
//   - 这是底座级辅助函数，供地址声明等共享结构复用；
//   - 业务模块如果有自己的 payload 规则，可以自行调用或实现同等逻辑，
//     但不应该反过来要求 poolcore 依赖某个业务模块。
func normalizeStringList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, raw := range values {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}
