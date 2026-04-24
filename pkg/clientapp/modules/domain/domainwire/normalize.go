package domainwire

import (
	"fmt"
	"strings"

	"github.com/adraffy/go-ens-normalize/ensip15"
)

// NormalizeName 按 ENSIP-15 统一名字。
// 设计说明：
// - 域名系统、客户端 locator、链上提交验证都必须共用同一规范化实现；
// - 这里不再保留旧 resolver 的 trim+lowercase 简化逻辑，避免未来状态键不一致；
// - 规范化失败直接拒绝，让错误尽早暴露，不让脏名字进入锁表和注册表。
func NormalizeName(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("name is required")
	}
	norm, err := ensip15.Shared().Normalize(value)
	if err != nil {
		return "", fmt.Errorf("name normalize failed: %w", err)
	}
	norm = strings.TrimSpace(norm)
	if norm == "" {
		return "", fmt.Errorf("name is required")
	}
	return norm, nil
}
