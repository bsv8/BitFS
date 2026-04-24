package clientapp

import (
	"fmt"
	"strings"

	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
)

// normalizeResolverNameCanonical 统一把 resolver 名字收敛到系统内规范。
// 设计说明：
// - 这里只做名字规范化，不再承担旧 resolver 的网络解析入口；
// - 这个辅助仍然给 domain 相关 trigger 复用，避免散落重复校验。
func normalizeResolverNameCanonical(raw string) (string, error) {
	value, err := domainwire.NormalizeName(raw)
	if err != nil {
		return "", err
	}
	if strings.Contains(value, "/") {
		return "", fmt.Errorf("resolver name must not contain /")
	}
	return value, nil
}
