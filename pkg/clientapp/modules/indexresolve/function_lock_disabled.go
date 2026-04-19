//go:build indexresolve_disabled

package indexresolve

import "github.com/bsv8/BitFS/pkg/clientapp/modulelock"

// FunctionLocks 在禁用构建下返回空白名单。
func FunctionLocks() []modulelock.LockedFunction {
	return nil
}
