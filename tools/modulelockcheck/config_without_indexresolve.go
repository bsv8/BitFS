//go:build !with_indexresolve

package main

func init() {
	moduleConfigs = map[string]moduleConfig{}
}
