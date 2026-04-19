package main

import (
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

func init() {
	moduleConfigs = map[string]moduleConfig{
		indexresolve.ModuleIdentity: {
			name:     indexresolve.ModuleIdentity,
			dir:      "BitFS",
			provider: func() []moduleapi.LockedFunction { return indexresolve.FunctionLocks() },
		},
	}
}
