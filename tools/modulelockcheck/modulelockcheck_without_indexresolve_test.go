//go:build !with_indexresolve

package main

import (
	"strings"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

func TestModuleConfigsEmptyWithoutIndexResolve(t *testing.T) {
	if len(moduleConfigs) != 0 {
		t.Fatalf("expected empty module configs, got %#v", moduleConfigs)
	}
}

func TestParseModulesWithoutIndexResolveKeepsEmptyInput(t *testing.T) {
	selected, err := parseModules("")
	if err != nil {
		t.Fatalf("parse modules failed: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("expected empty selected modules, got %#v", selected)
	}
}

func TestValidateWhitelistShapeRejectsUnsupportedModuleWithoutIndexResolve(t *testing.T) {
	items := []modulelock.LockedFunction{
		{ID: "one", Module: "indexresolve", Package: "./pkg/clientapp/modules/indexresolve", Symbol: "A", Signature: "func A()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "unsupported module") {
		t.Fatalf("expected unsupported module error, got %v", err)
	}
}
