package main

import (
	"strings"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func TestParseModulesAcceptsIndexResolve(t *testing.T) {
	selected, err := parseModules("indexresolve")
	if err != nil {
		t.Fatalf("parse modules failed: %v", err)
	}
	if len(selected) != 1 {
		t.Fatalf("expected one selected module, got %#v", selected)
	}
	if _, ok := selected["indexresolve"]; !ok {
		t.Fatalf("expected indexresolve selected, got %#v", selected)
	}
}

func TestParseModulesRejectsUnsupportedModule(t *testing.T) {
	_, err := parseModules("unknownmodule")
	if err == nil || !strings.Contains(err.Error(), "unsupported module") {
		t.Fatalf("expected unsupported module error, got %v", err)
	}
}

func TestParseModulesKeepsEmptyInput(t *testing.T) {
	selected, err := parseModules("")
	if err != nil {
		t.Fatalf("parse modules failed: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("expected empty selected modules, got %#v", selected)
	}
}

func TestBuiltinModuleLockModulesReturnsIndexResolve(t *testing.T) {
	modules := clientapp.BuiltinModuleLockModules()
	if len(modules) != 1 {
		t.Fatalf("expected 1 module with lock items, got %d", len(modules))
	}
	if modules[0] != "indexresolve" {
		t.Fatalf("expected indexresolve, got %s", modules[0])
	}
}

func TestBuiltinModuleLockItemsIncludesIndexResolve(t *testing.T) {
	items, missing := clientapp.BuiltinModuleLockItems("indexresolve")
	if len(missing) != 0 {
		t.Fatalf("expected no missing modules, got %v", missing)
	}
	if len(items) != 4 {
		t.Fatalf("expected 4 items for indexresolve, got %d", len(items))
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Symbol, "Biz") {
			t.Fatalf("unexpected lock symbol: %s", item.Symbol)
		}
	}
}

func TestBuiltinModuleLockItemsReportsMissingModule(t *testing.T) {
	items, missing := clientapp.BuiltinModuleLockItems("domain")
	if len(items) != 0 {
		t.Fatalf("expected no items for domain (no lock provider), got %d", len(items))
	}
	if len(missing) != 1 || missing[0] != "domain" {
		t.Fatalf("expected domain in missing, got %v", missing)
	}
}

func TestBuiltinModuleLockItemsWithUnknownModule(t *testing.T) {
	_, missing := clientapp.BuiltinModuleLockItems("unknownmodule")
	if len(missing) != 1 || missing[0] != "unknownmodule" {
		t.Fatalf("expected unknownmodule in missing, got %v", missing)
	}
}

func TestValidateWhitelistShapeRejectsUnsupportedModule(t *testing.T) {
	items := []moduleapi.LockedFunction{
		{ID: "one", Module: "other", Package: "./other", Symbol: "A", Signature: "func A()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "unsupported module") {
		t.Fatalf("expected unsupported module error, got %v", err)
	}
}
