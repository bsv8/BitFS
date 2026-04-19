package main

import (
	"strings"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
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

func TestParseModulesKeepsEmptyInput(t *testing.T) {
	selected, err := parseModules("")
	if err != nil {
		t.Fatalf("parse modules failed: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("expected empty selected modules, got %#v", selected)
	}
}

func TestValidateWhitelistShapeRejectsUnsupportedModule(t *testing.T) {
	items := []modulelock.LockedFunction{
		{ID: "one", Module: "other", Package: "./other", Symbol: "A", Signature: "func A()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "unsupported module") {
		t.Fatalf("expected unsupported module error, got %v", err)
	}
}
