package indexresolve

import (
	"encoding/hex"
	"strings"
	"unicode"
)

func NormalizeRoute(raw string) (string, error) {
	route := strings.TrimSpace(raw)
	if route == "" {
		return DefaultRouteKey, nil
	}
	if isDefaultRouteAlias(route) {
		return DefaultRouteKey, nil
	}
	if hasInvalidRouteRune(route) {
		return "", NewError("ROUTE_INVALID", "route is invalid")
	}
	if !strings.HasPrefix(route, "/") {
		route = "/" + route
	}
	if route == "/" {
		return DefaultRouteKey, nil
	}
	return route, nil
}

func NormalizeSeedHashHex(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "", NewError("SEED_HASH_INVALID", "seed_hash is invalid")
	}
	if len(value) != 64 {
		return "", NewError("SEED_HASH_INVALID", "seed_hash is invalid")
	}
	rawBytes, err := hex.DecodeString(value)
	if err != nil || len(rawBytes) != 32 {
		return "", NewError("SEED_HASH_INVALID", "seed_hash is invalid")
	}
	return value, nil
}

func isDefaultRouteAlias(route string) bool {
	switch route {
	case "", "/", "/index", "index":
		return true
	default:
		return false
	}
}

func hasInvalidRouteRune(route string) bool {
	for _, r := range route {
		if r == '?' || r == '#' || unicode.IsControl(r) || unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

func routeNotFoundErr() error {
	return NewError("ROUTE_NOT_FOUND", "route not found")
}

func moduleDisabledErr() error {
	return NewError("MODULE_DISABLED", "index_resolve module is disabled")
}

func seedNotFoundErr() error {
	return NewError("SEED_NOT_FOUND", "seed not found")
}
