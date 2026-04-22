package seedstorage

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

func normalizeSeedHashHex(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if len(s) != 64 {
		return ""
	}
	for _, ch := range s {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			continue
		}
		return ""
	}
	return s
}

func normalizeSeedHashes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		h := normalizeSeedHashHex(raw)
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func sanitizeRecommendedFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, "\x00", "")
	return name
}

func sanitizeMIMEHint(raw string) string {
	return strings.TrimSpace(raw)
}

func contiguousChunkIndexes(chunkCount uint32) []uint32 {
	if chunkCount == 0 {
		return nil
	}
	out := make([]uint32, 0, chunkCount)
	for i := uint32(0); i < chunkCount; i++ {
		out = append(out, i)
	}
	return out
}

func stringsJoin(values []string, sep string) string {
	switch len(values) {
	case 0:
		return ""
	case 1:
		return values[0]
	default:
		return strings.Join(values, sep)
	}
}

func seedStorageErr(msg string) error {
	return fmt.Errorf("%s", msg)
}
