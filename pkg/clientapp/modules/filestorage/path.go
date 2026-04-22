package filestorage

import (
	"fmt"
	"path/filepath"
	"strings"
)

func normalizeRootPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("workspace path is required")
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func normalizeRelFilePath(raw string) (string, error) {
	p := filepath.ToSlash(strings.TrimSpace(raw))
	if p == "" {
		return "", fmt.Errorf("file path is required")
	}
	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("file path must be relative")
	}
	clean := filepath.ToSlash(filepath.Clean(p))
	if clean == "." || clean == "" || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return "", fmt.Errorf("file path is outside root")
	}
	return clean, nil
}

func workspacePathJoin(root, rel string) string {
	root = filepath.Clean(strings.TrimSpace(root))
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	if rel == "" {
		return root
	}
	return filepath.Join(root, filepath.FromSlash(rel))
}
