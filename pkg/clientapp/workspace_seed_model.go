package clientapp

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

type workspaceRelativePath struct {
	WorkspacePath string
	FilePath      string
}

type seedChunkSupplyRow struct {
	SeedHash   string
	ChunkIndex uint32
}

// normalizeSeedHashHex 统一 seed hash 入库前口径。
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

// normalizeWorkspacePath 统一工作区根路径口径。
func normalizeWorkspacePath(raw string) (string, error) {
	abs, err := filepath.Abs(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// normalizeWorkspaceFilePath 统一工作区内文件相对路径。
func normalizeWorkspaceFilePath(raw string) (string, error) {
	p := filepath.ToSlash(strings.TrimSpace(raw))
	if p == "" {
		return "", fmt.Errorf("file path is empty")
	}
	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("file path must be relative")
	}
	clean := filepath.ToSlash(filepath.Clean(p))
	if clean == "." || clean == "" || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", fmt.Errorf("file path is outside workspace")
	}
	if strings.Contains(clean, "/../") || strings.HasSuffix(clean, "/..") {
		return "", fmt.Errorf("file path is outside workspace")
	}
	return clean, nil
}

// resolveWorkspaceRelativePath 将绝对路径落到唯一 workspace 下。
func resolveWorkspaceRelativePath(absPath string, workspaceRoots []string) (workspaceRelativePath, bool) {
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return workspaceRelativePath{}, false
	}
	type candidate struct {
		root string
		rel  string
	}
	cands := make([]candidate, 0, len(workspaceRoots))
	for _, root := range workspaceRoots {
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "" {
			continue
		}
		rel, err := filepath.Rel(root, absPath)
		if err != nil {
			continue
		}
		if rel == "." {
			continue
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		cands = append(cands, candidate{root: root, rel: rel})
	}
	if len(cands) == 0 {
		return workspaceRelativePath{}, false
	}
	sort.Slice(cands, func(i, j int) bool {
		if len(cands[i].root) == len(cands[j].root) {
			return cands[i].root < cands[j].root
		}
		return len(cands[i].root) > len(cands[j].root)
	})
	rel := filepath.ToSlash(filepath.Clean(cands[0].rel))
	if rel == "." || rel == "" || strings.HasPrefix(rel, "../") || strings.Contains(rel, "/../") {
		return workspaceRelativePath{}, false
	}
	return workspaceRelativePath{
		WorkspacePath: cands[0].root,
		FilePath:      rel,
	}, true
}

func workspacePathJoin(root, rel string) string {
	root = filepath.Clean(strings.TrimSpace(root))
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	if rel == "" {
		return root
	}
	return filepath.Join(root, filepath.FromSlash(rel))
}

// parseSeedBytesV1 解析 seed 文件头，拿到块数与文件大小。
