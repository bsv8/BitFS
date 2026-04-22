package filestorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func handleAdminStaticTree(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodGet {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		workspacePath := strings.TrimSpace(r.URL.Query().Get("workspace_path"))
		if workspacePath == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		abs, rel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), workspacePath, r.URL.Query().Get("path"))
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		recursive := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "1") ||
			strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "true")
		maxDepth := parseBoundInt(r.URL.Query().Get("max_depth"), 1, 1, 16)
		if !recursive {
			maxDepth = 1
		}
		st, err := os.Stat(abs)
		if err != nil {
			if errorsIsNotExist(err) {
				writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "path not found")
				return
			}
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if !st.IsDir() {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "path is not directory")
			return
		}
		items, err := buildStaticTree(r.Context(), svc, abs, rel, 1, maxDepth)
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		parent := "/"
		if rel != "/" {
			parent = filepath.ToSlash(filepath.Dir(rel))
			if !strings.HasPrefix(parent, "/") {
				parent = "/" + parent
			}
		}
		writeModuleOK(w, map[string]any{
			"root_path":    workspacePath,
			"current_path": rel,
			"parent_path":  parent,
			"recursive":    recursive,
			"max_depth":    maxDepth,
			"total":        len(items),
			"items":        items,
		})
	}
}

func handleAdminStaticMkdir(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodPost {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		var req struct {
			WorkspacePath string `json:"workspace_path"`
			Path          string `json:"path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
			return
		}
		abs, rel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), req.WorkspacePath, req.Path)
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		if err := os.MkdirAll(abs, 0o755); err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		writeModuleOK(w, map[string]any{"ok": true, "path": rel})
	}
}

func handleAdminStaticUpload(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodPost {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		if err := r.ParseMultipartForm(128 << 20); err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid multipart form")
			return
		}
		workspacePath := strings.TrimSpace(r.FormValue("workspace_path"))
		if workspacePath == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		targetDirRaw := strings.TrimSpace(r.FormValue("target_dir"))
		if targetDirRaw == "" {
			targetDirRaw = strings.TrimSpace(r.FormValue("path"))
		}
		if targetDirRaw == "" {
			targetDirRaw = "/"
		}
		targetAbs, targetRel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), workspacePath, targetDirRaw)
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		overwrite := strings.EqualFold(strings.TrimSpace(r.FormValue("overwrite")), "1") ||
			strings.EqualFold(strings.TrimSpace(r.FormValue("overwrite")), "true")
		fileName := sanitizeRecommendedFileName(strings.TrimSpace(r.FormValue("file_name")))
		file, fileHeader, err := r.FormFile("file")
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "file is required")
			return
		}
		defer file.Close()
		if fileName == "" {
			fileName = sanitizeRecommendedFileName(strings.TrimSpace(fileHeader.Filename))
		}
		if fileName == "" {
			fileName = fmt.Sprintf("upload_%d.bin", time.Now().UnixNano())
		}
		if err := os.MkdirAll(targetAbs, 0o755); err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		dst := filepath.Join(targetAbs, fileName)
		if _, err := os.Stat(dst); err == nil && !overwrite {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "target file already exists")
			return
		}
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		written, copyErr := io.Copy(out, file)
		closeErr := out.Close()
		if copyErr != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", copyErr.Error())
			return
		}
		if closeErr != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", closeErr.Error())
			return
		}
		seedRec, err := svc.seed.EnsureSeedForFile(r.Context(), moduleapi.SeedEnsureInput{
			FilePath:            dst,
			RecommendedFileName: fileName,
			MimeHint:            guessContentType(dst, nil),
		})
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if err := dbUpsertWorkspaceFileByAbsPath(r.Context(), svc.host.Store(), dst, seedRec.SeedHash, false); err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		relPath := filepath.ToSlash(filepath.Join(targetRel, fileName))
		if !strings.HasPrefix(relPath, "/") {
			relPath = "/" + relPath
		}
		writeModuleOK(w, map[string]any{
			"ok":         true,
			"path":       relPath,
			"seed_hash":  seedRec.SeedHash,
			"target_dir": targetRel,
			"bytes":      written,
			"overwrite":  overwrite,
		})
	}
}

func handleAdminStaticMove(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodPost {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		var req struct {
			WorkspacePath string `json:"workspace_path"`
			FromPath      string `json:"from_path"`
			ToPath        string `json:"to_path"`
			SourcePath    string `json:"source_path"`
			TargetDir     string `json:"target_dir"`
			NewName       string `json:"new_name"`
			Overwrite     bool   `json:"overwrite"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
			return
		}
		sourcePath := strings.TrimSpace(req.SourcePath)
		if sourcePath == "" {
			sourcePath = strings.TrimSpace(req.FromPath)
		}
		if strings.TrimSpace(req.WorkspacePath) == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		fromAbs, fromRel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), req.WorkspacePath, sourcePath)
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		targetPath := strings.TrimSpace(req.ToPath)
		if strings.TrimSpace(req.TargetDir) != "" || strings.TrimSpace(req.NewName) != "" {
			targetDir := strings.TrimSpace(req.TargetDir)
			if targetDir == "" {
				targetDir = filepath.ToSlash(filepath.Dir(fromRel))
			}
			newName := sanitizeRecommendedFileName(strings.TrimSpace(req.NewName))
			if newName == "" {
				newName = filepath.Base(fromRel)
			}
			targetPath = filepath.ToSlash(filepath.Join(targetDir, newName))
			if !strings.HasPrefix(targetPath, "/") {
				targetPath = "/" + targetPath
			}
		}
		if strings.TrimSpace(targetPath) == "" || normalizeStaticInputPath(targetPath) == "/" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "target path is required")
			return
		}
		toAbs, toRel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), req.WorkspacePath, targetPath)
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		if _, err := os.Stat(fromAbs); err != nil {
			if errorsIsNotExist(err) {
				writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "source path not found")
				return
			}
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if _, err := os.Stat(toAbs); err == nil {
			if fromAbs == toAbs {
				seedRec, err := svc.seed.EnsureSeedForFile(r.Context(), moduleapi.SeedEnsureInput{
					FilePath:            toAbs,
					RecommendedFileName: filepath.Base(toAbs),
					MimeHint:            guessContentType(toAbs, nil),
				})
				if err != nil {
					writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
					return
				}
				if err := dbUpsertWorkspaceFileByAbsPath(r.Context(), svc.host.Store(), toAbs, seedRec.SeedHash, false); err != nil {
					writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
					return
				}
				writeModuleOK(w, map[string]any{
					"ok":        true,
					"from_path": fromRel,
					"to_path":   toRel,
				})
				return
			}
			if !req.Overwrite {
				writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "target path already exists")
				return
			}
			_ = os.RemoveAll(toAbs)
		}
		if err := os.MkdirAll(filepath.Dir(toAbs), 0o755); err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if fromAbs != toAbs {
			if err := os.Rename(fromAbs, toAbs); err != nil {
				writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
				return
			}
		}
		seedRec, err := svc.seed.EnsureSeedForFile(r.Context(), moduleapi.SeedEnsureInput{
			FilePath:            toAbs,
			RecommendedFileName: filepath.Base(toAbs),
			MimeHint:            guessContentType(toAbs, nil),
		})
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if fromAbs != toAbs {
			if err := dbDeleteWorkspaceFileByAbsPath(r.Context(), svc.host.Store(), fromAbs); err != nil {
				writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
				return
			}
		}
		if err := dbUpsertWorkspaceFileByAbsPath(r.Context(), svc.host.Store(), toAbs, seedRec.SeedHash, false); err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		writeModuleOK(w, map[string]any{
			"ok":        true,
			"from_path": fromRel,
			"to_path":   toRel,
		})
	}
}

func handleAdminStaticEntry(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodDelete {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		workspacePath := strings.TrimSpace(r.URL.Query().Get("workspace_path"))
		if workspacePath == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		abs, rel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), workspacePath, r.URL.Query().Get("path"))
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		if rel == "/" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace root cannot be deleted")
			return
		}
		recursive := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "1") ||
			strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "true")
		st, err := os.Stat(abs)
		if err != nil {
			if errorsIsNotExist(err) {
				writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "path not found")
				return
			}
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if st.IsDir() {
			if !recursive {
				writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "directory delete requires recursive=true")
				return
			}
			if err := os.RemoveAll(abs); err != nil {
				writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
				return
			}
		} else {
			if err := os.Remove(abs); err != nil {
				writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
				return
			}
		}
		if err := deleteWorkspaceFilesByAbsPath(r.Context(), svc.host.Store(), abs); err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if svc.seed != nil {
			_ = svc.seed.CleanupOrphanSeeds(r.Context())
		}
		writeModuleOK(w, map[string]any{"ok": true, "deleted": true, "path": rel})
	}
}

func handleAdminStaticPriceSet(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodPost {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		var req struct {
			WorkspacePath       string `json:"workspace_path"`
			Path                string `json:"path"`
			FloorPriceSatPer64K uint64 `json:"floor_price_sat_per_64k"`
			ResaleDiscountBPS   uint64 `json:"resale_discount_bps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
			return
		}
		if strings.TrimSpace(req.WorkspacePath) == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		if req.FloorPriceSatPer64K == 0 {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "floor_price_sat_per_64k must be > 0")
			return
		}
		if req.ResaleDiscountBPS == 0 {
			req.ResaleDiscountBPS = svc.host.SellerResaleDiscountBPS()
		}
		if req.ResaleDiscountBPS > 10000 {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "resale_discount_bps must be <= 10000")
			return
		}
		abs, rel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), req.WorkspacePath, req.Path)
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		st, err := os.Stat(abs)
		if err != nil {
			if errorsIsNotExist(err) {
				writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "path not found")
				return
			}
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if st.IsDir() {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "path is directory")
			return
		}
		seedHash, err := dbGetWorkspaceFileSeedHashByAbsPath(r.Context(), svc.host.Store(), abs)
		if err != nil {
			writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
			return
		}
		if seedHash == "" {
			writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "file is not registered")
			return
		}
		snap, ok, err := svc.seed.LoadSeedSnapshot(r.Context(), seedHash)
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if !ok {
			writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "seed snapshot not found")
			return
		}
		if err := dbUpsertSeedPricingPolicy(r.Context(), svc.host.Store(), seedHash, req.FloorPriceSatPer64K, req.ResaleDiscountBPS, "user", time.Now().Unix()); err != nil {
			writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
			return
		}
		writeModuleOK(w, map[string]any{
			"ok":                           true,
			"path":                         rel,
			"seed_hash":                    seedHash,
			"floor_unit_price_sat_per_64k": req.FloorPriceSatPer64K,
			"resale_discount_bps":          req.ResaleDiscountBPS,
			"seed_price_satoshi":           req.FloorPriceSatPer64K * uint64(snap.ChunkCount),
			"chunk_price_sat":              req.FloorPriceSatPer64K,
			"pricing_bound":                true,
		})
	}
}

func handleAdminStaticPriceGet(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodGet {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		workspacePath := strings.TrimSpace(r.URL.Query().Get("workspace_path"))
		if workspacePath == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		abs, rel, err := resolveStaticWorkspacePath(r.Context(), svc.host.Store(), workspacePath, r.URL.Query().Get("path"))
		if err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		seedHash, err := dbGetWorkspaceFileSeedHashByAbsPath(r.Context(), svc.host.Store(), abs)
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if seedHash == "" {
			writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "price not configured")
			return
		}
		snap, ok, err := svc.seed.LoadSeedSnapshot(r.Context(), seedHash)
		if err != nil {
			writeModuleError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		if !ok {
			writeModuleError(w, http.StatusNotFound, "NOT_FOUND", "price not configured")
			return
		}
		writeModuleOK(w, map[string]any{
			"path":                         rel,
			"seed_hash":                    seedHash,
			"floor_unit_price_sat_per_64k": snap.FloorPriceSatPer64K,
			"resale_discount_bps":          snap.ResaleDiscountBPS,
			"price_updated_at_unix":        snap.PriceUpdatedAtUnix,
		})
	}
}

type adminStaticTreeNode struct {
	Name                string                `json:"name"`
	Path                string                `json:"path"`
	Type                string                `json:"type"`
	Size                int64                 `json:"size"`
	MtimeUnix           int64                 `json:"mtime_unix"`
	HasChildren         bool                  `json:"has_children,omitempty"`
	Children            []adminStaticTreeNode `json:"children,omitempty"`
	SeedHash            string                `json:"seed_hash,omitempty"`
	FloorPriceSatPer64K uint64                `json:"floor_unit_price_sat_per_64k,omitempty"`
	ResaleDiscountBPS   uint64                `json:"resale_discount_bps,omitempty"`
	PriceUpdatedAtUnix  int64                 `json:"price_updated_at_unix,omitempty"`
}

func buildStaticTree(ctx context.Context, svc *service, abs string, rel string, depth int, maxDepth int) ([]adminStaticTreeNode, error) {
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}
	out := make([]adminStaticTreeNode, 0, len(entries))
	for _, de := range entries {
		name := strings.TrimSpace(de.Name())
		if name == "" {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		full := filepath.Join(abs, name)
		relPath := filepath.ToSlash(filepath.Join(rel, name))
		if !strings.HasPrefix(relPath, "/") {
			relPath = "/" + relPath
		}
		node := adminStaticTreeNode{
			Name:      name,
			Path:      relPath,
			Size:      info.Size(),
			MtimeUnix: info.ModTime().Unix(),
		}
		if de.IsDir() {
			node.Type = "dir"
			if kids, err := os.ReadDir(full); err == nil && len(kids) > 0 {
				node.HasChildren = true
			}
			if depth < maxDepth {
				children, err := buildStaticTree(ctx, svc, full, relPath, depth+1, maxDepth)
				if err == nil {
					node.Children = children
				}
			}
		} else {
			node.Type = "file"
			seedHash, err := dbGetWorkspaceFileSeedHashByAbsPath(ctx, svc.host.Store(), full)
			if err == nil && seedHash != "" && svc.seed != nil {
				node.SeedHash = seedHash
				if snap, ok, err := svc.seed.LoadSeedSnapshot(ctx, seedHash); err == nil && ok {
					node.FloorPriceSatPer64K = snap.FloorPriceSatPer64K
					node.ResaleDiscountBPS = snap.ResaleDiscountBPS
					node.PriceUpdatedAtUnix = snap.PriceUpdatedAtUnix
				}
			}
		}
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type == "dir"
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func resolveStaticWorkspacePath(ctx context.Context, store moduleapi.Store, workspacePath, input string) (string, string, error) {
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return "", "", err
	}
	if store == nil {
		return "", "", fmt.Errorf("store is required")
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return "", "", err
	}
	found := false
	for _, root := range roots {
		rootAbs, err := normalizeRootPath(root)
		if err != nil {
			continue
		}
		if rootAbs == workspacePath {
			found = true
			break
		}
	}
	if !found {
		return "", "", fmt.Errorf("workspace_path is not registered")
	}
	clean := normalizeStaticInputPath(input)
	if clean == "/" {
		return workspacePath, "/", nil
	}
	rel := strings.TrimPrefix(clean, "/")
	abs := filepath.Clean(filepath.Join(workspacePath, filepath.FromSlash(rel)))
	if relCheck, err := filepath.Rel(workspacePath, abs); err != nil || relCheck == "." || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("path is outside workspace")
	}
	return abs, "/" + filepath.ToSlash(rel), nil
}

func normalizeStaticInputPath(raw string) string {
	p := strings.TrimSpace(raw)
	if p == "" || p == "/" {
		return "/"
	}
	p = strings.ReplaceAll(p, "\\", "/")
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	clean := path.Clean(p)
	if clean == "." || clean == "" {
		return "/"
	}
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return clean
}

func dbGetWorkspaceFileSeedHashByAbsPath(ctx context.Context, store moduleapi.Store, absPath string) (string, error) {
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return "", nil
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return "", err
	}
	root, rel, ok := resolveWorkspaceRelativePath(absPath, roots)
	if !ok {
		return "", nil
	}
	var seedHash string
	err = store.Read(ctx, func(rc moduleapi.ReadConn) error {
		row := rc.QueryRowContext(ctx, `SELECT seed_hash FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, root, rel)
		if err := row.Scan(&seedHash); err != nil {
			if errorsIsNoRows(err) {
				return nil
			}
			return err
		}
		return nil
	})
	return seedHash, err
}

func dbUpsertWorkspaceFileByAbsPath(ctx context.Context, store moduleapi.Store, absPath, seedHash string, locked bool) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return fmt.Errorf("output path is required")
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return err
	}
	root, rel, ok := resolveWorkspaceRelativePath(absPath, roots)
	if !ok {
		return fmt.Errorf("output path is outside registered biz_workspaces")
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return dbUpsertWorkspaceFileTx(ctx, tx, root, rel, seedHash, locked)
	})
}

func dbDeleteWorkspaceFileByAbsPath(ctx context.Context, store moduleapi.Store, absPath string) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return nil
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return err
	}
	root, rel, ok := resolveWorkspaceRelativePath(absPath, roots)
	if !ok {
		return nil
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return dbDeleteWorkspaceFileTx(ctx, tx, root, rel)
	})
}

func deleteWorkspaceFilesByAbsPath(ctx context.Context, store moduleapi.Store, absPath string) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return nil
	}
	files, _, err := dbListWorkspaceFiles(ctx, store, -1, 0, "")
	if err != nil {
		return err
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		for _, row := range files {
			full := filepath.Clean(workspacePathJoin(row.WorkspacePath, row.FilePath))
			if full != absPath {
				rel, err := filepath.Rel(absPath, full)
				if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					continue
				}
			}
			if err := dbDeleteWorkspaceFileTx(ctx, tx, row.WorkspacePath, row.FilePath); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbUpsertSeedPricingPolicy(ctx context.Context, store moduleapi.Store, seedHash string, floor uint64, bps uint64, source string, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return upsertSeedPricingPolicyTx(ctx, tx, seedHash, floor, bps, source, updatedAtUnix)
	})
}

func upsertSeedPricingPolicyTx(ctx context.Context, tx moduleapi.WriteTx, seedHash string, floor uint64, bps uint64, source string, updatedAtUnix int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	if source != "user" && source != "system" {
		source = "system"
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO biz_seed_pricing_policy(seed_hash, floor_unit_price_sat_per_64k, resale_discount_bps, pricing_source, updated_at_unix)
		VALUES(?,?,?,?,?)
		ON CONFLICT(seed_hash) DO UPDATE SET
			floor_unit_price_sat_per_64k=excluded.floor_unit_price_sat_per_64k,
			resale_discount_bps=excluded.resale_discount_bps,
			pricing_source=CASE
				WHEN biz_seed_pricing_policy.pricing_source='user' AND excluded.pricing_source='system' THEN biz_seed_pricing_policy.pricing_source
				ELSE excluded.pricing_source
			END,
			updated_at_unix=excluded.updated_at_unix`,
		seedHash, int64(floor), int64(bps), source, updatedAtUnix)
	return err
}

func parseBoundInt(raw string, def int, min int, max int) int {
	v := strings.TrimSpace(raw)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func errorsIsNotExist(err error) bool {
	return err != nil && os.IsNotExist(err)
}

func errorsIsNoRows(err error) bool {
	return err == sql.ErrNoRows
}

func guessContentType(path string, head []byte) string {
	return http.DetectContentType(head)
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
