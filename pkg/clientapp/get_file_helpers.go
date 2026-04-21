package clientapp

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// 设计说明：
// - 这些函数原本跟旧 HTTP handler 放在同一个文件里；
// - 现在旧对外 API 已经下线，但 workspace、HTTP 静态响应还在复用它们；
// - 所以只保留纯工具函数，不把旧路由和旧 handler 带回来。

type staticResourcePlanItem struct {
	SeedHash            string `json:"seed_hash"`
	Status              string `json:"status"`
	Reason              string `json:"reason,omitempty"`
	LocalReady          bool   `json:"local_ready"`
	RecommendedFileName string `json:"recommended_file_name,omitempty"`
	MIMEHint            string `json:"mime_hint,omitempty"`
	FileSize            uint64 `json:"file_size,omitempty"`
	ChunkCount          uint32 `json:"chunk_count,omitempty"`
	SeedPrice           uint64 `json:"seed_price,omitempty"`
	ChunkPrice          uint64 `json:"chunk_price_sat_per_64k,omitempty"`
	EstimatedTotalSat   uint64 `json:"estimated_total_sat,omitempty"`
}

type staticResourceStatus struct {
	staticResourcePlanItem
	OutputFilePath string `json:"output_file_path,omitempty"`
	ContentURI     string `json:"content_uri,omitempty"`
}

func findCompleteLocalFileBySeedHash(ctx context.Context, store *clientDB, seedHash string) (string, uint64, bool) {
	if store == nil {
		return "", 0, false
	}
	path, _, err := dbFindLatestWorkspaceFileBySeedHash(ctx, store, seedHash)
	if err != nil {
		return "", 0, false
	}
	st, err := os.Stat(path)
	if err != nil || !st.Mode().IsRegular() {
		return "", 0, false
	}
	return path, uint64(st.Size()), true
}

func loadSeedChunkCount(ctx context.Context, store *clientDB, seedHash string) (uint32, bool) {
	if store == nil {
		return 0, false
	}
	return dbGetSeedChunkCount(ctx, store, seedHash)
}

func localSeedMetadataBySeedHash(ctx context.Context, store *clientDB, seedHash string, path string) (string, string) {
	recommendedName := dbRecommendedFileNameBySeedHash(ctx, store, seedHash)
	if recommendedName == "" {
		recommendedName = sanitizeRecommendedFileName(filepath.Base(path))
	}
	mimeHint := dbMimeHintBySeedHash(ctx, store, seedHash)
	if mimeHint == "" {
		mimeHint = sanitizeMIMEHint(guessContentType(path, nil))
	}
	return recommendedName, mimeHint
}

func buildLocalStaticResourceStatus(ctx context.Context, store *clientDB, seedHash string, path string) staticResourceStatus {
	path = strings.TrimSpace(path)
	recommendedName, mimeHint := localSeedMetadataBySeedHash(ctx, store, seedHash, path)
	status := staticResourceStatus{
		staticResourcePlanItem: staticResourcePlanItem{
			SeedHash:            strings.ToLower(strings.TrimSpace(seedHash)),
			Status:              "local",
			LocalReady:          true,
			RecommendedFileName: recommendedName,
			MIMEHint:            mimeHint,
		},
		OutputFilePath: path,
		ContentURI:     "bitfs://" + strings.ToLower(strings.TrimSpace(seedHash)),
	}
	if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() {
		status.FileSize = uint64(st.Size())
	}
	if chunkCount, ok := loadSeedChunkCount(ctx, store, seedHash); ok {
		status.ChunkCount = chunkCount
	}
	return status
}

func serveSeedContentFile(w http.ResponseWriter, r *http.Request, path string, mimeHint string) {
	f, err := os.Open(path)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	contentType := sanitizeMIMEHint(mimeHint)
	if contentType == "" {
		contentType = guessContentType(path, head[:n])
	}
	w.Header().Set("Content-Type", contentType)
	http.ServeContent(w, r, filepath.Base(path), st.ModTime(), f)
}

func guessContentType(name string, head []byte) string {
	ext := strings.TrimSpace(filepath.Ext(strings.TrimSpace(name)))
	if ext != "" {
		if ct := mime.TypeByExtension(ext); strings.TrimSpace(ct) != "" {
			return ct
		}
	}
	if len(head) > 0 {
		if ct := guessContentTypeFromTextHead(head); ct != "" {
			return ct
		}
		return http.DetectContentType(head)
	}
	return "application/octet-stream"
}

// 设计说明：
// - 纯种子文件经常没有扩展名，不能只靠 MIME 默认猜；
// - 先识别 HTML / SVG / JSON / JS / CSS，避免静态页面被发成 text/plain；
// - 这个判断只用于响应头，不参与业务决策。
func guessContentTypeFromTextHead(head []byte) string {
	trimmed := strings.TrimSpace(string(head))
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(lower, "<!doctype html"), strings.HasPrefix(lower, "<html"):
		return "text/html; charset=utf-8"
	case strings.HasPrefix(lower, "<?xml"), strings.Contains(lower, "<svg"):
		return "image/svg+xml"
	case json.Valid([]byte(trimmed)):
		if strings.HasPrefix(lower, "{") || strings.HasPrefix(lower, "[") {
			return "application/json; charset=utf-8"
		}
	case looksLikeJavaScript(trimmed):
		return "application/javascript; charset=utf-8"
	case looksLikeCSS(lower):
		return "text/css; charset=utf-8"
	}
	return ""
}

func looksLikeCSS(lower string) bool {
	if lower == "" {
		return false
	}
	if looksLikeJavaScript(lower) {
		return false
	}
	if strings.HasPrefix(lower, ":root") || strings.HasPrefix(lower, "@media") || strings.HasPrefix(lower, "@import") {
		return true
	}
	if !strings.Contains(lower, "{") || !strings.Contains(lower, "}") {
		return false
	}
	if strings.Contains(lower, "function") || strings.Contains(lower, "=>") ||
		strings.Contains(lower, "const ") || strings.Contains(lower, "let ") ||
		strings.Contains(lower, "var ") || strings.Contains(lower, "return ") ||
		strings.Contains(lower, "document.") || strings.Contains(lower, "window.") {
		return false
	}
	return strings.Contains(lower, ":") && strings.Contains(lower, ";")
}

func looksLikeJavaScript(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	if lower == "" {
		return false
	}
	prefixes := []string{
		"(function", "(()=>", "const ", "let ", "var ", "function ",
		"import ", "export ", "\"use strict\"", "'use strict'",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return strings.Contains(lower, "=>") ||
		strings.Contains(lower, "document.") ||
		strings.Contains(lower, "window.") ||
		strings.Contains(lower, "createelement(") ||
		strings.Contains(lower, "queryselector(")
}
