package clientapp

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/getfilebyhash"
)

// getfilebyhashHTTPHandler HTTP API handler（getfilebyhash 模块）。
// 约束：
// - handler 只做 method 校验、JSON/query 解析、调模块函数、统一 JSON 返回；
// - 不写下载业务，不发布 demand，不读取 quote，不写 workspace。
type getfilebyhashHTTPHandler struct {
	caps *GetFileByHashModuleCaps
}

func newGetFileByHashHTTPHandler(caps *GetFileByHashModuleCaps) *getfilebyhashHTTPHandler {
	return &getfilebyhashHTTPHandler{caps: caps}
}

func (h *getfilebyhashHTTPHandler) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	var req getfilebyhash.StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body")
		return
	}

	result, err := h.caps.StartDownload(r.Context(), req)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, getfilebyhash.CodeOf(err), getfilebyhash.MessageOf(err))
		return
	}

	h.writeSuccess(w, result)
}

func (h *getfilebyhashHTTPHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	if jobID == "" {
		h.writeError(w, http.StatusBadRequest, "BAD_REQUEST", "job_id is required")
		return
	}

	status, err := h.caps.GetStatus(r.Context(), jobID)
	if err != nil {
		code := getfilebyhash.CodeOf(err)
		if code == getfilebyhash.CodeJobNotFound {
			h.writeError(w, http.StatusNotFound, code, getfilebyhash.MessageOf(err))
			return
		}
		h.writeError(w, http.StatusInternalServerError, code, getfilebyhash.MessageOf(err))
		return
	}

	h.writeSuccess(w, status)
}

func (h *getfilebyhashHTTPHandler) handleChunks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	if jobID == "" {
		h.writeError(w, http.StatusBadRequest, "BAD_REQUEST", "job_id is required")
		return
	}

	chunks, err := h.caps.ListChunks(r.Context(), jobID)
	if err != nil {
		code := getfilebyhash.CodeOf(err)
		if code == getfilebyhash.CodeJobNotFound {
			h.writeError(w, http.StatusNotFound, code, getfilebyhash.MessageOf(err))
			return
		}
		h.writeError(w, http.StatusInternalServerError, code, getfilebyhash.MessageOf(err))
		return
	}

	h.writeSuccess(w, chunks)
}

func (h *getfilebyhashHTTPHandler) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	if jobID == "" {
		h.writeError(w, http.StatusBadRequest, "BAD_REQUEST", "job_id is required")
		return
	}

	nodes, err := h.caps.ListNodes(r.Context(), jobID)
	if err != nil {
		code := getfilebyhash.CodeOf(err)
		if code == getfilebyhash.CodeJobNotFound {
			h.writeError(w, http.StatusNotFound, code, getfilebyhash.MessageOf(err))
			return
		}
		h.writeError(w, http.StatusInternalServerError, code, getfilebyhash.MessageOf(err))
		return
	}

	h.writeSuccess(w, nodes)
}

func (h *getfilebyhashHTTPHandler) handleQuotes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	if jobID == "" {
		h.writeError(w, http.StatusBadRequest, "BAD_REQUEST", "job_id is required")
		return
	}

	quotes, err := h.caps.ListQuotes(r.Context(), jobID)
	if err != nil {
		code := getfilebyhash.CodeOf(err)
		if code == getfilebyhash.CodeJobNotFound {
			h.writeError(w, http.StatusNotFound, code, getfilebyhash.MessageOf(err))
			return
		}
		h.writeError(w, http.StatusInternalServerError, code, getfilebyhash.MessageOf(err))
		return
	}

	h.writeSuccess(w, quotes)
}

func (h *getfilebyhashHTTPHandler) writeSuccess(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func (h *getfilebyhashHTTPHandler) writeError(w http.ResponseWriter, status int, code string, message string) {
	obs.Info("getfilebyhash", "http_api_error", map[string]any{
		"code":    code,
		"message": message,
	})
	writeJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}