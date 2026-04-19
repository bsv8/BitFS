package modulekit

import (
	"encoding/json"
	"net/http"
)

func WriteOK(w http.ResponseWriter, data any) {
	WriteJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func WriteJSON(w http.ResponseWriter, status int, payload any) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
