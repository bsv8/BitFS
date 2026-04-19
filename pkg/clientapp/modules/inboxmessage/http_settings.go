package inboxmessage

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/modulekit"
)

func handleInboxMessagesSettings(store Store) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodGet {
			modulekit.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		items, err := BizList(r.Context(), store)
		if err != nil {
			modulekit.WriteError(w, inboxMessageSettingsStatusFromError(err), inboxMessageErrorCode(err), inboxMessageErrorMessage(err))
			return
		}
		modulekit.WriteOK(w, map[string]any{"total": len(items), "items": items})
	}
}

func handleInboxMessageDetailSettings(store Store) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodGet {
			modulekit.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid id")
			return
		}
		detail, err := BizDetail(r.Context(), store, id)
		if err != nil {
			modulekit.WriteError(w, inboxMessageSettingsStatusFromError(err), inboxMessageErrorCode(err), inboxMessageErrorMessage(err))
			return
		}
		out := map[string]any{
			"id":                detail.ID,
			"message_id":        detail.MessageID,
			"sender_pubkey_hex": detail.SenderPubKey,
			"target_input":      detail.TargetInput,
			"route":             detail.Route,
			"content_type":      detail.ContentType,
			"body_size_bytes":   detail.BodySizeBytes,
			"received_at_unix":  detail.ReceivedAtUnix,
		}
		if len(detail.BodyBytes) > 0 {
			out["body_base64"] = base64.StdEncoding.EncodeToString(detail.BodyBytes)
			if json.Valid(detail.BodyBytes) {
				var decoded any
				if err := json.Unmarshal(detail.BodyBytes, &decoded); err == nil {
					out["body_json"] = decoded
				}
			}
		}
		modulekit.WriteOK(w, out)
	}
}

func inboxMessageSettingsStatusFromError(err error) int {
	switch CodeOf(err) {
	case "BAD_REQUEST":
		return http.StatusBadRequest
	case "NOT_FOUND":
		return http.StatusNotFound
	case "MODULE_DISABLED":
		return http.StatusServiceUnavailable
	case "REQUEST_CANCELED":
		return 499
	default:
		return http.StatusInternalServerError
	}
}

func inboxMessageErrorCode(err error) string {
	code := CodeOf(err)
	if code != "" {
		return code
	}
	return "INTERNAL_ERROR"
}

func inboxMessageErrorMessage(err error) string {
	if msg := MessageOf(err); msg != "" {
		return msg
	}
	return "internal error"
}
