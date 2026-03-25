package clientapp

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

type apiRouteCallRequest struct {
	To          string          `json:"to"`
	Route       string          `json:"route"`
	ContentType string          `json:"content_type"`
	Body        json.RawMessage `json:"body,omitempty"`
	BodyBase64  string          `json:"body_base64,omitempty"`
}

func (s *httpAPIServer) handleCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req apiRouteCallRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	body, err := decodeRouteCallBody(req.Body, req.BodyBase64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	resp, err := TriggerClientCall(r.Context(), s.rt, TriggerClientCallParams{
		To:          req.To,
		Route:       req.Route,
		ContentType: req.ContentType,
		Body:        body,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, routeCallHTTPResponse(resp.Ok, resp.Code, resp.Message, resp.ContentType, resp.Body))
}

func (s *httpAPIServer) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		To    string `json:"to"`
		Route string `json:"route"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerClientResolve(r.Context(), s.rt, TriggerClientResolveParams{To: req.To, Route: req.Route})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, routeCallHTTPResponse(resp.Ok, resp.Code, resp.Message, resp.ContentType, resp.Body))
}

func (s *httpAPIServer) handleInboxMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "db not initialized"})
		return
	}
	rows, err := s.db.Query(
		`SELECT id,message_id,sender_pubkey_hex,target_input,route,content_type,body_size_bytes,received_at_unix
		   FROM inbox_messages
		  ORDER BY received_at_unix DESC,id DESC`,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type item struct {
		ID             int64  `json:"id"`
		MessageID      string `json:"message_id"`
		SenderPubKey   string `json:"sender_pubkey_hex"`
		TargetInput    string `json:"target_input"`
		Route          string `json:"route"`
		ContentType    string `json:"content_type"`
		BodySizeBytes  int64  `json:"body_size_bytes"`
		ReceivedAtUnix int64  `json:"received_at_unix"`
	}
	items := make([]item, 0)
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ID, &it.MessageID, &it.SenderPubKey, &it.TargetInput, &it.Route, &it.ContentType, &it.BodySizeBytes, &it.ReceivedAtUnix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": len(items), "items": items})
}

func (s *httpAPIServer) handleInboxMessageDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "db not initialized"})
		return
	}
	id, err := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("id")), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
		return
	}
	var (
		messageID      string
		senderPubKey   string
		targetInput    string
		route          string
		contentType    string
		bodyBytes      []byte
		bodySizeBytes  int64
		receivedAtUnix int64
	)
	err = s.db.QueryRow(
		`SELECT message_id,sender_pubkey_hex,target_input,route,content_type,body_bytes,body_size_bytes,received_at_unix
		   FROM inbox_messages WHERE id=?`,
		id,
	).Scan(&messageID, &senderPubKey, &targetInput, &route, &contentType, &bodyBytes, &bodySizeBytes, &receivedAtUnix)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	out := map[string]any{
		"id":                id,
		"message_id":        messageID,
		"sender_pubkey_hex": senderPubKey,
		"target_input":      targetInput,
		"route":             route,
		"content_type":      contentType,
		"body_size_bytes":   bodySizeBytes,
		"received_at_unix":  receivedAtUnix,
	}
	attachHTTPBodyPayload(out, contentType, bodyBytes)
	writeJSON(w, http.StatusOK, out)
}

func (s *httpAPIServer) handleAdminRouteIndexes(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "db not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		rows, err := s.db.Query(`SELECT route,seed_hash,updated_at_unix FROM published_route_indexes ORDER BY route ASC`)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		defer rows.Close()
		type item struct {
			Route         string `json:"route"`
			SeedHash      string `json:"seed_hash"`
			UpdatedAtUnix int64  `json:"updated_at_unix"`
		}
		items := make([]item, 0)
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.Route, &it.SeedHash, &it.UpdatedAtUnix); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			items = append(items, it)
		}
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"total": len(items), "items": items})
	case http.MethodPost:
		var req struct {
			Route    string `json:"route"`
			SeedHash string `json:"seed_hash"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		updatedAtUnix, err := upsertPublishedRouteIndex(s.db, req.Route, req.SeedHash)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":              true,
			"route":           strings.TrimSpace(req.Route),
			"seed_hash":       strings.ToLower(strings.TrimSpace(req.SeedHash)),
			"updated_at_unix": updatedAtUnix,
		})
	case http.MethodDelete:
		route := strings.TrimSpace(r.URL.Query().Get("route"))
		if route == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "route is required"})
			return
		}
		if _, err := s.db.Exec(`DELETE FROM published_route_indexes WHERE route=?`, route); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true, "route": route})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func decodeRouteCallBody(raw json.RawMessage, bodyBase64 string) ([]byte, error) {
	if strings.TrimSpace(bodyBase64) != "" {
		if len(strings.TrimSpace(string(raw))) != 0 && strings.TrimSpace(string(raw)) != "null" {
			return nil, errors.New("body and body_base64 are mutually exclusive")
		}
		out, err := base64.StdEncoding.DecodeString(strings.TrimSpace(bodyBase64))
		if err != nil {
			return nil, errors.New("invalid body_base64")
		}
		return out, nil
	}
	if len(strings.TrimSpace(string(raw))) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	return append([]byte(nil), raw...), nil
}

func routeCallHTTPResponse(ok bool, code, message, contentType string, body []byte) map[string]any {
	out := map[string]any{
		"ok":      ok,
		"code":    strings.TrimSpace(code),
		"message": strings.TrimSpace(message),
	}
	if strings.TrimSpace(contentType) != "" {
		out["content_type"] = strings.TrimSpace(contentType)
	}
	attachHTTPBodyPayload(out, contentType, body)
	return out
}

func attachHTTPBodyPayload(out map[string]any, contentType string, body []byte) {
	if len(body) == 0 {
		return
	}
	out["body_base64"] = base64.StdEncoding.EncodeToString(body)
	if strings.Contains(strings.ToLower(strings.TrimSpace(contentType)), "json") && json.Valid(body) {
		var decoded any
		if err := json.Unmarshal(body, &decoded); err == nil {
			out["body_json"] = decoded
		}
	}
}
