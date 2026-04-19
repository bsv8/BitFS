package clientapp

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage"
	oldproto "github.com/golang/protobuf/proto"
)

type inboxMessageStoreAdapter struct {
	store moduleBootstrapStore
}

func (a inboxMessageStoreAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.store.ExecContext(ctx, query, args...)
}

func (a inboxMessageStoreAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.store.QueryContext(ctx, query, args...)
}

func (a inboxMessageStoreAdapter) Do(ctx context.Context, fn func(inboxmessage.Conn) error) error {
	return a.store.Do(ctx, func(conn SQLConn) error {
		return fn(conn)
	})
}

func (a inboxMessageStoreAdapter) SerialAccess() bool {
	return a.store.SerialAccess()
}

func registerInboxMessageModule(ctx context.Context, rt *Runtime, store moduleBootstrapStore) (func(), error) {
	if rt == nil || store == nil {
		return func() {}, nil
	}
	if ctx == nil {
		return func() {}, fmt.Errorf("ctx is required")
	}

	connAdapter := inboxMessageStoreAdapter{store: store}
	var serialExec inboxmessage.SerialExecutor
	if store.SerialAccess() {
		serialExec = connAdapter
	}

	moduleStore, err := inboxmessage.BootstrapStore(ctx, connAdapter, serialExec)
	if err != nil {
		return func() {}, err
	}

	reg := ensureModuleRegistry(rt)
	if reg == nil {
		return func() {}, nil
	}

	capabilityCleanup, err := reg.registerCapabilityHook(inboxmessage.CapabilityItem)
	if err != nil {
		return nil, err
	}
	httpCleanup, err := reg.RegisterHTTPAPIHook(func(s *httpAPIServer, mux *http.ServeMux, prefix string) {
		if s == nil || mux == nil {
			return
		}
		mux.HandleFunc(prefix+"/v1/settings/inbox/messages", s.withAuth(handleInboxMessagesSettings(moduleStore)))
		mux.HandleFunc(prefix+"/v1/settings/inbox/messages/detail", s.withAuth(handleInboxMessageDetailSettings(moduleStore)))
	})
	if err != nil {
		capabilityCleanup()
		return nil, err
	}
	callCleanup, err := reg.RegisterLibP2PHook(LibP2PProtocolNodeCall, inboxmessage.InboxMessageRoute, func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		receipt, err := inboxmessage.BizReceive(ctx, moduleStore, inboxmessage.ReceiveParams{
			MessageID:       ev.Meta.MessageID,
			SenderPubKeyHex: ev.Meta.SenderPubkeyHex,
			TargetInput:     strings.TrimSpace(ev.Req.To),
			Route:           inboxmessage.InboxMessageRoute,
			ContentType:     ev.ContentType,
			Body:            ev.Req.Body,
			ReceivedAtUnix:  time.Now().Unix(),
		})
		if err != nil {
			code := inboxmessage.CodeOf(err)
			msg := inboxmessage.MessageOf(err)
			if code == "" {
				code = "INTERNAL_ERROR"
				msg = err.Error()
			}
			return LibP2PResult{CallResp: ncall.CallResp{Ok: false, Code: code, Message: msg}}, nil
		}
		bodyBytes, _ := oldproto.Marshal(&inboxmessage.InboxReceipt{InboxMessageID: receipt.InboxMessageID, ReceivedAtUnix: receipt.ReceivedAtUnix})
		return LibP2PResult{CallResp: ncall.CallResp{
			Ok:          true,
			Code:        "OK",
			ContentType: contractmessage.ContentTypeProto,
			Body:        bodyBytes,
		}}, nil
	})
	if err != nil {
		httpCleanup()
		capabilityCleanup()
		return nil, err
	}

	return func() {
		callCleanup()
		httpCleanup()
		capabilityCleanup()
	}, nil
}

func handleInboxMessagesSettings(store inboxmessage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		if store == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "error", "error": map[string]any{"code": "MODULE_DISABLED", "message": "module disabled"}})
			return
		}
		items, err := inboxmessage.BizList(r.Context(), store)
		if err != nil {
			code := inboxmessage.CodeOf(err)
			msg := inboxmessage.MessageOf(err)
			if code == "" {
				code = "INTERNAL_ERROR"
				msg = err.Error()
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "error": map[string]any{"code": code, "message": msg}})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "data": map[string]any{"total": len(items), "items": items}})
	}
}

func handleInboxMessageDetailSettings(store inboxmessage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		if store == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "error", "error": map[string]any{"code": "MODULE_DISABLED", "message": "module disabled"}})
			return
		}
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "error": map[string]any{"code": "BAD_REQUEST", "message": "invalid id"}})
			return
		}
		detail, err := inboxmessage.BizDetail(r.Context(), store, id)
		if err != nil {
			code := inboxmessage.CodeOf(err)
			msg := inboxmessage.MessageOf(err)
			if code == "NOT_FOUND" {
				writeJSON(w, http.StatusNotFound, map[string]any{"status": "error", "error": map[string]any{"code": code, "message": msg}})
				return
			}
			if code == "" {
				code = "INTERNAL_ERROR"
				msg = err.Error()
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "error": map[string]any{"code": code, "message": msg}})
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
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "data": out})
	}
}
