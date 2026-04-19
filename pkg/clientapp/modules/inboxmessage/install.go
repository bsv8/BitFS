package inboxmessage

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
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	oldproto "github.com/golang/protobuf/proto"
)

type inboxMessageStoreAdapter struct {
	store moduleapi.Store
}

type inboxMessageModuleState struct {
	store Store
}

func (a inboxMessageStoreAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.store.ExecContext(ctx, query, args...)
}

func (a inboxMessageStoreAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.store.QueryContext(ctx, query, args...)
}

func (a inboxMessageStoreAdapter) Do(ctx context.Context, fn func(Conn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is required")
	}
	if fn == nil {
		return fmt.Errorf("store fn is required")
	}
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

func (a inboxMessageStoreAdapter) SerialAccess() bool {
	return a.store != nil && a.store.SerialAccess()
}

// Install 把 inbox message 模块接到主干能力上。
//
// 设计说明：
// - HTTP 只负责路由和 JSON 壳，业务仍然走模块自己的 Biz*；
// - libp2p 收件与 settings 读取共享同一份模块 store；
// - cleanup 关闭后，捕获的 handler 也要失效，避免继续误用旧入口。
func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}
	store := host.Store()
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}

	db := inboxMessageStoreAdapter{store: store}
	moduleStore, err := BootstrapStore(ctx, db, serialExecutorForInboxStore(store))
	if err != nil {
		return nil, err
	}
	state := &inboxMessageModuleState{store: moduleStore}

	cleanups := make([]func(), 0, 4)
	pushCleanup := func(cleanup func()) {
		if cleanup != nil {
			cleanups = append(cleanups, cleanup)
		}
	}
	closeAll := func() {
		state.store = nil
		for i := len(cleanups) - 1; i >= 0; i-- {
			if cleanups[i] != nil {
				cleanups[i]()
			}
		}
	}
	fail := func(err error) (func(), error) {
		closeAll()
		return nil, err
	}

	cleanup, err := host.RegisterCapability(ModuleID, CapabilityVersion)
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterHTTPRoute("/v1/settings/inbox/messages", func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeInboxMessageSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		handleInboxMessagesSettings(state)(w, r)
	})
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterHTTPRoute("/v1/settings/inbox/messages/detail", func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeInboxMessageSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		handleInboxMessageDetailSettings(state)(w, r)
	})
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterLibP2P(moduleapi.LibP2PProtocolNodeCall, InboxMessageRoute, func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		receipt, err := BizReceive(ctx, state.store, ReceiveParams{
			MessageID:       ev.MessageID,
			SenderPubKeyHex: ev.SenderPubkeyHex,
			TargetInput:     strings.TrimSpace(ev.Request.To),
			Route:           InboxMessageRoute,
			ContentType:     strings.TrimSpace(ev.Request.ContentType),
			Body:            append([]byte(nil), ev.Request.Body...),
			ReceivedAtUnix:  time.Now().Unix(),
		})
		if err != nil {
			return moduleapi.LibP2PResult{}, toModuleAPIError(err)
		}
		bodyBytes, _ := oldproto.Marshal(&InboxReceipt{
			InboxMessageID: receipt.InboxMessageID,
			ReceivedAtUnix: receipt.ReceivedAtUnix,
		})
		return moduleapi.LibP2PResult{
			CallResp: moduleapi.CallResponse{
				Ok:          true,
				Code:        "OK",
				ContentType: contractmessage.ContentTypeProto,
				Body:        bodyBytes,
			},
		}, nil
	})
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	return func() {
		closeAll()
	}, nil
}

func handleInboxMessagesSettings(state *inboxMessageModuleState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeInboxMessageSettingsError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		if state == nil || state.store == nil {
			writeInboxMessageSettingsError(w, http.StatusServiceUnavailable, "MODULE_DISABLED", "module is disabled")
			return
		}
		items, err := BizList(r.Context(), state.store)
		if err != nil {
			writeInboxMessageSettingsError(w, inboxMessageSettingsStatusFromError(err), inboxMessageErrorCode(err), inboxMessageErrorMessage(err))
			return
		}
		writeInboxMessageSettingsOK(w, map[string]any{"total": len(items), "items": items})
	}
}

func handleInboxMessageDetailSettings(state *inboxMessageModuleState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeInboxMessageSettingsError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		if state == nil || state.store == nil {
			writeInboxMessageSettingsError(w, http.StatusServiceUnavailable, "MODULE_DISABLED", "module is disabled")
			return
		}
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			writeInboxMessageSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid id")
			return
		}
		detail, err := BizDetail(r.Context(), state.store, id)
		if err != nil {
			writeInboxMessageSettingsError(w, inboxMessageSettingsStatusFromError(err), inboxMessageErrorCode(err), inboxMessageErrorMessage(err))
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
		writeInboxMessageSettingsOK(w, out)
	}
}

func serialExecutorForInboxStore(store moduleapi.Store) SerialExecutor {
	if store == nil || !store.SerialAccess() {
		return nil
	}
	return inboxMessageSerialExecutor{store: store}
}

type inboxMessageSerialExecutor struct {
	store moduleapi.Store
}

func (a inboxMessageSerialExecutor) Do(ctx context.Context, fn func(Conn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is required")
	}
	if fn == nil {
		return fmt.Errorf("store fn is required")
	}
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

func writeInboxMessageSettingsOK(w http.ResponseWriter, data any) {
	writeInboxMessageJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func writeInboxMessageSettingsError(w http.ResponseWriter, status int, code, message string) {
	writeInboxMessageJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    strings.TrimSpace(code),
			"message": strings.TrimSpace(message),
		},
	})
}

func writeInboxMessageJSON(w http.ResponseWriter, status int, payload any) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
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

func toModuleAPIError(err error) error {
	if err == nil {
		return nil
	}
	if code := CodeOf(err); code != "" {
		return moduleapi.NewError(code, MessageOf(err))
	}
	if code := moduleapi.CodeOf(err); code != "" {
		return err
	}
	return moduleapi.NewError("INTERNAL_ERROR", err.Error())
}
