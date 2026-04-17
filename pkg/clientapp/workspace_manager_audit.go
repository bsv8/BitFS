package clientapp

import (
	"context"
	"errors"
	"strings"
	"time"
)

const (
	workspaceCommandTypeList   = "workspace.list"
	workspaceCommandTypeAdd    = "workspace.add"
	workspaceCommandTypeUpdate = "workspace.update"
	workspaceCommandTypeDelete = "workspace.delete"
	workspaceCommandTypeSync   = "workspace.sync"
)

type workspaceCommandMeta struct {
	CommandID   string
	CommandType string
	RequestedBy string
	RequestedAt int64
	TriggerKey  string
}

type workspaceCommandMetaKey struct{}

func withWorkspaceCommandMeta(ctx context.Context, meta workspaceCommandMeta) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, workspaceCommandMetaKey{}, meta)
}

func workspaceCommandMetaFromContext(ctx context.Context, defaultCommandType string) workspaceCommandMeta {
	meta := workspaceCommandMeta{}
	if ctx != nil {
		if v, ok := ctx.Value(workspaceCommandMetaKey{}).(workspaceCommandMeta); ok {
			meta = v
		}
	}
	meta.CommandType = strings.TrimSpace(meta.CommandType)
	if meta.CommandType == "" {
		meta.CommandType = strings.TrimSpace(defaultCommandType)
	}
	if strings.TrimSpace(meta.CommandID) == "" {
		meta.CommandID = newKernelCommandID()
	}
	meta.RequestedBy = strings.TrimSpace(meta.RequestedBy)
	if meta.RequestedBy == "" {
		meta.RequestedBy = "workspace_manager"
	}
	if meta.RequestedAt <= 0 {
		meta.RequestedAt = time.Now().Unix()
	}
	meta.TriggerKey = strings.TrimSpace(meta.TriggerKey)
	return meta
}

type workspaceRejectedError struct {
	code string
	msg  string
}

func (e workspaceRejectedError) Error() string {
	return strings.TrimSpace(e.msg)
}

func workspaceRejected(code, msg string) error {
	return workspaceRejectedError{code: strings.TrimSpace(code), msg: strings.TrimSpace(msg)}
}

func workspaceManagerResultStatus(err error) (bool, string) {
	if err == nil {
		return true, "applied"
	}
	var rejected workspaceRejectedError
	if errors.As(err, &rejected) {
		return false, "rejected"
	}
	return true, "failed"
}

func workspaceManagerErrorCode(action string, err error) string {
	if err == nil {
		return ""
	}
	var rejected workspaceRejectedError
	if errors.As(err, &rejected) {
		if strings.TrimSpace(rejected.code) != "" {
			return strings.TrimSpace(rejected.code)
		}
		return "workspace_rejected"
	}
	switch strings.TrimSpace(action) {
	case "workspace_list":
		return "workspace_list_failed"
	case "workspace_add":
		return "workspace_add_failed"
	case "workspace_update":
		return "workspace_update_failed"
	case "workspace_delete":
		return "workspace_delete_failed"
	case "workspace_sync":
		return "workspace_sync_failed"
	default:
		return "workspace_failed"
	}
}

func workspaceManagerErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func resolveWorkspaceCtx(preferred context.Context, fallback context.Context) context.Context {
	if preferred != nil {
		return preferred
	}
	return fallback
}

func (m *workspaceManager) auditWorkspaceRejected(ctx context.Context, meta workspaceCommandMeta, aggregateID, stateBefore, stateAfter string, err error, payload map[string]any) {
	errCode := workspaceManagerErrorCode(strings.TrimSpace(stateBefore), err)
	m.auditWorkspaceCommand(ctx, meta, aggregateID, stateBefore, stateAfter, false, "rejected", errCode, workspaceManagerErrorMessage(err), payload, map[string]any{}, 0)
}

func (m *workspaceManager) auditWorkspaceCommand(ctx context.Context, meta workspaceCommandMeta, aggregateID, stateBefore, stateAfter string, accepted bool, status, errorCode, errorMessage string, payload map[string]any, result map[string]any, duration time.Duration) {
	store := workspaceStore(m)
	if store == nil {
		return
	}
	meta = workspaceCommandMetaFromContext(withWorkspaceCommandMeta(ctx, meta), meta.CommandType)
	if strings.TrimSpace(aggregateID) == "" {
		aggregateID = "workspace:default"
	}
	if payload == nil {
		payload = map[string]any{}
	}
	if result == nil {
		result = map[string]any{}
	}
	_ = dbAppendCommandJournal(resolveWorkspaceCtx(ctx, m.ctx), store, commandJournalEntry{
		CommandID:     meta.CommandID,
		CommandType:   meta.CommandType,
		GatewayPeerID: "workspace",
		AggregateID:   strings.TrimSpace(aggregateID),
		RequestedBy:   meta.RequestedBy,
		RequestedAt:   meta.RequestedAt,
		Accepted:      accepted,
		Status:        strings.TrimSpace(status),
		ErrorCode:     strings.TrimSpace(errorCode),
		ErrorMessage:  strings.TrimSpace(errorMessage),
		StateBefore:   strings.TrimSpace(stateBefore),
		StateAfter:    strings.TrimSpace(stateAfter),
		DurationMS:    duration.Milliseconds(),
		TriggerKey:    meta.TriggerKey,
		Payload:       payload,
		Result:        result,
	})
}
