package inboxmessage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type inboxMessageStoreAdapter struct {
	store moduleapi.Store
}

func (a inboxMessageStoreAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.store.ExecContext(ctx, query, args...)
}

func (a inboxMessageStoreAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.store.QueryContext(ctx, query, args...)
}

func (a inboxMessageStoreAdapter) Do(ctx context.Context, fn func(Conn) error) error {
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

func (a inboxMessageStoreAdapter) SerialAccess() bool {
	return a.store != nil && a.store.SerialAccess()
}

type inboxMessageSerialExecutor struct {
	store moduleapi.Store
}

func (a inboxMessageSerialExecutor) Do(ctx context.Context, fn func(Conn) error) error {
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

func openInboxMessageStore(ctx context.Context, host moduleapi.Host) (Store, error) {
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

	return moduleStore, nil
}

func serialExecutorForInboxStore(store moduleapi.Store) SerialExecutor {
	if store == nil || !store.SerialAccess() {
		return nil
	}
	return inboxMessageSerialExecutor{store: store}
}

func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	moduleStore, err := openInboxMessageStore(ctx, host)
	if err != nil {
		return nil, err
	}

	return host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleID,
		Version: CapabilityVersion,
		HTTP: []moduleapi.HTTPRoute{
			{Path: "/v1/settings/inbox/messages", Handler: handleInboxMessagesSettings(moduleStore)},
			{Path: "/v1/settings/inbox/messages/detail", Handler: handleInboxMessageDetailSettings(moduleStore)},
		},
		LibP2P: []moduleapi.LibP2PRoute{
			{Protocol: moduleapi.LibP2PProtocolNodeCall, Route: InboxMessageRoute, Handler: receiveInboxMessage(moduleStore)},
		},
	})
}
