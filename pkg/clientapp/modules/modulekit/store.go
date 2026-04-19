package modulekit

import (
	"context"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func RequireStore(host moduleapi.Host) (moduleapi.Store, error) {
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}
	store := host.Store()
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	return store, nil
}

func Do(ctx context.Context, store moduleapi.Store, fn func(moduleapi.Conn) error) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	if fn == nil {
		return fmt.Errorf("fn is required")
	}
	return store.Do(ctx, fn)
}
