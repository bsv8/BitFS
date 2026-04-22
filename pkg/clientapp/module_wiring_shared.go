package clientapp

import (
	"context"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type moduleBootstrapStore interface {
	Read(ctx context.Context, fn func(moduleapi.ReadConn) error) error
	WriteTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error
}