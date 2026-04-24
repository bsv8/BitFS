package chain_tx_v1

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/chain_tx_v1/storedb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/chain_tx_v1/storedb/gen/chaintxv1payments"
)

// Store 是 chain_tx_v1 模块的数据库能力封装。
type Store interface {
	RecordPayment(ctx context.Context, payment PaymentRow) error
	GetPaymentByRef(ctx context.Context, refID string) (PaymentRow, bool, error)
}

// PaymentRow 对齐 storedb/gen 的 chain_tx_v1_payments 表。
type PaymentRow struct {
	PaymentRefID   string
	TargetPeerHex  string
	AmountSatoshi  uint64
	State          string
	Txid           string
	TxHex          string
	MinerFeeSat    uint64
	Route          string
	QuoteHash      string
	ReceiptScheme  string
	ReceiptHash    string
	ConfirmedAtUnix int64
	ErrorMessage   string
	CreatedAtUnix  int64
	UpdatedAtUnix  int64
}

// chainTxStore 是 Store 的实现。内部持有 moduleapi.Store，通过 Read/WriteTx 进入 BitFS 读写通道。
type chainTxStore struct {
	store moduleapi.Store
}

// newChainTxStore 创建 chain_tx_v1 的 store 实例。
func newChainTxStore(store moduleapi.Store) *chainTxStore {
	return &chainTxStore{store: store}
}

type readOnlyQuerier struct {
	conn moduleapi.ReadConn
}

func (q readOnlyQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, fmt.Errorf("read-only connection does not support exec")
}

func (q readOnlyQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.conn.QueryContext(ctx, query, args...)
}

func (s *chainTxStore) RecordPayment(ctx context.Context, payment PaymentRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		_, err := client.ChainTxV1Payments.Create().
			SetPaymentRefID(payment.PaymentRefID).
			SetTargetPeerHex(payment.TargetPeerHex).
			SetAmountSatoshi(int(payment.AmountSatoshi)).
			SetState(payment.State).
			SetTxid(payment.Txid).
			SetTxHex(payment.TxHex).
			SetMinerFeeSatoshi(int(payment.MinerFeeSat)).
			SetRoute(payment.Route).
			SetQuoteHash(payment.QuoteHash).
			SetReceiptScheme(payment.ReceiptScheme).
			SetReceiptHash(payment.ReceiptHash).
			SetConfirmedAtUnix(payment.ConfirmedAtUnix).
			SetErrorMessage(payment.ErrorMessage).
			SetCreatedAtUnix(payment.CreatedAtUnix).
			SetUpdatedAtUnix(payment.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *chainTxStore) GetPaymentByRef(ctx context.Context, refID string) (PaymentRow, bool, error) {
	if s.store == nil {
		return PaymentRow{}, false, fmt.Errorf("store not available")
	}
	var result PaymentRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.ChainTxV1Payments.Query().
			Where(chaintxv1payments.PaymentRefIDEQ(refID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		result = toPaymentRow(row)
		return nil
	})
	if err != nil {
		return PaymentRow{}, false, err
	}
	if result.PaymentRefID == "" {
		return PaymentRow{}, false, nil
	}
	return result, true, nil
}

func toPaymentRow(r *gen.ChainTxV1Payments) PaymentRow {
	return PaymentRow{
		PaymentRefID:   r.PaymentRefID,
		TargetPeerHex:  r.TargetPeerHex,
		AmountSatoshi:  uint64(r.AmountSatoshi),
		State:          r.State,
		Txid:           r.Txid,
		TxHex:          r.TxHex,
		MinerFeeSat:    uint64(r.MinerFeeSatoshi),
		Route:          r.Route,
		QuoteHash:      r.QuoteHash,
		ReceiptScheme: r.ReceiptScheme,
		ReceiptHash:   r.ReceiptHash,
		ConfirmedAtUnix: r.ConfirmedAtUnix,
		ErrorMessage:   r.ErrorMessage,
		CreatedAtUnix:  r.CreatedAtUnix,
		UpdatedAtUnix:  r.UpdatedAtUnix,
	}
}