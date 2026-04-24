package pool_2of3_v1

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of3_v1/storedb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of3_v1/storedb/gen/pool2of3v1arbitrations"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of3_v1/storedb/gen/pool2of3v1entries"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of3_v1/storedb/gen/pool2of3v1sessions"
)

// Store 是 pool_2of3_v1 模块的数据库能力封装。
type Store interface {
	OpenSession(ctx context.Context, session TradeSessionRow) error
	GetSessionByRef(ctx context.Context, sessionRef string) (TradeSessionRow, bool, error)
	UpdateSession(ctx context.Context, sessionRef string, update func(*TradeSessionRow)) error

	AppendEntry(ctx context.Context, entry TradeEntryRow) error
	GetEntriesBySession(ctx context.Context, sessionRef string) ([]TradeEntryRow, error)

	OpenArbitration(ctx context.Context, arb ArbitrationRow) error
	GetArbitrationByCaseID(ctx context.Context, caseID string) (ArbitrationRow, bool, error)
	UpdateArbitration(ctx context.Context, caseID string, update func(*ArbitrationRow)) error
}

// TradeSessionRow 对应 pool_2of3_v1_sessions 表。
type TradeSessionRow struct {
	Ref                   string
	TradeID               string
	BuyerHex              string
	SellerHex             string
	ArbiterHex            string
	TotalAmountSatoshi    uint64
	PaidAmountSatoshi     uint64
	RefundedAmountSatoshi uint64
	ChunkCount            uint32
	State                 string
	CurrentTxid           string
	CreatedAtUnix         int64
	UpdatedAtUnix         int64
	ClosedAtUnix          int64
}

// TradeEntryRow 对应 pool_2of3_v1_entries 表。
type TradeEntryRow struct {
	SessionRef        string
	ChunkIndex        uint32
	AmountSatoshi    uint64
	State            string
	PaymentTxid       string
	PaidAtUnix        int64
	ArbitrationCaseID string
	ArbitratedAtUnix  int64
	AwardTxid        string
	CreatedAtUnix     int64
	UpdatedAtUnix     int64
}

// ArbitrationRow 对应 pool_2of3_v1_arbitrations 表。
type ArbitrationRow struct {
	SessionRef    string
	ChunkIndex    uint32
	CaseID        string
	State         string
	Evidence      string
	AwardTxid     string
	DecidedAtUnix int64
	CreatedAtUnix int64
	UpdatedAtUnix int64
}

// tradeStore 是 Store 的实现。内部持有 moduleapi.Store，通过 Read/WriteTx 进入 BitFS 读写通道。
type tradeStore struct {
	store moduleapi.Store
}

// newTradeStore 创建 pool_2of3_v1 的 store 实例。
func newTradeStore(store moduleapi.Store) *tradeStore {
	return &tradeStore{store: store}
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

func (s *tradeStore) OpenSession(ctx context.Context, session TradeSessionRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		_, err := client.Pool2of3V1Sessions.Create().
			SetSessionRef(session.Ref).
			SetTradeID(session.TradeID).
			SetBuyerHex(session.BuyerHex).
			SetSellerHex(session.SellerHex).
			SetArbiterHex(session.ArbiterHex).
			SetTotalAmountSatoshi(int(session.TotalAmountSatoshi)).
			SetPaidAmountSatoshi(int(session.PaidAmountSatoshi)).
			SetRefundedAmountSatoshi(int(session.RefundedAmountSatoshi)).
			SetChunkCount(int(session.ChunkCount)).
			SetState(session.State).
			SetCurrentTxid(session.CurrentTxid).
			SetCreatedAtUnix(session.CreatedAtUnix).
			SetUpdatedAtUnix(session.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *tradeStore) GetSessionByRef(ctx context.Context, sessionRef string) (TradeSessionRow, bool, error) {
	if s.store == nil {
		return TradeSessionRow{}, false, fmt.Errorf("store not available")
	}
	var result TradeSessionRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.Pool2of3V1Sessions.Query().
			Where(pool2of3v1sessions.SessionRefEQ(sessionRef)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		result = toTradeSessionRow(row)
		return nil
	})
	if err != nil {
		return TradeSessionRow{}, false, err
	}
	if result.Ref == "" {
		return TradeSessionRow{}, false, nil
	}
	return result, true, nil
}

func (s *tradeStore) UpdateSession(ctx context.Context, sessionRef string, update func(*TradeSessionRow)) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		row, err := client.Pool2of3V1Sessions.Query().
			Where(pool2of3v1sessions.SessionRefEQ(sessionRef)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return moduleapi.SessionNotFound(sessionRef)
			}
			return err
		}
		sess := toTradeSessionRow(row)
		update(&sess)
		_, err = client.Pool2of3V1Sessions.UpdateOne(row).
			SetPaidAmountSatoshi(int(sess.PaidAmountSatoshi)).
			SetRefundedAmountSatoshi(int(sess.RefundedAmountSatoshi)).
			SetState(sess.State).
			SetCurrentTxid(sess.CurrentTxid).
			SetUpdatedAtUnix(sess.UpdatedAtUnix).
			SetClosedAtUnix(sess.ClosedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *tradeStore) AppendEntry(ctx context.Context, entry TradeEntryRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		_, err := client.Pool2of3V1Entries.Create().
			SetSessionRef(entry.SessionRef).
			SetChunkIndex(int(entry.ChunkIndex)).
			SetAmountSatoshi(int(entry.AmountSatoshi)).
			SetState(entry.State).
			SetPaymentTxid(entry.PaymentTxid).
			SetPaidAtUnix(entry.PaidAtUnix).
			SetArbitrationCaseID(entry.ArbitrationCaseID).
			SetArbitratedAtUnix(entry.ArbitratedAtUnix).
			SetAwardTxid(entry.AwardTxid).
			SetCreatedAtUnix(entry.CreatedAtUnix).
			SetUpdatedAtUnix(entry.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *tradeStore) GetEntriesBySession(ctx context.Context, sessionRef string) ([]TradeEntryRow, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store not available")
	}
	var result []TradeEntryRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		rows, err := client.Pool2of3V1Entries.Query().
			Where(pool2of3v1entries.SessionRefEQ(sessionRef)).
			All(ctx)
		if err != nil {
			return err
		}
		result = make([]TradeEntryRow, len(rows))
		for i, r := range rows {
			result[i] = toTradeEntryRow(r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *tradeStore) OpenArbitration(ctx context.Context, arb ArbitrationRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		_, err := client.Pool2of3V1Arbitrations.Create().
			SetSessionRef(arb.SessionRef).
			SetChunkIndex(int(arb.ChunkIndex)).
			SetCaseID(arb.CaseID).
			SetState(arb.State).
			SetEvidence(arb.Evidence).
			SetAwardTxid(arb.AwardTxid).
			SetDecidedAtUnix(arb.DecidedAtUnix).
			SetCreatedAtUnix(arb.CreatedAtUnix).
			SetUpdatedAtUnix(arb.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *tradeStore) GetArbitrationByCaseID(ctx context.Context, caseID string) (ArbitrationRow, bool, error) {
	if s.store == nil {
		return ArbitrationRow{}, false, fmt.Errorf("store not available")
	}
	var result ArbitrationRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.Pool2of3V1Arbitrations.Query().
			Where(pool2of3v1arbitrations.CaseIDEQ(caseID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		result = toArbitrationRow(row)
		return nil
	})
	if err != nil {
		return ArbitrationRow{}, false, err
	}
	if result.CaseID == "" {
		return ArbitrationRow{}, false, nil
	}
	return result, true, nil
}

func (s *tradeStore) UpdateArbitration(ctx context.Context, caseID string, update func(*ArbitrationRow)) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		row, err := client.Pool2of3V1Arbitrations.Query().
			Where(pool2of3v1arbitrations.CaseIDEQ(caseID)).
			Only(ctx)
		if err != nil {
		if gen.IsNotFound(err) {
			return moduleapi.SessionNotFound(caseID)
		}
			return err
		}
		arb := toArbitrationRow(row)
		update(&arb)
		_, err = client.Pool2of3V1Arbitrations.UpdateOne(row).
			SetState(arb.State).
			SetAwardTxid(arb.AwardTxid).
			SetDecidedAtUnix(arb.DecidedAtUnix).
			SetUpdatedAtUnix(arb.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func toTradeSessionRow(r *gen.Pool2of3V1Sessions) TradeSessionRow {
	return TradeSessionRow{
		Ref:                   r.SessionRef,
		TradeID:               r.TradeID,
		BuyerHex:              r.BuyerHex,
		SellerHex:             r.SellerHex,
		ArbiterHex:            r.ArbiterHex,
		TotalAmountSatoshi:    uint64(r.TotalAmountSatoshi),
		PaidAmountSatoshi:     uint64(r.PaidAmountSatoshi),
		RefundedAmountSatoshi: uint64(r.RefundedAmountSatoshi),
		ChunkCount:            uint32(r.ChunkCount),
		State:                 r.State,
		CurrentTxid:           r.CurrentTxid,
		CreatedAtUnix:         r.CreatedAtUnix,
		UpdatedAtUnix:         r.UpdatedAtUnix,
		ClosedAtUnix:          r.ClosedAtUnix,
	}
}

func toTradeEntryRow(r *gen.Pool2of3V1Entries) TradeEntryRow {
	return TradeEntryRow{
		SessionRef:        r.SessionRef,
		ChunkIndex:       uint32(r.ChunkIndex),
		AmountSatoshi:    uint64(r.AmountSatoshi),
		State:            r.State,
		PaymentTxid:      r.PaymentTxid,
		PaidAtUnix:       r.PaidAtUnix,
		ArbitrationCaseID: r.ArbitrationCaseID,
		ArbitratedAtUnix:  r.ArbitratedAtUnix,
		AwardTxid:        r.AwardTxid,
		CreatedAtUnix:    r.CreatedAtUnix,
		UpdatedAtUnix:     r.UpdatedAtUnix,
	}
}

func toArbitrationRow(r *gen.Pool2of3V1Arbitrations) ArbitrationRow {
	return ArbitrationRow{
		SessionRef:    r.SessionRef,
		ChunkIndex:   uint32(r.ChunkIndex),
		CaseID:       r.CaseID,
		State:        r.State,
		Evidence:     r.Evidence,
		AwardTxid:    r.AwardTxid,
		DecidedAtUnix: r.DecidedAtUnix,
		CreatedAtUnix: r.CreatedAtUnix,
		UpdatedAtUnix: r.UpdatedAtUnix,
	}
}