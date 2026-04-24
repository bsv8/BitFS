package pool_2of2_v1

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of2_v1/storedb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of2_v1/storedb/gen/pool2of2v1entries"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/pool_2of2_v1/storedb/gen/pool2of2v1sessions"
)

// Store 是 pool_2of2_v1 模块的数据库能力封装。
type Store interface {
	EnsureSession(ctx context.Context, session SessionRow) error
	GetActiveSession(ctx context.Context, counterpartyHex string) (SessionRow, bool, error)
	GetSessionByRef(ctx context.Context, sessionRef string) (SessionRow, bool, error)
	UpdateSession(ctx context.Context, sessionRef string, update func(*SessionRow)) error
	AppendEntry(ctx context.Context, entry EntryRow) error
	ListEntries(ctx context.Context, sessionRef string) ([]EntryRow, error)
	ApplyPayment(ctx context.Context, sessionRef string, newSequence uint32, newServerAmount uint64, newSpendTxID string, entry EntryRow) error
}

// SessionRow 对齐 storedb/gen 的 pool_2of2_v1_sessions 表。
type SessionRow struct {
	Ref               string
	CounterpartyHex   string
	SpendTxID         string
	ClientAmountSat   uint64
	ServerAmountSat   uint64
	SequenceNum       uint32
	State             string
	CreatedAtUnix     int64
	UpdatedAtUnix     int64
	ClosedAtUnix      int64
}

// EntryRow 对齐 storedb/gen 的 pool_2of2_v1_entries 表。
type EntryRow struct {
	SessionRef          string
	ChargeReason        string
	ChargeAmountSat     uint64
	BalanceAfterSat     uint64
	SequenceNum         uint32
	ProofTxID           string
	ProofAtUnix         int64
	CreatedAtUnix       int64
}

// poolStore 是 Store 的实现。内部持有 moduleapi.Store，通过 Read/WriteTx 进入 BitFS 读写通道。
type poolStore struct {
	store moduleapi.Store
}

// newPoolStore 创建 pool_2of2_v1 的 store 实例。
func newPoolStore(store moduleapi.Store) *poolStore {
	return &poolStore{store: store}
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

func (s *poolStore) EnsureSession(ctx context.Context, session SessionRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		_, err := client.Pool2of2V1Sessions.Create().
			SetSessionRef(session.Ref).
			SetCounterpartyHex(session.CounterpartyHex).
			SetSpendTxid(session.SpendTxID).
			SetClientAmountSatoshi(int(session.ClientAmountSat)).
			SetServerAmountSatoshi(int(session.ServerAmountSat)).
			SetSequenceNum(int(session.SequenceNum)).
			SetState(session.State).
			SetCreatedAtUnix(session.CreatedAtUnix).
			SetUpdatedAtUnix(session.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *poolStore) GetActiveSession(ctx context.Context, counterpartyHex string) (SessionRow, bool, error) {
	if s.store == nil {
		return SessionRow{}, false, fmt.Errorf("store not available")
	}
	var result SessionRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.Pool2of2V1Sessions.Query().
			Where(pool2of2v1sessions.CounterpartyHexEQ(counterpartyHex)).
			Where(pool2of2v1sessions.StateIn("pending_open", "active")).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		result = toSessionRow(row)
		return nil
	})
	if err != nil {
		return SessionRow{}, false, err
	}
	if result.Ref == "" {
		return SessionRow{}, false, nil
	}
	return result, true, nil
}

func (s *poolStore) GetSessionByRef(ctx context.Context, sessionRef string) (SessionRow, bool, error) {
	if s.store == nil {
		return SessionRow{}, false, fmt.Errorf("store not available")
	}
	var result SessionRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.Pool2of2V1Sessions.Query().
			Where(pool2of2v1sessions.SessionRefEQ(sessionRef)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		result = toSessionRow(row)
		return nil
	})
	if err != nil {
		return SessionRow{}, false, err
	}
	if result.Ref == "" {
		return SessionRow{}, false, nil
	}
	return result, true, nil
}

func (s *poolStore) UpdateSession(ctx context.Context, sessionRef string, update func(*SessionRow)) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		row, err := client.Pool2of2V1Sessions.Query().
			Where(pool2of2v1sessions.SessionRefEQ(sessionRef)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return moduleapi.SessionNotFound(sessionRef)
			}
			return err
		}
		sess := toSessionRow(row)
		update(&sess)
		_, err = client.Pool2of2V1Sessions.UpdateOne(row).
			SetClientAmountSatoshi(int(sess.ClientAmountSat)).
			SetServerAmountSatoshi(int(sess.ServerAmountSat)).
			SetSequenceNum(int(sess.SequenceNum)).
			SetState(sess.State).
			SetUpdatedAtUnix(sess.UpdatedAtUnix).
			SetClosedAtUnix(sess.ClosedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *poolStore) AppendEntry(ctx context.Context, entry EntryRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		_, err := client.Pool2of2V1Entries.Create().
			SetSessionRef(entry.SessionRef).
			SetChargeReason(entry.ChargeReason).
			SetChargeAmountSatoshi(int(entry.ChargeAmountSat)).
			SetBalanceAfterSatoshi(int(entry.BalanceAfterSat)).
			SetSequenceNum(int(entry.SequenceNum)).
			SetProofTxid(entry.ProofTxID).
			SetProofAtUnix(entry.ProofAtUnix).
			SetCreatedAtUnix(entry.CreatedAtUnix).
			Save(ctx)
		return err
	})
}

func (s *poolStore) ListEntries(ctx context.Context, sessionRef string) ([]EntryRow, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store not available")
	}
	var result []EntryRow
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		rows, err := client.Pool2of2V1Entries.Query().
			Where(pool2of2v1entries.SessionRefEQ(sessionRef)).
			All(ctx)
		if err != nil {
			return err
		}
		result = make([]EntryRow, len(rows))
		for i, r := range rows {
			result[i] = toEntryRow(r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *poolStore) ApplyPayment(ctx context.Context, sessionRef string, newSequence uint32, newServerAmount uint64, newSpendTxID string, entry EntryRow) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))

		sessionRow, err := client.Pool2of2V1Sessions.Query().
			Where(pool2of2v1sessions.SessionRefEQ(sessionRef)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return moduleapi.SessionNotFound(sessionRef)
			}
			return err
		}

		currentSeq := uint32(sessionRow.SequenceNum)
		if currentSeq != entry.SequenceNum {
			return fmt.Errorf("sequence mismatch: current=%d, entry=%d", currentSeq, entry.SequenceNum)
		}
		if sessionRow.State != "active" {
			return fmt.Errorf("session not active: state=%s", sessionRow.State)
		}

		now := time.Now().Unix()
		_, err = client.Pool2of2V1Sessions.UpdateOne(sessionRow).
			SetSequenceNum(int(newSequence)).
			SetServerAmountSatoshi(int(newServerAmount)).
			SetSpendTxid(newSpendTxID).
			SetUpdatedAtUnix(now).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("update session failed: %w", err)
		}

		_, err = client.Pool2of2V1Entries.Create().
			SetSessionRef(entry.SessionRef).
			SetChargeReason(entry.ChargeReason).
			SetChargeAmountSatoshi(int(entry.ChargeAmountSat)).
			SetBalanceAfterSatoshi(int(entry.BalanceAfterSat)).
			SetSequenceNum(int(entry.SequenceNum)).
			SetProofTxid(entry.ProofTxID).
			SetProofAtUnix(entry.ProofAtUnix).
			SetCreatedAtUnix(entry.CreatedAtUnix).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("append entry failed: %w", err)
		}

		return nil
	})
}

func toSessionRow(r *gen.Pool2of2V1Sessions) SessionRow {
	return SessionRow{
		Ref:              r.SessionRef,
		CounterpartyHex:  r.CounterpartyHex,
		SpendTxID:        r.SpendTxid,
		ClientAmountSat:  uint64(r.ClientAmountSatoshi),
		ServerAmountSat:  uint64(r.ServerAmountSatoshi),
		SequenceNum:      uint32(r.SequenceNum),
		State:            r.State,
		CreatedAtUnix:    r.CreatedAtUnix,
		UpdatedAtUnix:    r.UpdatedAtUnix,
		ClosedAtUnix:     r.ClosedAtUnix,
	}
}

func toEntryRow(r *gen.Pool2of2V1Entries) EntryRow {
	return EntryRow{
		SessionRef:      r.SessionRef,
		ChargeReason:   r.ChargeReason,
		ChargeAmountSat: uint64(r.ChargeAmountSatoshi),
		BalanceAfterSat: uint64(r.BalanceAfterSatoshi),
		SequenceNum:    uint32(r.SequenceNum),
		ProofTxID:      r.ProofTxid,
		ProofAtUnix:    r.ProofAtUnix,
		CreatedAtUnix:  r.CreatedAtUnix,
	}
}