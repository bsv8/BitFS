package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

type directTransferPoolSessionFactInput struct {
	SessionID          string
	PoolScheme         string
	CounterpartyPubHex string
	SellerPubHex       string
	ArbiterPubHex      string
	GatewayPubHex      string
	PoolAmountSat      uint64
	SpendTxFeeSat      uint64
	FeeRateSatByte     float64
	LockBlocks         uint32
	OpenBaseTxID       string
	Status             string
	CreatedAtUnix      int64
	UpdatedAtUnix      int64
}

type directTransferPoolAllocationFactInput struct {
	SessionID        string
	AllocationKind   string
	SequenceNum      uint32
	PayeeAmountAfter uint64
	PayerAmountAfter uint64
	TxID             string
	TxHex            string
	CreatedAtUnix    int64
	UTXOFacts        []chainPaymentUTXOLinkEntry // Step 4 出项关联：UTXO 消耗明细
}

func dbUpsertDirectTransferPoolSessionTx(tx *sql.Tx, in directTransferPoolSessionFactInput) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := in.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}
	poolScheme := strings.TrimSpace(in.PoolScheme)
	if poolScheme == "" {
		poolScheme = "2of3"
	}
	status := strings.TrimSpace(in.Status)
	if status == "" {
		status = "active"
	}
	_, err := tx.Exec(
		`INSERT INTO fact_pool_sessions(
			pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
			pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(pool_session_id) DO UPDATE SET
			pool_scheme=excluded.pool_scheme,
			counterparty_pubkey_hex=excluded.counterparty_pubkey_hex,
			seller_pubkey_hex=excluded.seller_pubkey_hex,
			arbiter_pubkey_hex=excluded.arbiter_pubkey_hex,
			gateway_pubkey_hex=excluded.gateway_pubkey_hex,
			pool_amount_satoshi=excluded.pool_amount_satoshi,
			spend_tx_fee_satoshi=excluded.spend_tx_fee_satoshi,
			fee_rate_sat_byte=excluded.fee_rate_sat_byte,
			lock_blocks=excluded.lock_blocks,
			open_base_txid=excluded.open_base_txid,
			status=excluded.status,
			updated_at_unix=excluded.updated_at_unix`,
		sessionID,
		poolScheme,
		strings.ToLower(strings.TrimSpace(in.CounterpartyPubHex)),
		strings.ToLower(strings.TrimSpace(in.SellerPubHex)),
		strings.ToLower(strings.TrimSpace(in.ArbiterPubHex)),
		strings.ToLower(strings.TrimSpace(in.GatewayPubHex)),
		in.PoolAmountSat,
		in.SpendTxFeeSat,
		in.FeeRateSatByte,
		in.LockBlocks,
		strings.ToLower(strings.TrimSpace(in.OpenBaseTxID)),
		status,
		createdAt,
		updatedAt,
	)
	return err
}

func dbUpsertDirectTransferPoolAllocationTx(tx *sql.Tx, in directTransferPoolAllocationFactInput) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	kind := strings.TrimSpace(in.AllocationKind)
	if kind == "" {
		return fmt.Errorf("allocation_kind is required")
	}
	txID := strings.ToLower(strings.TrimSpace(in.TxID))
	if txID == "" {
		return fmt.Errorf("txid is required")
	}
	txHex := strings.ToLower(strings.TrimSpace(in.TxHex))
	if txHex == "" {
		return fmt.Errorf("tx_hex is required")
	}
	allocID := directTransferPoolAllocationID(sessionID, kind, in.SequenceNum)
	if allocID == "" {
		return fmt.Errorf("allocation_id is required")
	}
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	var allocationNo int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(allocation_no),0)+1 FROM fact_pool_session_events WHERE pool_session_id=? AND event_kind='pool_event'`,
		sessionID,
	).Scan(&allocationNo); err != nil {
		return err
	}
	_, err := tx.Exec(
		`INSERT INTO fact_pool_session_events(
			allocation_id,pool_session_id,allocation_no,allocation_kind,event_kind,sequence_num,state,direction,amount_satoshi,purpose,note,msg_id,cycle_index,payee_amount_after,payer_amount_after,txid,tx_hex,gateway_pubkey_hex,created_at_unix,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(allocation_id) DO UPDATE SET
			pool_session_id=excluded.pool_session_id,
			allocation_kind=excluded.allocation_kind,
			event_kind=excluded.event_kind,
			sequence_num=excluded.sequence_num,
			state=excluded.state,
			payee_amount_after=excluded.payee_amount_after,
			payer_amount_after=excluded.payer_amount_after,
			txid=excluded.txid,
			tx_hex=excluded.tx_hex,
			created_at_unix=excluded.created_at_unix,
			payload_json=excluded.payload_json`,
		allocID,
		sessionID,
		allocationNo,
		kind,
		"pool_event",
		in.SequenceNum,
		"confirmed",
		"",
		0,
		kind,
		"",
		"",
		0,
		in.PayeeAmountAfter,
		in.PayerAmountAfter,
		txID,
		txHex,
		"",
		createdAt,
		"{}",
	)
	if err != nil {
		return err
	}

	// Step 4 出项关联：对 input UTXO 按资产类型分流写入新消费表
	// B组改造：
	// - BSV 消耗写入 fact_bsv_consumptions
	// - Token 消耗写入 fact_token_consumptions + fact_token_utxo_links
	// - 只写新消费表，不回到旧消费表语义
	poolAllocID, err := dbGetPoolAllocationIDByAllocationIDTx(tx, allocID)
	if err != nil {
		return fmt.Errorf("lookup pool allocation id for alloc %s: %w", allocID, err)
	}
	// 结算周期必须先落地；这是写账本的锚点，不允许只写消耗不写 cycle。
	cycleID := fmt.Sprintf("cycle_pool_%d", poolAllocID)
	if err := dbUpsertSettlementCycle(tx,
		cycleID, "pool", "confirmed",
		poolAllocID, 0,
		0, 0, 0,
		0, createdAt, "auto-created from pool allocation", map[string]any{},
	); err != nil {
		return fmt.Errorf("upsert settlement cycle for pool allocation %d: %w", poolAllocID, err)
	}
	if len(in.UTXOFacts) > 0 {
		bsvFacts, tokenFacts, err := splitPoolAllocationUTXOFactsByAssetKind(tx, in.UTXOFacts)
		if err != nil {
			return fmt.Errorf("split utxo facts by asset kind: %w", err)
		}
		if len(bsvFacts) > 0 {
			if err := dbAppendBSVConsumptionsForPoolAllocation(tx, poolAllocID, bsvFacts, createdAt); err != nil {
				return fmt.Errorf("append BSV consumptions for pool allocation %s: %w", allocID, err)
			}
		}
		if len(tokenFacts) > 0 {
			if err := dbAppendTokenConsumptionsForPoolAllocation(tx, poolAllocID, tokenFacts, createdAt); err != nil {
				return fmt.Errorf("append token consumptions for pool allocation %s: %w", allocID, err)
			}
		}
	}
	return nil
}

// splitPoolAllocationUTXOFactsByAssetKind 按资产类型分流 Pool Allocation UTXO facts
// B组改造：从数据库查询 UTXO 的 asset_kind（当入参 AssetKind 为空时）
// 如果查不到记录，默认当作 BSV 处理（Pool 场景主要处理 BSV）
func splitPoolAllocationUTXOFactsByAssetKind(db sqlConn, facts []chainPaymentUTXOLinkEntry) ([]chainPaymentUTXOLinkEntry, []chainPaymentUTXOLinkEntry, error) {
	bsvFacts := make([]chainPaymentUTXOLinkEntry, 0)
	tokenFacts := make([]chainPaymentUTXOLinkEntry, 0)
	for _, fact := range facts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		assetKind := strings.TrimSpace(fact.AssetKind)
		if assetKind == "" {
			var dbAssetKind, tokenID, quantityText string
			err := db.QueryRow(
				`SELECT asset_kind, token_id, quantity_text FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN' LIMIT 1`,
				utxoID,
			).Scan(&dbAssetKind, &tokenID, &quantityText)
			if err != nil && err != sql.ErrNoRows {
				return nil, nil, fmt.Errorf("lookup asset kind for utxo %s: %w", utxoID, err)
			}
			if err == nil {
				assetKind = dbAssetKind
				fact.TokenID = tokenID
				fact.QuantityText = quantityText
			} else {
				assetKind = "BSV"
			}
		}
		if assetKind == "BSV" {
			bsvFacts = append(bsvFacts, fact)
		} else if assetKind == "BSV20" || assetKind == "BSV21" {
			fact.TokenStandard = assetKind
			tokenFacts = append(tokenFacts, fact)
		}
	}
	return bsvFacts, tokenFacts, nil
}

func dbUpsertDirectTransferPoolSession(ctx context.Context, store *clientDB, in directTransferPoolSessionFactInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		return dbUpsertDirectTransferPoolSessionTx(tx, in)
	})
}

func dbUpsertDirectTransferPoolAllocation(ctx context.Context, store *clientDB, in directTransferPoolAllocationFactInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		return dbUpsertDirectTransferPoolAllocationTx(tx, in)
	})
}

func directTransferPoolAllocationID(sessionID string, allocationKind string, sequenceNum uint32) string {
	sessionID = strings.TrimSpace(sessionID)
	allocationKind = strings.TrimSpace(allocationKind)
	if sessionID == "" || allocationKind == "" {
		return ""
	}
	return "poolalloc_" + sessionID + "_" + allocationKind + "_" + fmt.Sprint(sequenceNum)
}

func directTransferPoolTxIDFromHex(txHex string) (string, error) {
	if strings.TrimSpace(txHex) == "" {
		return "", fmt.Errorf("tx hex is required")
	}
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(parsed.TxID().String())), nil
}

// dbGetPoolAllocationIDByAllocationIDDB 按 allocation_id 查 fact_pool_session_events.id
// 设计说明：
// - 写入层和读层都只认事实表自增主键；
// - allocation_id 只作为旧入口和 payload 保留，不再直接承担 source_id 语义。
func dbGetPoolAllocationIDByAllocationIDDB(db *sql.DB, allocationID string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return 0, fmt.Errorf("allocation_id is required")
	}
	var id int64
	err := db.QueryRow(
		`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`,
		allocationID,
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// dbGetPoolAllocationIDByAllocationID 按 allocation_id 查自增 id
// 设计说明：财务来源已经收口到事实表自增主键，不再依赖业务键。
func dbGetPoolAllocationIDByAllocationID(ctx context.Context, store *clientDB, allocationID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbGetPoolAllocationIDByAllocationIDDB(db, allocationID)
	})
}

// dbGetPoolAllocationIDByAllocationIDTx 在事务内按 allocation_id 查自增 id
// 用于财务写入时在同一事务内获取主键
func dbGetPoolAllocationIDByAllocationIDTx(tx *sql.Tx, allocationID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return 0, fmt.Errorf("allocation_id is required")
	}
	var id int64
	err := tx.QueryRow(
		`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`,
		allocationID,
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// dbExtractUTXOFactsFromTxHex 从交易 hex 中提取 input UTXO facts
// 设计说明：
// - pool allocation 的 UTXO 明细从交易本身解析，不依赖外部传入
// - 只提取 input 方向，用于后续新消费表分流
func dbExtractUTXOFactsFromTxHex(txHex string, now int64) ([]chainPaymentUTXOLinkEntry, error) {
	txHex = strings.TrimSpace(txHex)
	if txHex == "" {
		return nil, nil
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return nil, err
	}
	inputs := parsed.Inputs
	if len(inputs) == 0 {
		return nil, nil
	}
	out := make([]chainPaymentUTXOLinkEntry, 0, len(inputs))
	for i, inp := range inputs {
		sourceTxID := strings.ToLower(inp.SourceTXID.String())
		utxoID := sourceTxID + ":" + fmt.Sprint(inp.SourceTxOutIndex)
		out = append(out, chainPaymentUTXOLinkEntry{
			UTXOID:        utxoID,
			IOSide:        "input",
			UTXORole:      "wallet_input",
			AmountSatoshi: 0, // pool tx 中 input 金额由后续 chain sync 确认
			CreatedAtUnix: now,
			Note:          fmt.Sprintf("pool tx input #%d", i),
		})
	}
	return out, nil
}
