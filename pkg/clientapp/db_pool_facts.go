package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpool"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpoolallocations"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factpoolsessionevents"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factsettlementchannelpoolsessionquotepay"
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
	UTXOFacts        []chainPaymentUTXOLinkEntry // 兼容保留：旧消费明细输入，主路径不再直写 fact_consumptions
}

type directTransferBizPoolSnapshotInput struct {
	SessionID          string
	PoolScheme         string
	CounterpartyPubHex string
	SellerPubHex       string
	ArbiterPubHex      string
	GatewayPubHex      string
	PoolAmountSat      uint64
	SpendTxFeeSat      uint64
	AllocatedSat       uint64
	CycleFeeSat        uint64
	AvailableSat       uint64
	NextSequenceNum    uint32
	Status             string
	OpenBaseTxID       string
	OpenAllocationID   string
	CloseAllocationID  string
	CreatedAtUnix      int64
	UpdatedAtUnix      int64
}

type directTransferBizPoolAllocationInput struct {
	SessionID        string
	AllocationID     string
	AllocationNo     int64
	AllocationKind   string
	SequenceNum      uint32
	PayeeAmountAfter uint64
	PayerAmountAfter uint64
	TxID             string
	TxHex            string
	CreatedAtUnix    int64
}

func dbUpsertDirectTransferBizPoolSnapshotTx(ctx context.Context, tx EntWriteRoot, in directTransferBizPoolSnapshotInput) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	poolScheme := strings.TrimSpace(in.PoolScheme)
	if poolScheme == "" {
		poolScheme = "2of3"
	}
	status := strings.TrimSpace(in.Status)
	if status == "" {
		status = "active"
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
	nextSeq := in.NextSequenceNum
	if nextSeq == 0 {
		nextSeq = 1
	}

	existing, err := tx.BizPool.Query().
		Where(bizpool.PoolSessionIDEQ(sessionID)).
		Only(ctx)
	if err == nil {
		if int64(nextSeq) < existing.NextSequenceNum {
			return nil
		}
		openBaseTxid := strings.ToLower(strings.TrimSpace(in.OpenBaseTxID))
		if openBaseTxid == "" {
			openBaseTxid = strings.TrimSpace(existing.OpenBaseTxid)
		}
		openAllocationID := strings.TrimSpace(in.OpenAllocationID)
		if openAllocationID == "" {
			openAllocationID = strings.TrimSpace(existing.OpenAllocationID)
		}
		closeAllocationID := strings.TrimSpace(in.CloseAllocationID)
		if closeAllocationID == "" {
			closeAllocationID = strings.TrimSpace(existing.CloseAllocationID)
		}
		_, err = tx.BizPool.UpdateOneID(existing.ID).
			SetPoolScheme(poolScheme).
			SetCounterpartyPubkeyHex(strings.ToLower(strings.TrimSpace(in.CounterpartyPubHex))).
			SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(in.SellerPubHex))).
			SetArbiterPubkeyHex(strings.ToLower(strings.TrimSpace(in.ArbiterPubHex))).
			SetGatewayPubkeyHex(strings.ToLower(strings.TrimSpace(in.GatewayPubHex))).
			SetPoolAmountSatoshi(int64(in.PoolAmountSat)).
			SetSpendTxFeeSatoshi(int64(in.SpendTxFeeSat)).
			SetAllocatedSatoshi(int64(in.AllocatedSat)).
			SetCycleFeeSatoshi(int64(in.CycleFeeSat)).
			SetAvailableSatoshi(int64(in.AvailableSat)).
			SetNextSequenceNum(int64(nextSeq)).
			SetStatus(status).
			SetOpenBaseTxid(openBaseTxid).
			SetOpenAllocationID(openAllocationID).
			SetCloseAllocationID(closeAllocationID).
			SetUpdatedAtUnix(updatedAt).
			Save(ctx)
		return err
	}
	if err != nil && !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.BizPool.Create().
		SetPoolSessionID(sessionID).
		SetPoolScheme(poolScheme).
		SetCounterpartyPubkeyHex(strings.ToLower(strings.TrimSpace(in.CounterpartyPubHex))).
		SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(in.SellerPubHex))).
		SetArbiterPubkeyHex(strings.ToLower(strings.TrimSpace(in.ArbiterPubHex))).
		SetGatewayPubkeyHex(strings.ToLower(strings.TrimSpace(in.GatewayPubHex))).
		SetPoolAmountSatoshi(int64(in.PoolAmountSat)).
		SetSpendTxFeeSatoshi(int64(in.SpendTxFeeSat)).
		SetAllocatedSatoshi(int64(in.AllocatedSat)).
		SetCycleFeeSatoshi(int64(in.CycleFeeSat)).
		SetAvailableSatoshi(int64(in.AvailableSat)).
		SetNextSequenceNum(int64(nextSeq)).
		SetStatus(status).
		SetOpenBaseTxid(strings.ToLower(strings.TrimSpace(in.OpenBaseTxID))).
		SetOpenAllocationID(strings.TrimSpace(in.OpenAllocationID)).
		SetCloseAllocationID(strings.TrimSpace(in.CloseAllocationID)).
		SetCreatedAtUnix(createdAt).
		SetUpdatedAtUnix(updatedAt).
		Save(ctx)
	return err
}

func dbUpsertDirectTransferBizPoolAllocationTx(ctx context.Context, tx EntWriteRoot, in directTransferBizPoolAllocationInput) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	allocationID := strings.TrimSpace(in.AllocationID)
	if allocationID == "" {
		return fmt.Errorf("allocation_id is required")
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
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}

	existing, err := tx.BizPoolAllocations.Query().
		Where(bizpoolallocations.AllocationIDEQ(allocationID)).
		Only(ctx)
	if err == nil {
		_, err = tx.BizPoolAllocations.UpdateOneID(existing.ID).
			SetPoolSessionID(sessionID).
			SetAllocationKind(kind).
			SetSequenceNum(int64(in.SequenceNum)).
			SetPayeeAmountAfter(int64(in.PayeeAmountAfter)).
			SetPayerAmountAfter(int64(in.PayerAmountAfter)).
			SetTxid(txID).
			SetTxHex(txHex).
			SetCreatedAtUnix(createdAt).
			Save(ctx)
		return err
	}
	if err != nil && !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.BizPoolAllocations.Create().
		SetAllocationID(allocationID).
		SetPoolSessionID(sessionID).
		SetAllocationNo(in.AllocationNo).
		SetAllocationKind(kind).
		SetSequenceNum(int64(in.SequenceNum)).
		SetPayeeAmountAfter(int64(in.PayeeAmountAfter)).
		SetPayerAmountAfter(int64(in.PayerAmountAfter)).
		SetTxid(txID).
		SetTxHex(txHex).
		SetCreatedAtUnix(createdAt).
		Save(ctx)
	return err
}

func dbUpsertDirectTransferPoolSessionTx(ctx context.Context, tx EntWriteRoot, in directTransferPoolSessionFactInput) error {
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
	paymentAttemptState := "pending"
	if strings.EqualFold(status, "settled") || strings.EqualFold(status, "closed") || strings.EqualFold(status, "confirmed") {
		paymentAttemptState = "confirmed"
	}

	var channelID int64
	var settlementPaymentAttemptID int64
	channel, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
		Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(sessionID)).
		Only(ctx)
	if err == nil {
		channelID = int64(channel.ID)
		settlementPaymentAttemptID = channel.SettlementPaymentAttemptID
	} else if !gen.IsNotFound(err) {
		return err
	}
	if settlementPaymentAttemptID == 0 {
		pendingSourceID := "pending:pool_session_quote_pay:" + sessionID
		settlementPaymentAttemptID, err = dbUpsertSettlementPaymentAttemptEntTx(ctx, tx,
			"payment_attempt_pool_session_quote_pay_"+sessionID,
			"pool_session_quote_pay",
			pendingSourceID,
			paymentAttemptState,
			0, 0, 0, 0, updatedAt,
			"pre-bind pool session quote pay channel",
			map[string]any{"pool_session_id": sessionID, "status": status},
		)
		if err != nil {
			return fmt.Errorf("upsert settlement payment attempt shell for pool session channel: %w", err)
		}
	}

	txid := strings.ToLower(strings.TrimSpace(in.OpenBaseTxID))
	openBaseTxid := strings.ToLower(strings.TrimSpace(in.OpenBaseTxID))
	if channel != nil {
		_, err = tx.FactSettlementChannelPoolSessionQuotePay.UpdateOneID(channel.ID).
			SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
			SetTxid(txid).
			SetPoolScheme(poolScheme).
			SetCounterpartyPubkeyHex(strings.ToLower(strings.TrimSpace(in.CounterpartyPubHex))).
			SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(in.SellerPubHex))).
			SetArbiterPubkeyHex(strings.ToLower(strings.TrimSpace(in.ArbiterPubHex))).
			SetGatewayPubkeyHex(strings.ToLower(strings.TrimSpace(in.GatewayPubHex))).
			SetPoolAmountSatoshi(int64(in.PoolAmountSat)).
			SetSpendTxFeeSatoshi(int64(in.SpendTxFeeSat)).
			SetFeeRateSatByte(in.FeeRateSatByte).
			SetLockBlocks(int64(in.LockBlocks)).
			SetOpenBaseTxid(openBaseTxid).
			SetStatus(status).
			SetUpdatedAtUnix(updatedAt).
			Save(ctx)
		if err != nil {
			return err
		}
		channelID = int64(channel.ID)
	} else {
		node, err := tx.FactSettlementChannelPoolSessionQuotePay.Create().
			SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
			SetPoolSessionID(sessionID).
			SetTxid(txid).
			SetPoolScheme(poolScheme).
			SetCounterpartyPubkeyHex(strings.ToLower(strings.TrimSpace(in.CounterpartyPubHex))).
			SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(in.SellerPubHex))).
			SetArbiterPubkeyHex(strings.ToLower(strings.TrimSpace(in.ArbiterPubHex))).
			SetGatewayPubkeyHex(strings.ToLower(strings.TrimSpace(in.GatewayPubHex))).
			SetPoolAmountSatoshi(int64(in.PoolAmountSat)).
			SetSpendTxFeeSatoshi(int64(in.SpendTxFeeSat)).
			SetFeeRateSatByte(in.FeeRateSatByte).
			SetLockBlocks(int64(in.LockBlocks)).
			SetOpenBaseTxid(openBaseTxid).
			SetStatus(status).
			SetCreatedAtUnix(createdAt).
			SetUpdatedAtUnix(updatedAt).
			Save(ctx)
		if err != nil {
			return err
		}
		channelID = int64(node.ID)
	}

	_, err = tx.FactSettlementPaymentAttempts.UpdateOneID(settlementPaymentAttemptID).
		SetSourceType("pool_session_quote_pay").
		SetSourceID(fmt.Sprintf("%d", channelID)).
		Save(ctx)
	return err
}

func dbUpsertDirectTransferPoolAllocationTx(ctx context.Context, tx EntWriteRoot, in directTransferPoolAllocationFactInput) error {
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
	if IsPoolFactAllocationDisallowed(kind) {
		obs.Important(ServiceName, "pool_allocation_fact_write_deprecated", map[string]any{
			"session_id":          sessionID,
			"allocation_id":       allocID,
			"allocation_kind":     kind,
			"fact_event_kind":     PoolFactEventKindPoolEvent,
			"legacy_fact_path":    true,
			"deprecated_behavior": "pool allocation writes fact rows directly",
		})
	}
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}

	allocationNo := int64(1)
	last, err := tx.FactPoolSessionEvents.Query().
		Where(
			factpoolsessionevents.PoolSessionIDEQ(sessionID),
			factpoolsessionevents.EventKindEQ(PoolFactEventKindPoolEvent),
		).
		Order(factpoolsessionevents.ByAllocationNo(entsql.OrderDesc())).
		First(ctx)
	if err == nil {
		allocationNo = last.AllocationNo + 1
	} else if err != nil && !gen.IsNotFound(err) {
		return err
	}

	existing, err := tx.FactPoolSessionEvents.Query().
		Where(factpoolsessionevents.AllocationIDEQ(allocID)).
		Only(ctx)
	if err == nil {
		allocationNo = existing.AllocationNo
		_, err = tx.FactPoolSessionEvents.UpdateOneID(existing.ID).
			SetPoolSessionID(sessionID).
			SetAllocationKind(kind).
			SetEventKind(PoolFactEventKindPoolEvent).
			SetSequenceNum(int64(in.SequenceNum)).
			SetState("confirmed").
			SetPayeeAmountAfter(int64(in.PayeeAmountAfter)).
			SetPayerAmountAfter(int64(in.PayerAmountAfter)).
			SetTxid(txID).
			SetTxHex(txHex).
			SetCreatedAtUnix(createdAt).
			SetPayloadJSON("{}").
			Save(ctx)
		if err != nil {
			return err
		}
	} else if err != nil && !gen.IsNotFound(err) {
		return err
	} else {
		_, err = tx.FactPoolSessionEvents.Create().
			SetAllocationID(allocID).
			SetPoolSessionID(sessionID).
			SetAllocationNo(allocationNo).
			SetAllocationKind(kind).
			SetEventKind(PoolFactEventKindPoolEvent).
			SetSequenceNum(int64(in.SequenceNum)).
			SetState("confirmed").
			SetDirection("").
			SetAmountSatoshi(0).
			SetPurpose(kind).
			SetNote("").
			SetMsgID("").
			SetCycleIndex(0).
			SetPayeeAmountAfter(int64(in.PayeeAmountAfter)).
			SetPayerAmountAfter(int64(in.PayerAmountAfter)).
			SetTxid(txID).
			SetTxHex(txHex).
			SetGatewayPubkeyHex("").
			SetCreatedAtUnix(createdAt).
			SetPayloadJSON("{}").
			Save(ctx)
		if err != nil {
			return err
		}
	}

	// 旧 fact 事件只保留兼容锚点，真正的池账写入走 dbApplyDirectTransferBizPoolAccountingTx。
	if err := dbApplyDirectTransferBizPoolAccountingTx(ctx, tx, in, allocationNo); err != nil {
		return err
	}

	channel, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
		Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(sessionID)).
		Only(ctx)
	if err != nil {
		return fmt.Errorf("resolve pool session channel for allocation %s: %w", allocID, err)
	}
	channelID := int64(channel.ID)
	settlementPaymentAttemptID := channel.SettlementPaymentAttemptID

	if _, err := tx.FactSettlementChannelPoolSessionQuotePay.UpdateOneID(channel.ID).
		SetTxid(txID).
		SetUpdatedAtUnix(createdAt).
		Save(ctx); err != nil {
		return err
	}
	if _, err := tx.FactSettlementPaymentAttempts.UpdateOneID(settlementPaymentAttemptID).
		SetSourceType("pool_session_quote_pay").
		SetSourceID(fmt.Sprintf("%d", channelID)).
		SetState("confirmed").
		SetOccurredAtUnix(createdAt).
		SetConfirmedAtUnix(createdAt).
		SetNote("updated from pool allocation event").
		Save(ctx); err != nil {
		return err
	}
	return nil
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

// dbGetPoolAllocationIDByAllocationIDEntTx 按 allocation_id 查 fact_pool_session_events.id
// 设计说明：
// - 写入层和读层都只认事实表自增主键；
// - allocation_id 只作为旧入口和 payload 保留，不再直接承担 source_id 语义。
func dbGetPoolAllocationIDByAllocationIDEntTx(ctx context.Context, tx EntWriteRoot, allocationID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return 0, fmt.Errorf("allocation_id is required")
	}
	row, err := tx.FactPoolSessionEvents.Query().
		Where(factpoolsessionevents.AllocationIDEQ(allocationID)).
		Only(ctx)
	if err != nil {
		return 0, err
	}
	return int64(row.ID), nil
}

// dbGetPoolAllocationIDByAllocationID 按 allocation_id 查自增 id
// 设计说明：财务来源已经收口到事实表自增主键，不再依赖业务键。
func dbGetPoolAllocationIDByAllocationID(ctx context.Context, store *clientDB, allocationID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (int64, error) {
		return dbGetPoolAllocationIDByAllocationIDEntRoot(ctx, root, allocationID)
	})
}

// dbGetPoolAllocationIDByAllocationIDRoot 在读壳内按 allocation_id 查自增 id
// 用于财务写入时在同一事务内获取主键
func dbGetPoolAllocationIDByAllocationIDEntRoot(ctx context.Context, root EntReadRoot, allocationID string) (int64, error) {
	if root == nil {
		return 0, fmt.Errorf("root is nil")
	}
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return 0, fmt.Errorf("allocation_id is required")
	}
	row, err := root.FactPoolSessionEvents.Query().
		Where(factpoolsessionevents.AllocationIDEQ(allocationID)).
		Only(ctx)
	if err != nil {
		return 0, err
	}
	return int64(row.ID), nil
}

// dbGetPoolAllocationIDByKindTx 在事务内按 session + kind 取第一条 allocation_id。
// 设计说明：
// - snapshot 更新需要稳定地拿到 open allocation id；
// - 不能拿当前 sequence 反推，否则重放时会漂。
func dbGetPoolAllocationIDByKindTx(ctx context.Context, tx EntWriteRoot, sessionID string, allocationKind string) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("tx is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", fmt.Errorf("session_id is required")
	}
	allocationKind = strings.TrimSpace(allocationKind)
	if allocationKind == "" {
		return "", fmt.Errorf("allocation_kind is required")
	}
	row, err := tx.FactPoolSessionEvents.Query().
		Where(
			factpoolsessionevents.PoolSessionIDEQ(sessionID),
			factpoolsessionevents.EventKindEQ(PoolFactEventKindPoolEvent),
			factpoolsessionevents.AllocationKindEQ(allocationKind),
		).
		Order(factpoolsessionevents.ByAllocationNo(entsql.OrderAsc())).
		First(ctx)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(row.AllocationID), nil
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
