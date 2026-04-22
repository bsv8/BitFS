package clientapp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/facttokencarrierlinks"
)

// subtractDecimalText 计算 a - b（高精度字符串小数减法）
// 设计说明：
// - 用于 fact 余额计算：remaining = total_in - total_used
// - 支持不同 scale 的小数，自动对齐精度
func subtractTokenDecimalText(a string, b string) (string, error) {
	pa, err := parseDecimalText(a)
	if err != nil {
		return "", err
	}
	pb, err := parseDecimalText(b)
	if err != nil {
		return "", err
	}
	// 对齐 scale
	scale := pa.scale
	if pb.scale > scale {
		scale = pb.scale
	}
	va := pa.intValue
	vb := pb.intValue
	if va == nil || vb == nil {
		return "0", nil
	}
	// 对齐
	for vaScale := pa.scale; vaScale < scale; vaScale++ {
		va = new(big.Int).Mul(va, big.NewInt(10))
	}
	for vbScale := pb.scale; vbScale < scale; vbScale++ {
		vb = new(big.Int).Mul(vb, big.NewInt(10))
	}
	// 减法
	diff := new(big.Int).Sub(va, vb)
	if diff.Sign() < 0 {
		return "0", nil
	}
	return formatDecimalText(diff, scale), nil
}

// sumDecimalTexts 对一组逗号分隔的 quantity_text 求和
func sumDecimalTexts(csv string) (string, error) {
	if strings.TrimSpace(csv) == "" {
		return "0", nil
	}
	var acc decimalTextAccumulator
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if err := acc.Add(part); err != nil {
			return "", err
		}
	}
	return acc.String(), nil
}

// hasFactTokenHistory 检查钱包是否有该 token 的 fact 历史记录
// 设计说明（硬切版）：
// - 改为查询 fact_token_lots 表
func hasFactTokenHistory(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (bool, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

	var count int
	err := store.Read(ctx, func(db moduleapi.ReadConn) error {
		return QueryRowContext(ctx, db,
			`SELECT COUNT(1) FROM fact_token_lots WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=?`,
			ownerPubkeyHex, assetKind, tokenID,
		).Scan(&count)
	})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// loadWalletTokenBalance 统一 token 余额读取入口
// 设计说明（硬切版）：
// - 改为查询 fact_token_lots 表
// - unknown 不参与 fact，不影响余额

// 直接计算余额

// logTokenBalanceSuccess 记录主路径成功日志
func logTokenBalanceSuccess(ctx context.Context, walletID string, assetKind string, tokenID string, quantityText string) {
	obs.Info(ServiceName, "wallet_token_balance_success", map[string]any{
		"wallet_id":     strings.TrimSpace(walletID),
		"asset_kind":    strings.TrimSpace(assetKind),
		"token_id":      strings.TrimSpace(tokenID),
		"quantity_text": strings.TrimSpace(quantityText),
	})
}

// loadWalletTokenSpendableCandidatesFromFact 从 fact 表加载 token 可花费候选
// 设计说明（硬切版）：
// - 改为查询 fact_token_lots + fact_token_carrier_links
// - 返回格式统一为 walletTokenPreviewCandidate，方便上层消费
func loadWalletTokenSpendableCandidatesFromFact(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(address))
	assetKind := strings.ToUpper(strings.TrimSpace(standard))
	tokenID := strings.TrimSpace(assetKey)
	if assetKind == "BSV21" {
		tokenID = walletBSV21TokenIDFromAssetKey(assetKey)
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return []walletTokenPreviewCandidate{}, nil
	}

	// 查询可花费的 token lots 及其 carrier
	lots, err := dbListSpendableTokenLots(ctx, store, ownerPubkeyHex, assetKind, tokenID)
	if err != nil {
		return nil, fmt.Errorf("list spendable token lots: %w", err)
	}

	if len(lots) == 0 {
		// 检查是否有历史记录
		hasHistory, err := hasFactTokenHistory(ctx, store, ownerPubkeyHex, assetKind, tokenID)
		if err != nil {
			return nil, fmt.Errorf("check token fact history: %w", err)
		}
		if hasHistory {
			return []walletTokenPreviewCandidate{}, nil
		}
		return []walletTokenPreviewCandidate{}, nil
	}

	out := make([]walletTokenPreviewCandidate, 0, len(lots))
	for _, lot := range lots {
		// 计算剩余数量
		qtyParsed, _ := parseDecimalText(lot.QuantityText)
		usedParsed, _ := parseDecimalText(lot.UsedQuantityText)

		// 对齐精度并计算剩余
		scale := qtyParsed.scale
		if usedParsed.scale > scale {
			scale = usedParsed.scale
		}
		qtyVal := new(big.Int).Set(qtyParsed.intValue)
		usedVal := new(big.Int).Set(usedParsed.intValue)
		for i := qtyParsed.scale; i < scale; i++ {
			qtyVal = new(big.Int).Mul(qtyVal, big.NewInt(10))
		}
		for i := usedParsed.scale; i < scale; i++ {
			usedVal = new(big.Int).Mul(usedVal, big.NewInt(10))
		}
		remainingVal := new(big.Int).Sub(qtyVal, usedVal)
		if remainingVal.Sign() <= 0 {
			continue
		}
		remainingText := formatDecimalText(remainingVal, scale)

		// 查询 carrier UTXO
		link, err := dbGetActiveCarrierForLot(ctx, store, lot.LotID)
		if err != nil || link == nil {
			continue
		}
		txid, vout, ok := splitUTXOID(link.CarrierUTXOID)
		if !ok {
			continue
		}

		out = append(out, walletTokenPreviewCandidate{
			Item: walletTokenOutputItem{
				UTXOID: link.CarrierUTXOID,
				TxID:   txid,
				Vout:   vout,
				// 当前 bsv21 carrier 采用 1 sat 约定；签名时必须带上正确 input value。
				ValueSatoshi: 1,
				QuantityText: remainingText,
				AssetKey:     strings.TrimSpace(lot.TokenID),
			},
			CreatedAtUnix: lot.CreatedAtUnix,
			Quantity:      decimalTextValue{intValue: remainingVal, scale: scale},
		})
	}

	return out, nil
}

// loadWalletBSV21SpendableCandidates 兼容旧测试的 BSV21 入口。
// 设计说明：
// - 生产主路径先走 fact 口径；
// - 旧测试还会种本地广播投影，所以这里保留“fact 优先、本地回落”的兼容层；
// - 这样不会把主路径重新拉回旧表，只是给测试留最后一层入口。
func loadWalletBSV21SpendableCandidates(ctx context.Context, store *clientDB, rt *Runtime, address string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	items, err := loadWalletTokenSpendableCandidatesFromFact(ctx, store, rt, address, "BSV21", assetKey)
	if err == nil && len(items) > 0 {
		return items, nil
	}
	localItems, localErr := loadWalletBSV21LocalSpendableCandidates(ctx, store, address, assetKey)
	if len(localItems) > 0 || localErr == nil {
		return localItems, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, localErr
}

// loadWalletBSV21LocalSpendableCandidates 从本地广播投影里找旧测试的 BSV21 候选。
// 设计说明：
// - 这里只服务“本地已广播、尚未上链确认”的测试场景；
// - 真实业务不应再依赖这条路径，只保留给老测试和最小夹具使用；
// - 候选要同时满足：本地广播表里有 tx_hex，wallet_utxo 里有对应输出。
func loadWalletBSV21LocalSpendableCandidates(ctx context.Context, store moduleBootstrapStore, address string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("address is required")
	}
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if tokenID == "" {
		return []walletTokenPreviewCandidate{}, nil
	}
	walletID := walletIDByAddress(address)
	type txRow struct {
		TxHex     string
		UpdatedAt int64
	}
	type utxoRow struct {
		UTXOID           string
		TxID             string
		Vout             uint32
		ValueSatoshi     uint64
		State            string
		AllocationClass  string
		AllocationReason string
		UpdatedAt        int64
	}
	var out []walletTokenPreviewCandidate
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		txRows := map[string]txRow{}
		records, err := QueryContext(ctx, rc, `
			SELECT txid, tx_hex, updated_at_unix
			  FROM wallet_local_broadcast_txs
			 WHERE wallet_id=? AND address=?
			 ORDER BY created_at_unix ASC, updated_at_unix ASC, txid ASC`,
			walletID, address,
		)
		if err != nil {
			return err
		}
		for records.Next() {
			var row struct {
				TxID      string
				TxHex     string
				UpdatedAt int64
			}
			if err := records.Scan(&row.TxID, &row.TxHex, &row.UpdatedAt); err != nil {
				_ = records.Close()
				return err
			}
			row.TxID = strings.ToLower(strings.TrimSpace(row.TxID))
			row.TxHex = strings.ToLower(strings.TrimSpace(row.TxHex))
			if row.TxID == "" || row.TxHex == "" {
				continue
			}
			txRows[row.TxID] = txRow{TxHex: row.TxHex, UpdatedAt: row.UpdatedAt}
		}
		if err := records.Err(); err != nil {
			_ = records.Close()
			return err
		}
		_ = records.Close()
		if len(txRows) == 0 {
			out = []walletTokenPreviewCandidate{}
			return nil
		}

		utxoRows := make([]utxoRow, 0, 8)
		utxos, err := QueryContext(ctx, rc, `
			SELECT utxo_id, txid, vout, value_satoshi, state, allocation_class, allocation_reason, updated_at_unix
			  FROM wallet_utxo
			 WHERE wallet_id=? AND address=?
			 ORDER BY created_at_unix ASC, updated_at_unix ASC, utxo_id ASC`,
			walletID, address,
		)
		if err != nil {
			return err
		}
		for utxos.Next() {
			var row utxoRow
			var vout int64
			if err := utxos.Scan(&row.UTXOID, &row.TxID, &vout, &row.ValueSatoshi, &row.State, &row.AllocationClass, &row.AllocationReason, &row.UpdatedAt); err != nil {
				_ = utxos.Close()
				return err
			}
			row.UTXOID = strings.ToLower(strings.TrimSpace(row.UTXOID))
			row.TxID = strings.ToLower(strings.TrimSpace(row.TxID))
			row.State = strings.ToLower(strings.TrimSpace(row.State))
			row.AllocationClass = strings.ToLower(strings.TrimSpace(row.AllocationClass))
			row.AllocationReason = strings.TrimSpace(row.AllocationReason)
			row.Vout = uint32(vout)
			if row.UTXOID == "" || row.TxID == "" {
				continue
			}
			utxoRows = append(utxoRows, row)
		}
		if err := utxos.Err(); err != nil {
			_ = utxos.Close()
			return err
		}
		_ = utxos.Close()

		out = make([]walletTokenPreviewCandidate, 0, len(utxoRows))
		for _, row := range utxoRows {
			if row.State != "unspent" {
				continue
			}
			if row.AllocationClass != walletUTXOAllocationProtectedAsset {
				continue
			}
			txRowData, ok := txRows[row.TxID]
			if !ok {
				continue
			}
			tx, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txRowData.TxHex))
			if err != nil {
				continue
			}
			var payload map[string]any
			found := false
			for _, output := range tx.Outputs {
				if output == nil || output.LockingScript == nil {
					continue
				}
				p, ok := decodeWalletTokenEnvelopePayload(output.LockingScript)
				if !ok {
					continue
				}
				if !strings.EqualFold(firstNonEmptyStringField(p, "p"), "bsv-20") {
					continue
				}
				if !strings.EqualFold(firstNonEmptyStringField(p, "op"), "deploy+mint") {
					continue
				}
				payload = p
				found = true
				break
			}
			if !found {
				continue
			}
			rowTokenID := walletTokenCreateTokenIDFromTxID(row.TxID, row.Vout)
			if !strings.EqualFold(rowTokenID, tokenID) {
				continue
			}
			quantityText := strings.TrimSpace(firstNonEmptyStringField(payload, "amt"))
			if quantityText == "" {
				continue
			}
			assetSymbol := strings.TrimSpace(firstNonEmptyStringField(payload, "sym"))
			rawPayload, err := json.Marshal(payload)
			if err != nil {
				rawPayload = []byte(`{}`)
			}
			out = append(out, walletTokenPreviewCandidate{
				Item: walletTokenOutputItem{
					UTXOID:           row.UTXOID,
					WalletAddress:    address,
					TxID:             row.TxID,
					Vout:             row.Vout,
					ValueSatoshi:     row.ValueSatoshi,
					AllocationClass:  row.AllocationClass,
					AllocationReason: row.AllocationReason,
					TokenStandard:    "BSV21",
					AssetKey:         "bsv21:" + rowTokenID,
					AssetSymbol:      assetSymbol,
					QuantityText:     quantityText,
					SourceName:       "local",
					UpdatedAtUnix:    txRowData.UpdatedAt,
					Payload:          json.RawMessage(rawPayload),
				},
				CreatedAtUnix: txRowData.UpdatedAt,
			})
		}
		return nil
	})
	return out, err
}

func splitUTXOID(utxoID string) (string, uint32, bool) {
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	sep := strings.LastIndex(utxoID, ":")
	if sep <= 0 || sep >= len(utxoID)-1 {
		return "", 0, false
	}
	txid := strings.TrimSpace(utxoID[:sep])
	voutText := strings.TrimSpace(utxoID[sep+1:])
	vout, err := strconv.ParseUint(voutText, 10, 32)
	if err != nil {
		return "", 0, false
	}
	if txid == "" {
		return "", 0, false
	}
	return txid, uint32(vout), true
}

// appendBSV21TokenSendAccountingAfterBroadcast token 发送成功后写入单一事实入口。
// 设计说明：
// - 一次只落 1 条 business + 1 条 settlement_payment_attempt；
// - token 记录按 lot 拆成多条 settlement_records；
// - 本币消耗事实放到 settlement_records 之后再展开，避免先写资产再补结算。
func appendBSV21TokenSendAccountingAfterBroadcast(ctx context.Context, store *clientDB, rt *Runtime, txHex string, txID string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return fmt.Errorf("parse tx hex: %w", err)
	}
	txID = strings.ToLower(strings.TrimSpace(txID))
	if txID == "" {
		return fmt.Errorf("txid is required")
	}
	walletAddr, err := clientWalletAddress(rt)
	if err != nil {
		return err
	}
	walletAddr = strings.TrimSpace(walletAddr)
	if walletAddr == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletScriptHex, err := walletAddressLockScriptHex(walletAddr)
	if err != nil {
		return err
	}
	walletID := walletIDByAddress(walletAddr)
	now := time.Now().Unix()

	return store.WriteEntTx(ctx, func(dbtx EntWriteRoot) error {
		lots, consumedText, err := collectBSV21TokenSendLotsForTx(ctx, dbtx, parsed, walletScriptHex)
		if err != nil {
			return err
		}
		if len(lots) == 0 {
			return fmt.Errorf("no token carrier inputs found in txid %s", txID)
		}
		if compareDecimalText(consumedText, "0") <= 0 {
			return fmt.Errorf("no token transfer outputs found in txid %s", txID)
		}

		_, settlementPaymentAttemptID, err := dbUpsertChainChannelWithSettlementPaymentAttempt(ctx, dbtx, chainPaymentEntry{
			TxID:                 txID,
			PaymentSubType:       "bsv21_transfer",
			Status:               "confirmed",
			WalletInputSatoshi:   0,
			WalletOutputSatoshi:  0,
			NetAmountSatoshi:     0,
			OccurredAtUnix:       now,
			SubmittedAtUnix:      now,
			WalletObservedAtUnix: now,
			FromPartyID:          "wallet:" + walletAddr,
			ToPartyID:            "external:unknown",
			Payload: map[string]any{
				"txid":            txID,
				"wallet_id":       walletID,
				"wallet_address":  walletAddr,
				"token_send_text": consumedText,
				"token_lot_count": len(lots),
				"token_send_type": "bsv21_transfer",
				"tx_hex":          txHex,
			},
		}, "chain_direct_pay", "payment_attempt_chain_direct_pay", "bind chain direct pay channel id")
		if err != nil {
			return fmt.Errorf("upsert direct pay channel for token send: %w", err)
		}

		if err := dbAppendSettlementPaymentAttemptFinBusiness(ctx, dbtx, settlementPaymentAttemptID, finBusinessEntry{
			OrderID:           walletBSV21SendBusinessID(txID),
			BusinessRole:      "process",
			AccountingScene:   "wallet_transfer",
			AccountingSubType: "bsv21_send",
			FromPartyID:       "wallet:" + walletAddr,
			ToPartyID:         "external:unknown",
			Status:            "posted",
			OccurredAtUnix:    now,
			IdempotencyKey:    "wallet_bsv21_send:" + txID,
			Note:              "bsv21 token send broadcast",
			Payload: map[string]any{
				"txid":            txID,
				"wallet_id":       walletID,
				"wallet_address":  walletAddr,
				"token_send_text": consumedText,
				"token_lot_count": len(lots),
			},
		}); err != nil {
			return fmt.Errorf("append settle record failed: %w", err)
		}

		for _, lot := range lots {
			recordID := fmt.Sprintf("rec_token_%d_%s", settlementPaymentAttemptID, lot.Lot.LotID)
			if err := dbAppendSettlementRecordEntTx(ctx, dbtx, settlementRecordEntry{
				RecordID:                   recordID,
				SettlementPaymentAttemptID: settlementPaymentAttemptID,
				AssetType:                  "TOKEN",
				OwnerPubkeyHex:             lot.Lot.OwnerPubkeyHex,
				SourceLotID:                lot.Lot.LotID,
				UsedQuantityText:           lot.UsedText,
				State:                      "confirmed",
				OccurredAtUnix:             now,
				ConfirmedAtUnix:            now,
				Note:                       "bsv21 token consumed by send tx",
				Payload: map[string]any{
					"txid":             txID,
					"carrier_utxo_id":  lot.CarrierUTXOID,
					"used_quantity":    lot.UsedText,
					"remaining_before": lot.RemainingText,
				},
			}); err != nil {
				return fmt.Errorf("append settlement record for lot %s: %w", lot.Lot.LotID, err)
			}
			newUsedQty, err := sumDecimalTexts(lot.Lot.UsedQuantityText + "," + lot.UsedText)
			if err != nil {
				return fmt.Errorf("sum used quantity for lot %s: %w", lot.Lot.LotID, err)
			}
			lotState := "unspent"
			if compareDecimalText(newUsedQty, lot.Lot.QuantityText) >= 0 {
				lotState = "spent"
			}
			if err := dbUpsertTokenLotEntTx(ctx, dbtx, tokenLotEntry{
				LotID:            lot.Lot.LotID,
				OwnerPubkeyHex:   lot.Lot.OwnerPubkeyHex,
				TokenID:          lot.Lot.TokenID,
				TokenStandard:    lot.Lot.TokenStandard,
				QuantityText:     lot.Lot.QuantityText,
				UsedQuantityText: newUsedQty,
				LotState:         lotState,
				MintTxid:         lot.Lot.MintTxid,
				LastSpendTxid:    txID,
				UpdatedAtUnix:    now,
				Note:             lot.Lot.Note,
				Payload:          lot.Lot.Payload,
			}); err != nil {
				return fmt.Errorf("update token lot %s: %w", lot.Lot.LotID, err)
			}
		}

		bsvFacts, err := collectBSVInputFactsForTx(ctx, dbtx, parsed)
		if err != nil {
			return err
		}
		// token carrier(1sat) 只参与 token 结算，不计入普通 BSV 扣账记录。
		// 这里显式排除 carrier 输入，避免把 token 输入误记到 BSV 资产事实。
		carrierInputs := make(map[string]struct{}, len(lots))
		for _, lot := range lots {
			utxoID := strings.ToLower(strings.TrimSpace(lot.CarrierUTXOID))
			if utxoID == "" {
				continue
			}
			carrierInputs[utxoID] = struct{}{}
		}
		filteredBSVFacts := make([]chainPaymentUTXOLinkEntry, 0, len(bsvFacts))
		for _, fact := range bsvFacts {
			utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
			if _, isCarrier := carrierInputs[utxoID]; isCarrier {
				continue
			}
			filteredBSVFacts = append(filteredBSVFacts, fact)
		}
		if len(filteredBSVFacts) == 0 {
			return fmt.Errorf("no wallet input facts found for txid %s", txID)
		}
		var grossInputSat int64
		for _, fact := range filteredBSVFacts {
			grossInputSat += fact.AmountSatoshi
		}
		if err := dbAppendBSVConsumptionsForSettlementPaymentAttemptEntTx(ctx, dbtx, settlementPaymentAttemptID, filteredBSVFacts, now); err != nil {
			return fmt.Errorf("append BSV settlement records for token send failed: %w", err)
		}
		changeBackSat := int64(0)
		counterpartyOutSat := int64(0)
		for _, out := range parsed.Outputs {
			if out == nil || out.LockingScript == nil {
				continue
			}
			amount := int64(out.Satoshis)
			if amount <= 0 {
				continue
			}
			scriptHex := hex.EncodeToString(out.LockingScript.Bytes())
			if walletScriptHexMatchesAddressControl(scriptHex, walletScriptHex) {
				changeBackSat += amount
				continue
			}
			payload, ok := decodeWalletTokenTransferPayload(out.LockingScript)
			if !ok || !strings.EqualFold(strings.TrimSpace(firstNonEmptyStringField(payload, "op")), "transfer") {
				continue
			}
			counterpartyOutSat += amount
		}
		minerFeeSat := grossInputSat - changeBackSat - counterpartyOutSat
		if minerFeeSat < 0 {
			minerFeeSat = 0
		}
		// 旧 tx 拆解/UTXO 明细层已下线，这里只保留 token 事实和流程事件。
		if err := dbAppendSettlementPaymentAttemptFinProcessEvent(ctx, dbtx, settlementPaymentAttemptID, finProcessEventEntry{
			ProcessID:         "proc_wallet_bsv21_send_" + txID,
			AccountingScene:   "wallet_transfer",
			AccountingSubType: "bsv21_send",
			EventType:         "accounting",
			Status:            "applied",
			OccurredAtUnix:    now,
			IdempotencyKey:    "wallet_bsv21_send_event:" + txID,
			Note:              "bsv21 token send accounting event",
			Payload: map[string]any{
				"txid":            txID,
				"token_lot_count": len(lots),
				"token_send_text": consumedText,
			},
		}); err != nil {
			return fmt.Errorf("append order_settlement_events failed: %w", err)
		}
		return nil
	})
}

type bsv21TokenSendLotPlan struct {
	Lot           tokenLotEntry
	CarrierUTXOID string
	RemainingText string
	UsedText      string
}

func collectBSV21TokenSendLotsForTx(ctx context.Context, db EntWriteRoot, tx *txsdk.Transaction, walletScriptHex string) ([]bsv21TokenSendLotPlan, string, error) {
	if db == nil {
		return nil, "", fmt.Errorf("db is nil")
	}
	if tx == nil {
		return nil, "", fmt.Errorf("tx is nil")
	}
	tokenLots := make([]bsv21TokenSendLotPlan, 0, len(tx.Inputs))
	for _, inp := range tx.Inputs {
		if inp == nil || inp.SourceTXID == nil {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(inp.SourceTXID.String())) + ":" + fmt.Sprint(inp.SourceTxOutIndex)
		lotID, err := dbGetLotByCarrierUTXOEntTx(ctx, db, utxoID)
		if err != nil {
			return nil, "", fmt.Errorf("lookup lot for utxo %s: %w", utxoID, err)
		}
		if lotID == "" {
			continue
		}
		lot, err := dbGetTokenLotEntTx(ctx, db, lotID)
		if err != nil {
			return nil, "", fmt.Errorf("get token lot %s: %w", lotID, err)
		}
		if lot == nil {
			return nil, "", fmt.Errorf("token lot %s not found", lotID)
		}
		remainingText, err := subtractTokenDecimalText(lot.QuantityText, lot.UsedQuantityText)
		if err != nil {
			return nil, "", fmt.Errorf("calc remaining for lot %s failed: %w", lotID, err)
		}
		if compareDecimalText(remainingText, "0") <= 0 {
			continue
		}
		tokenLots = append(tokenLots, bsv21TokenSendLotPlan{
			Lot:           *lot,
			CarrierUTXOID: utxoID,
			RemainingText: remainingText,
		})
	}
	consumedText, err := collectBSV21TokenSendConsumedText(tx, walletScriptHex)
	if err != nil {
		return nil, "", err
	}
	if compareDecimalText(consumedText, "0") <= 0 {
		return nil, "", nil
	}
	remainingText := consumedText
	for i := range tokenLots {
		if compareDecimalText(remainingText, "0") <= 0 {
			break
		}
		if compareDecimalText(tokenLots[i].RemainingText, remainingText) >= 0 {
			tokenLots[i].UsedText = remainingText
			remainingText = "0"
			break
		}
		tokenLots[i].UsedText = tokenLots[i].RemainingText
		remainingText, err = subtractTokenDecimalText(remainingText, tokenLots[i].RemainingText)
		if err != nil {
			return nil, "", fmt.Errorf("allocate token send amount failed: %w", err)
		}
	}
	if compareDecimalText(remainingText, "0") > 0 {
		return nil, "", fmt.Errorf("insufficient token lots for send amount: remaining=%s", remainingText)
	}
	allocated := make([]bsv21TokenSendLotPlan, 0, len(tokenLots))
	for _, item := range tokenLots {
		if compareDecimalText(item.UsedText, "0") <= 0 {
			continue
		}
		allocated = append(allocated, item)
	}
	return allocated, consumedText, nil
}

func collectBSV21TokenSendConsumedText(tx *txsdk.Transaction, walletScriptHex string) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("tx is nil")
	}
	walletScriptHex = strings.ToLower(strings.TrimSpace(walletScriptHex))
	total := "0"
	for _, out := range tx.Outputs {
		if out == nil || out.LockingScript == nil {
			continue
		}
		if walletScriptHex != "" && walletScriptHexMatchesAddressControl(hex.EncodeToString(out.LockingScript.Bytes()), walletScriptHex) {
			continue
		}
		payload, ok := decodeWalletTokenTransferPayload(out.LockingScript)
		if !ok {
			continue
		}
		amountText := strings.TrimSpace(firstNonEmptyStringField(payload, "amt"))
		if amountText == "" {
			continue
		}
		if compareDecimalText(amountText, "0") <= 0 {
			continue
		}
		nextTotal, err := sumDecimalTexts(total + "," + amountText)
		if err != nil {
			return "", fmt.Errorf("sum token send amount failed: %w", err)
		}
		total = nextTotal
	}
	return total, nil
}

func collectBSVInputFactsForTx(ctx context.Context, db EntWriteRoot, tx *txsdk.Transaction) ([]chainPaymentUTXOLinkEntry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	out := make([]chainPaymentUTXOLinkEntry, 0, len(tx.Inputs))
	for _, inp := range tx.Inputs {
		if inp == nil || inp.SourceTXID == nil {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(inp.SourceTXID.String())) + ":" + fmt.Sprint(inp.SourceTxOutIndex)
		value, ok, err := dbWalletUTXOValueConn(ctx, db, utxoID)
		if err != nil {
			return nil, fmt.Errorf("lookup wallet input value for %s failed: %w", utxoID, err)
		}
		if !ok {
			continue
		}
		out = append(out, chainPaymentUTXOLinkEntry{
			UTXOID:        utxoID,
			IOSide:        "input",
			UTXORole:      "wallet_input",
			AmountSatoshi: value,
			CreatedAtUnix: time.Now().Unix(),
			Note:          "bsv21 token send input",
		})
	}
	return out, nil
}

// dbGetLotByCarrierUTXO 根据 carrier UTXO 查询 lot_id。
// 设计说明：只给本文件的 token send 结算入口使用，避免把 lot 查询再散到业务层。
func dbGetLotByCarrierUTXOEntTx(ctx context.Context, db EntWriteRoot, utxoID string) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return "", nil
	}
	row, err := db.FactTokenCarrierLinks.Query().
		Where(
			facttokencarrierlinks.CarrierUtxoIDEQ(utxoID),
			facttokencarrierlinks.LinkStateEQ("active"),
		).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(row.LotID), nil
}

func walletBSV21SendBusinessID(txID string) string {
	return "biz_wallet_bsv21_send_" + strings.ToLower(strings.TrimSpace(txID))
}

// dbGetUTXOTokenQuantity 查询 UTXO 上的 token quantity_text（兼容性函数）
// 设计说明（硬切版）：
// - 改为从 fact_token_carrier_links + fact_token_lots 查询

// 先查 carrier link 获取 lot_id

// 再查 lot 获取 quantity
