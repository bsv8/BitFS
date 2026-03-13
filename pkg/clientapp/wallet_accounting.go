package clientapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
)

type finBusinessEntry struct {
	BusinessID     string
	SceneType      string
	SceneSubType   string
	FromPartyID    string
	ToPartyID      string
	RefID          string
	Status         string
	OccurredAtUnix int64
	IdempotencyKey string
	Note           string
	Payload        any
}

type finTxBreakdownEntry struct {
	BusinessID         string
	TxID               string
	GrossInputSatoshi  int64
	ChangeBackSatoshi  int64
	ExternalInSatoshi  int64
	CounterpartyOutSat int64
	MinerFeeSatoshi    int64
	NetOutSatoshi      int64
	NetInSatoshi       int64
	CreatedAtUnix      int64
	Note               string
	Payload            any
}

type bizUTXOLinkEntry struct {
	BusinessID    string
	TxID          string
	UTXOID        string
	Role          string
	AmountSatoshi int64
	CreatedAtUnix int64
	Note          string
	Payload       any
}

type finProcessEventEntry struct {
	ProcessID      string
	SceneType      string
	SceneSubType   string
	EventType      string
	Status         string
	RefID          string
	OccurredAtUnix int64
	IdempotencyKey string
	Note           string
	Payload        any
}

func mustJSONString(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func appendFinBusiness(db *sql.DB, e finBusinessEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.BusinessID
	}
	_, err := db.Exec(
		`INSERT INTO fin_business(business_id,scene_type,scene_subtype,from_party_id,to_party_id,ref_id,status,occurred_at_unix,idempotency_key,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(idempotency_key) DO UPDATE SET
			status=excluded.status,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		e.BusinessID,
		strings.TrimSpace(e.SceneType),
		strings.TrimSpace(e.SceneSubType),
		strings.TrimSpace(e.FromPartyID),
		strings.TrimSpace(e.ToPartyID),
		strings.TrimSpace(e.RefID),
		strings.TrimSpace(e.Status),
		e.OccurredAtUnix,
		e.IdempotencyKey,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func appendFinTxBreakdownIfAbsent(db *sql.DB, e finTxBreakdownEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_tx_breakdown WHERE business_id=? AND txid=?`, strings.TrimSpace(e.BusinessID), strings.ToLower(strings.TrimSpace(e.TxID))).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	return appendFinTxBreakdown(db, e)
}

func appendBizUTXOLinkIfAbsent(db *sql.DB, e bizUTXOLinkEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM biz_utxo_links WHERE business_id=? AND txid=? AND utxo_id=? AND role=?`,
		strings.TrimSpace(e.BusinessID),
		strings.ToLower(strings.TrimSpace(e.TxID)),
		strings.ToLower(strings.TrimSpace(e.UTXOID)),
		strings.TrimSpace(e.Role),
	).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	return appendBizUTXOLink(db, e)
}

func appendFinTxBreakdown(db *sql.DB, e finTxBreakdownEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	_, err := db.Exec(
		`INSERT INTO fin_tx_breakdown(
			business_id,txid,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		strings.TrimSpace(e.BusinessID),
		strings.ToLower(strings.TrimSpace(e.TxID)),
		e.GrossInputSatoshi,
		e.ChangeBackSatoshi,
		e.ExternalInSatoshi,
		e.CounterpartyOutSat,
		e.MinerFeeSatoshi,
		e.NetOutSatoshi,
		e.NetInSatoshi,
		e.CreatedAtUnix,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func appendBizUTXOLink(db *sql.DB, e bizUTXOLinkEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	_, err := db.Exec(
		`INSERT INTO biz_utxo_links(business_id,txid,utxo_id,role,amount_satoshi,created_at_unix,note,payload_json) VALUES(?,?,?,?,?,?,?,?)`,
		strings.TrimSpace(e.BusinessID),
		strings.ToLower(strings.TrimSpace(e.TxID)),
		strings.ToLower(strings.TrimSpace(e.UTXOID)),
		strings.TrimSpace(e.Role),
		e.AmountSatoshi,
		e.CreatedAtUnix,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func appendFinProcessEvent(db *sql.DB, e finProcessEventEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.ProcessID = strings.TrimSpace(e.ProcessID)
	if e.ProcessID == "" {
		return fmt.Errorf("process_id is required")
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.ProcessID + ":" + strings.TrimSpace(e.EventType)
	}
	_, err := db.Exec(
		`INSERT INTO fin_process_events(process_id,scene_type,scene_subtype,event_type,status,ref_id,occurred_at_unix,idempotency_key,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(idempotency_key) DO UPDATE SET
			status=excluded.status,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		e.ProcessID,
		strings.TrimSpace(e.SceneType),
		strings.TrimSpace(e.SceneSubType),
		strings.TrimSpace(e.EventType),
		strings.TrimSpace(e.Status),
		strings.TrimSpace(e.RefID),
		e.OccurredAtUnix,
		e.IdempotencyKey,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

type feePoolOpenAccountingInput struct {
	BusinessID        string
	SpendTxID         string
	BaseTxID          string
	BaseTxHex         string
	ClientLockScript  string
	PoolAmountSatoshi uint64
	FromPartyID       string
	ToPartyID         string
}

func recordFeePoolOpenAccounting(db *sql.DB, in feePoolOpenAccountingInput) {
	if db == nil {
		return
	}
	businessID := strings.TrimSpace(in.BusinessID)
	if businessID == "" {
		businessID = "biz_feepool_open_" + randHex(8)
	}
	baseTxHex := strings.TrimSpace(in.BaseTxHex)
	baseTxID := strings.ToLower(strings.TrimSpace(in.BaseTxID))
	lockScript := strings.TrimSpace(in.ClientLockScript)

	var grossInput int64
	var changeBack int64
	var lockAmount int64
	if baseTxHex != "" {
		t, err := transaction.NewTransactionFromHex(baseTxHex)
		if err != nil {
			obs.Error("bitcast-client", "wallet_accounting_parse_base_tx_failed", map[string]any{"error": err.Error(), "base_txid": baseTxID})
		} else {
			for _, input := range t.Inputs {
				if input.SourceTxOutput() != nil {
					grossInput += int64(input.SourceTxOutput().Satoshis)
					if input.SourceTXID != nil {
						utxoID := strings.ToLower(strings.TrimSpace(input.SourceTXID.String())) + ":" + fmt.Sprint(input.SourceTxOutIndex)
						_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
							BusinessID:    businessID,
							TxID:          baseTxID,
							UTXOID:        utxoID,
							Role:          "input",
							AmountSatoshi: int64(input.SourceTxOutput().Satoshis),
							Note:          "fee pool open input",
						})
					}
				}
			}
			for idx, out := range t.Outputs {
				amount := int64(out.Satoshis)
				if idx == 0 {
					lockAmount += amount
					_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
						BusinessID:    businessID,
						TxID:          baseTxID,
						UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
						Role:          "lock",
						AmountSatoshi: amount,
						Note:          "fee pool lock output",
					})
					continue
				}
				if lockScript != "" && strings.EqualFold(strings.TrimSpace(out.LockingScript.String()), lockScript) {
					changeBack += amount
					_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
						BusinessID:    businessID,
						TxID:          baseTxID,
						UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
						Role:          "change",
						AmountSatoshi: amount,
						Note:          "wallet change output",
					})
				}
			}
			if lockAmount == 0 {
				lockAmount = int64(in.PoolAmountSatoshi)
			}
		}
	}
	if lockAmount == 0 {
		lockAmount = int64(in.PoolAmountSatoshi)
	}
	minerFee := grossInput - changeBack - lockAmount
	if minerFee < 0 {
		minerFee = 0
	}

	if err := appendFinBusiness(db, finBusinessEntry{
		BusinessID:     businessID,
		SceneType:      "fee_pool",
		SceneSubType:   "open",
		FromPartyID:    strings.TrimSpace(in.FromPartyID),
		ToPartyID:      strings.TrimSpace(in.ToPartyID),
		RefID:          strings.TrimSpace(in.SpendTxID),
		Status:         "posted",
		OccurredAtUnix: time.Now().Unix(),
		IdempotencyKey: "fee_pool_open:" + strings.TrimSpace(in.SpendTxID),
		Note:           "fee pool open lock",
		Payload: map[string]any{
			"spend_txid": strings.TrimSpace(in.SpendTxID),
			"base_txid":  baseTxID,
		},
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_open"})
		return
	}
	if err := appendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               baseTxID,
		GrossInputSatoshi:  grossInput,
		ChangeBackSatoshi:  changeBack,
		ExternalInSatoshi:  0,
		CounterpartyOutSat: lockAmount,
		MinerFeeSatoshi:    minerFee,
		NetOutSatoshi:      lockAmount + minerFee,
		NetInSatoshi:       0,
		Note:               "open lock gross_input-change_back",
		Payload: map[string]any{
			"formula": "net_out = counterparty_out + miner_fee",
		},
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_open"})
	}
}

func recordFeePoolCycleEvent(db *sql.DB, spendTxID string, sequence uint32, amount uint64, gatewayPeerID string) {
	if db == nil {
		return
	}
	processID := "proc_feepool_cycle_" + strings.TrimSpace(spendTxID)
	if err := appendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:      processID,
		SceneType:      "fee_pool",
		SceneSubType:   "cycle_pay",
		EventType:      "update",
		Status:         "applied",
		RefID:          strings.TrimSpace(spendTxID),
		OccurredAtUnix: time.Now().Unix(),
		IdempotencyKey: "fee_pool_cycle_event:" + strings.TrimSpace(spendTxID) + ":" + fmt.Sprint(sequence),
		Note:           "fee pool cycle event (offchain)",
		Payload: map[string]any{
			"sequence":           sequence,
			"charge_amount_sat":  amount,
			"gateway_pubkey_hex":    strings.TrimSpace(gatewayPeerID),
			"financial_affected": false,
		},
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_cycle"})
	}
}

type directPoolOpenAccountingInput struct {
	SessionID         string
	DealID            string
	BaseTxID          string
	BaseTxHex         string
	ClientLockScript  string
	PoolAmountSatoshi uint64
	SellerPeerID      string
}

func recordDirectPoolOpenAccounting(db *sql.DB, in directPoolOpenAccountingInput) {
	if db == nil {
		return
	}
	businessID := "biz_c2c_open_" + strings.TrimSpace(in.SessionID)
	baseTxID := strings.ToLower(strings.TrimSpace(in.BaseTxID))
	lockScript := strings.TrimSpace(in.ClientLockScript)
	var grossInput int64
	var changeBack int64
	var lockAmount int64
	if t, err := transaction.NewTransactionFromHex(strings.TrimSpace(in.BaseTxHex)); err == nil {
		for _, input := range t.Inputs {
			if input.SourceTxOutput() == nil {
				continue
			}
			grossInput += int64(input.SourceTxOutput().Satoshis)
			if input.SourceTXID != nil {
				utxoID := strings.ToLower(strings.TrimSpace(input.SourceTXID.String())) + ":" + fmt.Sprint(input.SourceTxOutIndex)
				_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
					BusinessID:    businessID,
					TxID:          baseTxID,
					UTXOID:        utxoID,
					Role:          "input",
					AmountSatoshi: int64(input.SourceTxOutput().Satoshis),
					Note:          "direct pool open input",
				})
			}
		}
		for idx, out := range t.Outputs {
			amount := int64(out.Satoshis)
			if idx == 0 {
				lockAmount += amount
				_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
					BusinessID:    businessID,
					TxID:          baseTxID,
					UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
					Role:          "lock",
					AmountSatoshi: amount,
					Note:          "direct pool lock output",
				})
				continue
			}
			if lockScript != "" && strings.EqualFold(strings.TrimSpace(out.LockingScript.String()), lockScript) {
				changeBack += amount
				_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
					BusinessID:    businessID,
					TxID:          baseTxID,
					UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
					Role:          "change",
					AmountSatoshi: amount,
					Note:          "wallet change output",
				})
			}
		}
	}
	if lockAmount == 0 {
		lockAmount = int64(in.PoolAmountSatoshi)
	}
	minerFee := grossInput - changeBack - lockAmount
	if minerFee < 0 {
		minerFee = 0
	}
	if err := appendFinBusiness(db, finBusinessEntry{
		BusinessID:     businessID,
		SceneType:      "c2c_transfer",
		SceneSubType:   "open",
		FromPartyID:    "client:self",
		ToPartyID:      "seller:" + strings.TrimSpace(in.SellerPeerID),
		RefID:          strings.TrimSpace(in.SessionID),
		Status:         "posted",
		OccurredAtUnix: time.Now().Unix(),
		IdempotencyKey: "c2c_open:" + strings.TrimSpace(in.SessionID),
		Note:           "direct transfer pool open lock",
		Payload: map[string]any{
			"session_id": strings.TrimSpace(in.SessionID),
			"deal_id":    strings.TrimSpace(in.DealID),
			"base_txid":  baseTxID,
		},
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "c2c_open"})
		return
	}
	_ = appendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               baseTxID,
		GrossInputSatoshi:  grossInput,
		ChangeBackSatoshi:  changeBack,
		ExternalInSatoshi:  0,
		CounterpartyOutSat: lockAmount,
		MinerFeeSatoshi:    minerFee,
		NetOutSatoshi:      lockAmount + minerFee,
		NetInSatoshi:       0,
		Note:               "direct open lock gross_input-change_back",
		Payload:            map[string]any{"session_id": strings.TrimSpace(in.SessionID)},
	})
}

func recordDirectPoolPayAccounting(db *sql.DB, sessionID string, sequence uint32, amount uint64, sellerPeerID string, relatedTxID string) {
	if db == nil {
		return
	}
	businessID := fmt.Sprintf("biz_c2c_pay_%s_%d", strings.TrimSpace(sessionID), sequence)
	if err := appendFinBusiness(db, finBusinessEntry{
		BusinessID:     businessID,
		SceneType:      "c2c_transfer",
		SceneSubType:   "chunk_pay",
		FromPartyID:    "client:self",
		ToPartyID:      "seller:" + strings.TrimSpace(sellerPeerID),
		RefID:          strings.TrimSpace(sessionID),
		Status:         "posted",
		OccurredAtUnix: time.Now().Unix(),
		IdempotencyKey: "c2c_pay:" + strings.TrimSpace(sessionID) + ":" + fmt.Sprint(sequence),
		Note:           "direct transfer chunk pay",
		Payload:        map[string]any{"sequence": sequence},
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "c2c_pay"})
		return
	}
	_ = appendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               strings.TrimSpace(relatedTxID),
		GrossInputSatoshi:  0,
		ChangeBackSatoshi:  0,
		ExternalInSatoshi:  0,
		CounterpartyOutSat: int64(amount),
		MinerFeeSatoshi:    0,
		NetOutSatoshi:      int64(amount),
		NetInSatoshi:       0,
		Note:               "offchain chunk pay",
		Payload:            map[string]any{"sequence": sequence},
	})
}

func recordDirectPoolCloseAccounting(db *sql.DB, sessionID string, finalTxID string, finalTxHex string, sellerAmount uint64, buyerAmount uint64, sellerPeerID string) {
	if db == nil {
		return
	}
	finalTxID = strings.ToLower(strings.TrimSpace(finalTxID))
	txHex := strings.TrimSpace(finalTxHex)
	var parsedFinalTx *transaction.Transaction
	if txHex != "" {
		t, err := transaction.NewTransactionFromHex(txHex)
		if err != nil {
			obs.Error("bitcast-client", "wallet_accounting_parse_final_tx_failed", map[string]any{"error": err.Error(), "final_txid": finalTxID})
		} else {
			parsedFinalTx = t
			if finalTxID == "" {
				finalTxID = strings.ToLower(strings.TrimSpace(t.TxID().String()))
			}
		}
	}
	businessID := "biz_c2c_close_" + strings.TrimSpace(sessionID)
	if err := appendFinBusiness(db, finBusinessEntry{
		BusinessID:     businessID,
		SceneType:      "c2c_transfer",
		SceneSubType:   "close",
		FromPartyID:    "client:self",
		ToPartyID:      "seller:" + strings.TrimSpace(sellerPeerID),
		RefID:          strings.TrimSpace(sessionID),
		Status:         "posted",
		OccurredAtUnix: time.Now().Unix(),
		IdempotencyKey: "c2c_close:" + strings.TrimSpace(sessionID),
		Note:           "direct transfer settle close",
		Payload: map[string]any{
			"seller_amount_satoshi": sellerAmount,
			"buyer_amount_satoshi":  buyerAmount,
		},
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "c2c_close"})
		return
	}
	_ = appendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               finalTxID,
		GrossInputSatoshi:  0,
		ChangeBackSatoshi:  int64(buyerAmount),
		ExternalInSatoshi:  0,
		CounterpartyOutSat: 0,
		MinerFeeSatoshi:    0,
		NetOutSatoshi:      0,
		NetInSatoshi:       0,
		Note:               "pool settle return",
		Payload:            map[string]any{"session_id": strings.TrimSpace(sessionID)},
	})

	// close 是链上结算点：补齐业务与输入/输出 UTXO 映射，保证账务可追溯到 UTXO 颗粒度。
	if parsedFinalTx == nil {
		return
	}

	inputValueHint := int64(sellerAmount + buyerAmount)
	for i, in := range parsedFinalTx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		value := int64(0)
		if i == 0 {
			value = inputValueHint
		}
		utxoID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String())) + ":" + fmt.Sprint(in.SourceTxOutIndex)
		_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
			BusinessID:    businessID,
			TxID:          finalTxID,
			UTXOID:        utxoID,
			Role:          "settle_input",
			AmountSatoshi: value,
			Note:          "direct pool settle input",
		})
	}

	sellerLeft := sellerAmount
	buyerLeft := buyerAmount
	for idx, out := range parsedFinalTx.Outputs {
		amount := out.Satoshis
		if amount == 0 {
			continue
		}
		role := "settle_other"
		note := "direct pool settle output"
		if sellerLeft > 0 && amount == sellerLeft {
			role = "settle_to_seller"
			note = "direct pool settle output to seller"
			sellerLeft = 0
		} else if buyerLeft > 0 && amount == buyerLeft {
			role = "settle_to_buyer"
			note = "direct pool settle output to buyer"
			buyerLeft = 0
		}
		_ = appendBizUTXOLinkIfAbsent(db, bizUTXOLinkEntry{
			BusinessID:    businessID,
			TxID:          finalTxID,
			UTXOID:        finalTxID + ":" + fmt.Sprint(idx),
			Role:          role,
			AmountSatoshi: int64(amount),
			Note:          note,
		})
	}
}

func recordWalletChainAccounting(db *sql.DB, txid string, category string, walletInSat int64, walletOutSat int64, netSat int64, payload map[string]any) {
	if db == nil {
		return
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return
	}
	businessID := "biz_wallet_chain_" + txid
	sceneSubType := "internal"
	fromParty := "wallet:self"
	toParty := "wallet:self"
	externalIn := int64(0)
	counterpartyOut := int64(0)
	netIn := int64(0)
	netOut := int64(0)
	changeBack := int64(0)

	switch strings.ToUpper(strings.TrimSpace(category)) {
	case "CHANGE":
		sceneSubType = "internal_change"
		changeBack = walletOutSat
	case "REPAYMENT":
		sceneSubType = "external_in"
		externalIn = netSat
		if externalIn < 0 {
			externalIn = 0
		}
		netIn = externalIn
		fromParty = "external:unknown"
	case "THIRD_PARTY":
		sceneSubType = "external_out"
		counterpartyOut = walletInSat - walletOutSat
		if counterpartyOut < 0 {
			counterpartyOut = -netSat
		}
		if counterpartyOut < 0 {
			counterpartyOut = 0
		}
		netOut = counterpartyOut
		toParty = "external:unknown"
	case "FEE_POOL":
		sceneSubType = "fee_pool_settle"
		changeBack = walletOutSat
	default:
		if netSat > 0 {
			sceneSubType = "external_in"
			externalIn = netSat
			netIn = netSat
			fromParty = "external:unknown"
		} else if netSat < 0 {
			sceneSubType = "external_out"
			counterpartyOut = -netSat
			netOut = -netSat
			toParty = "external:unknown"
		}
	}

	if err := appendFinBusiness(db, finBusinessEntry{
		BusinessID:     businessID,
		SceneType:      "wallet_transfer",
		SceneSubType:   sceneSubType,
		FromPartyID:    fromParty,
		ToPartyID:      toParty,
		RefID:          txid,
		Status:         "posted",
		OccurredAtUnix: time.Now().Unix(),
		IdempotencyKey: "wallet_chain:" + txid,
		Note:           "wallet chain sync accounting",
		Payload:        payload,
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "wallet_chain", "txid": txid})
		return
	}
	_ = appendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               txid,
		GrossInputSatoshi:  walletInSat,
		ChangeBackSatoshi:  changeBack,
		ExternalInSatoshi:  externalIn,
		CounterpartyOutSat: counterpartyOut,
		MinerFeeSatoshi:    0,
		NetOutSatoshi:      netOut,
		NetInSatoshi:       netIn,
		Note:               "wallet chain derived breakdown",
		Payload:            payload,
	})
}
