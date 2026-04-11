package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	bsvscript "github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func TestWalletWOCQuantityTextUnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "quoted", raw: `{"amt":"42"}`, want: "42"},
		{name: "number", raw: `{"amt":42}`, want: "42"},
		{name: "null", raw: `{"amt":null}`, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var payload struct {
				Amount walletWOCQuantityText `json:"amt"`
			}
			if err := json.Unmarshal([]byte(tt.raw), &payload); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if got := payload.Amount.String(); got != tt.want {
				t.Fatalf("amount=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoadWalletBSV21SpendableCandidates_ReturnsLocalCandidatesWhenWOCUnavailable(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := newWalletBSV21LocalCandidateTestRuntime(t, db, "http://127.0.0.1:1")
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	tokenID, txID := seedWalletBSV21LocalCreateCandidate(t, db, rt, address, "LOCAL", "100")

	items, err := loadWalletBSV21SpendableCandidates(context.Background(), newClientDB(db, nil), rt, address, "bsv21:"+tokenID)
	if err != nil {
		t.Fatalf("loadWalletBSV21SpendableCandidates: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("candidate count mismatch: got=%d want=1", len(items))
	}
	if got := strings.TrimSpace(items[0].Item.SourceName); got != "local" {
		t.Fatalf("source_name=%q, want local", got)
	}
	if got := strings.TrimSpace(items[0].Item.TxID); got != txID {
		t.Fatalf("txid=%q, want %q", got, txID)
	}
	if got := strings.TrimSpace(items[0].Item.AssetKey); got != "bsv21:"+tokenID {
		t.Fatalf("asset_key=%q, want %q", got, "bsv21:"+tokenID)
	}
	if got := strings.TrimSpace(items[0].Item.QuantityText); got != "100" {
		t.Fatalf("quantity_text=%q, want 100", got)
	}
}

type testWalletChainClient struct {
	baseURL string
}

func (t testWalletChainClient) BaseURL() string { return strings.TrimSpace(t.baseURL) }

func (testWalletChainClient) GetAddressConfirmedUnspent(context.Context, string) ([]whatsonchain.UTXO, error) {
	return nil, nil
}

func (testWalletChainClient) GetAddressBSV21TokenUnspent(context.Context, string) ([]whatsonchain.BSV21TokenUTXO, error) {
	return nil, nil
}

func (testWalletChainClient) GetChainInfo(context.Context) (uint32, error) { return 0, nil }

func (testWalletChainClient) GetAddressConfirmedHistory(context.Context, string) ([]whatsonchain.AddressHistoryItem, error) {
	return nil, nil
}

func (testWalletChainClient) GetAddressConfirmedHistoryPage(context.Context, string, whatsonchain.ConfirmedHistoryQuery) (whatsonchain.ConfirmedHistoryPage, error) {
	return whatsonchain.ConfirmedHistoryPage{}, nil
}

func (testWalletChainClient) GetAddressUnconfirmedHistory(context.Context, string) ([]string, error) {
	return nil, nil
}

func (testWalletChainClient) GetTxHash(context.Context, string) (whatsonchain.TxDetail, error) {
	return whatsonchain.TxDetail{}, nil
}

func (testWalletChainClient) GetTxHex(context.Context, string) (string, error) { return "", nil }

func newWalletBSV21LocalCandidateTestRuntime(t *testing.T, db *sql.DB, baseURL string) *Runtime {
	t.Helper()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("1", 64)
	return &Runtime{
		runIn:       NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex),
		WalletChain: testWalletChainClient{baseURL: baseURL},
	}
}

func seedWalletBSV21LocalCreateCandidate(t *testing.T, db *sql.DB, rt *Runtime, address string, symbol string, amount string) (string, string) {
	t.Helper()

	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	walletAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		t.Fatalf("NewAddressFromString: %v", err)
	}
	lockScript, err := p2pkh.Lock(walletAddr)
	if err != nil {
		t.Fatalf("p2pkh.Lock: %v", err)
	}
	payload, err := buildBSV21DeployMintData(symbol, amount, 0)
	if err != nil {
		t.Fatalf("buildBSV21DeployMintData: %v", err)
	}
	tx := txsdk.NewTransaction()
	if err := tx.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: lockScript,
		ContentType:   walletTokenContentType,
		Data:          payload,
	}); err != nil {
		t.Fatalf("tx.Inscribe: %v", err)
	}
	txID := strings.ToLower(strings.TrimSpace(tx.TxID().String()))
	tokenID := walletTokenCreateTokenIDFromTxID(txID, 0)
	now := time.Now().Unix()
	walletID := walletIDByAddress(address)
	if _, err := db.Exec(
		`INSERT INTO wallet_local_broadcast_txs(txid,wallet_id,address,tx_hex,created_at_unix,updated_at_unix,observed_at_unix)
		 VALUES(?,?,?,?,?,?,?)`,
		txID,
		walletID,
		address,
		strings.ToLower(strings.TrimSpace(tx.Hex())),
		now,
		now,
		int64(0),
	); err != nil {
		t.Fatalf("insert wallet_local_broadcast_txs: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		txID+":0",
		walletID,
		address,
		txID,
		0,
		1,
		"unspent",
		walletUTXOAllocationProtectedAsset,
		"local_bsv21_create",
		txID,
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("insert wallet_utxo: %v", err)
	}
	var factCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payments WHERE txid=?`, txID).Scan(&factCount); err != nil {
		t.Fatalf("query fact_chain_payments count failed: %v", err)
	}
	if factCount != 0 {
		t.Fatalf("fact_chain_payments should stay empty for local broadcast, got=%d", factCount)
	}
	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txID).Scan(&cycleCount); err != nil {
		t.Fatalf("query fact_settlement_cycles count failed: %v", err)
	}
	if cycleCount != 0 {
		t.Fatalf("fact_settlement_cycles should stay empty for local broadcast, got=%d", cycleCount)
	}
	return tokenID, txID
}
