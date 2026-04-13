package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/modules/domain"
	_ "modernc.org/sqlite"
)

func TestTriggerWalletOrderPreview_UnknownTemplateShowsHighWarning(t *testing.T) {
	t.Parallel()

	rt, store := newWalletOrderTestRuntime(t)
	resp, err := TriggerWalletOrderPreview(context.Background(), store, rt, WalletOrderRequest{
		SignerPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SignedEnvelope:  []byte(`[["unknown-template-v1","x"],"abcd"]`),
	})
	if err != nil {
		t.Fatalf("TriggerWalletOrderPreview failed: %v", err)
	}
	if !resp.Ok {
		t.Fatalf("preview should succeed: %+v", resp)
	}
	if resp.Preview.Recognized {
		t.Fatalf("preview should be unrecognized: %+v", resp.Preview)
	}
	if resp.Preview.CanSign {
		t.Fatalf("unknown template should not be signable: %+v", resp.Preview)
	}
	if resp.Preview.WarningLevel != "high" {
		t.Fatalf("warning level mismatch: got=%s want=high", resp.Preview.WarningLevel)
	}
	if !strings.Contains(resp.Preview.Summary, "未知交易请求") {
		t.Fatalf("unexpected summary: %s", resp.Preview.Summary)
	}
}

func TestTriggerWalletOrderSign_DomainRegisterTemplateBuildsSignedTx(t *testing.T) {
	t.Parallel()

	rt, store := newWalletOrderTestRuntime(t)
	clientPubkeyHex := rt.runIn.ClientID
	targetPubkeyHex, err := clientIDFromPrivHex("3333333333333333333333333333333333333333333333333333333333333333")
	if err != nil {
		t.Fatalf("derive target pubkey failed: %v", err)
	}
	domainPrivHex := "2222222222222222222222222222222222222222222222222222222222222222"
	domainPubkeyHex, err := clientIDFromPrivHex(domainPrivHex)
	if err != nil {
		t.Fatalf("derive domain pubkey failed: %v", err)
	}
	signedEnvelope, err := signEnvelopeForTest(domainPrivHex, domainmodule.RegisterQuoteFields{
		QuoteID:                  "quote-1",
		Name:                     "alice.david",
		OwnerPubkeyHex:           clientPubkeyHex,
		TargetPubkeyHex:          targetPubkeyHex,
		PayToAddress:             "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		RegisterPriceSatoshi:     1200,
		RegisterSubmitFeeSatoshi: 200,
		TotalPaySatoshi:          1400,
		LockExpiresAtUnix:        time.Now().Unix() + 600,
		TermSeconds:              3600,
	}.Array())
	if err != nil {
		t.Fatalf("signEnvelopeForTest failed: %v", err)
	}

	previewResp, err := TriggerWalletOrderPreview(context.Background(), store, rt, WalletOrderRequest{
		SignerPubkeyHex: domainPubkeyHex,
		SignedEnvelope:  signedEnvelope,
	})
	if err != nil {
		t.Fatalf("TriggerWalletOrderPreview failed: %v", err)
	}
	if !previewResp.Ok || !previewResp.Preview.Recognized || !previewResp.Preview.CanSign {
		t.Fatalf("unexpected preview response: %+v", previewResp)
	}
	if previewResp.Preview.BusinessAmountSatoshi != 1400 {
		t.Fatalf("business amount mismatch: got=%d want=1400", previewResp.Preview.BusinessAmountSatoshi)
	}
	if previewResp.Preview.MinerFeeSatoshi == 0 {
		t.Fatalf("miner fee should be calculated")
	}
	signResp, err := TriggerWalletOrderSign(context.Background(), store, rt, WalletOrderRequest{
		SignerPubkeyHex:     domainPubkeyHex,
		SignedEnvelope:      signedEnvelope,
		ExpectedPreviewHash: previewResp.Preview.PreviewHash,
	})
	if err != nil {
		t.Fatalf("TriggerWalletOrderSign failed: %v", err)
	}
	if !signResp.Ok {
		t.Fatalf("sign response not ok: %+v", signResp)
	}
	if strings.TrimSpace(signResp.SignedTxHex) == "" || strings.TrimSpace(signResp.TxID) == "" {
		t.Fatalf("signed tx missing: %+v", signResp)
	}
	if !strings.EqualFold(signResp.TxID, previewResp.Preview.TxID) {
		t.Fatalf("txid mismatch: preview=%s sign=%s", previewResp.Preview.TxID, signResp.TxID)
	}
}

func newWalletOrderTestRuntime(t *testing.T) (*Runtime, *clientDB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas failed: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	const clientPrivHex = "1111111111111111111111111111111111111111111111111111111111111111"
	clientPubkeyHex, err := clientIDFromPrivHex(clientPrivHex)
	if err != nil {
		t.Fatalf("derive client pubkey failed: %v", err)
	}
	cfg := Config{ClientID: clientPubkeyHex}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = clientPrivHex
	rt := &Runtime{
		runIn:       NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex),
		ActionChain: &feePoolKernelMockChain{},
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	if err := seedWalletUTXOsForKernelTest(db, addr, []poolcore.UTXO{
		{TxID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Vout: 0, Value: 4000},
		{TxID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Vout: 1, Value: 6000},
	}); err != nil {
		t.Fatalf("seedWalletUTXOsForKernelTest failed: %v", err)
	}
	return rt, newClientDB(db, nil)
}

func signEnvelopeForTest(privHex string, fields []any) ([]byte, error) {
	priv, err := parsePrivHex(privHex)
	if err != nil {
		return nil, err
	}
	return domainmodule.SignEnvelope(fields, priv.Sign)
}
