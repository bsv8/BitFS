package clientapp

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func TestPurchaseSchemaExists(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)

	exists, err := hasTable(db, "biz_purchases")
	if err != nil {
		t.Fatalf("hasTable biz_purchases: %v", err)
	}
	if !exists {
		t.Fatalf("expected biz_purchases table")
	}

	cols, err := tableColumns(db, "biz_purchases")
	if err != nil {
		t.Fatalf("read biz_purchases columns: %v", err)
	}
	wantCols := []string{
		"id", "demand_id", "seller_pub_hex", "arbiter_pub_hex", "chunk_index", "object_hash",
		"amount_satoshi", "status", "error_message", "created_at_unix", "finished_at_unix",
	}
	for _, col := range wantCols {
		if _, ok := cols[col]; !ok {
			t.Fatalf("biz_purchases missing column %s", col)
		}
	}

	wantIndexes := []string{
		"idx_biz_purchases_created_at",
		"idx_biz_purchases_demand_created",
		"idx_biz_purchases_seller_created",
		"idx_biz_purchases_status_created",
		"idx_biz_purchases_history_lookup",
	}
	gotIndexes, err := tableIndexNames(db, "biz_purchases")
	if err != nil {
		t.Fatalf("read biz_purchases indexes: %v", err)
	}
	for _, want := range wantIndexes {
		if !containsString(gotIndexes, want) {
			t.Fatalf("biz_purchases missing index %s", want)
		}
	}

	if !tableHasFK(db, "biz_purchases", "demand_id", "biz_demands", "demand_id") {
		t.Fatalf("biz_purchases foreign key mismatch")
	}
}

func TestPurchaseWritesAndSummary(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()

	seedDone := purchaseDoneEntry{
		DemandID:      "dmd_purchase",
		SellerPubHex:  "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArbiterPubHex: "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ChunkIndex:    0,
		ObjectHash:    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		AmountSatoshi: 120,
	}
	chunkDone := purchaseDoneEntry{
		DemandID:      seedDone.DemandID,
		SellerPubHex:  seedDone.SellerPubHex,
		ArbiterPubHex: seedDone.ArbiterPubHex,
		ChunkIndex:    2,
		ObjectHash:    "8899aabbccddeeff00112233445566778899aabbccddeeff0011223344556677",
		AmountSatoshi: 33,
	}
	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, seedDone.DemandID, "seed_purchase", time.Now().Unix()); err != nil {
		t.Fatalf("insert demand: %v", err)
	}

	if err := dbAppendPurchaseDone(ctx, store, seedDone); err != nil {
		t.Fatalf("append seed purchase: %v", err)
	}
	if err := dbAppendPurchaseDone(ctx, store, chunkDone); err != nil {
		t.Fatalf("append chunk purchase: %v", err)
	}

	page, err := dbListPurchases(ctx, store, purchaseFilter{Limit: 20, DemandID: seedDone.DemandID})
	if err != nil {
		t.Fatalf("list biz_purchases: %v", err)
	}
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("purchase list mismatch: total=%d len=%d", page.Total, len(page.Items))
	}

	byStatus, err := dbListPurchases(ctx, store, purchaseFilter{Limit: 20, DemandID: seedDone.DemandID, Status: "failed"})
	if err != nil {
		t.Fatalf("list failed biz_purchases: %v", err)
	}
	if byStatus.Total != 0 || len(byStatus.Items) != 0 {
		t.Fatalf("failed purchase filter mismatch: %+v", byStatus)
	}

	summary, err := dbSummarizeDemandPurchases(ctx, store, seedDone.DemandID)
	if err != nil {
		t.Fatalf("summary biz_purchases: %v", err)
	}
	if summary.SeedPurchaseCount != 1 || summary.ChunkPurchaseCount != 1 || summary.TotalPurchaseCount != 2 {
		t.Fatalf("summary count mismatch: %+v", summary)
	}
	if summary.ChunkPurchaseAmountSat != int64(chunkDone.AmountSatoshi) {
		t.Fatalf("chunk amount mismatch: got=%d want=%d", summary.ChunkPurchaseAmountSat, chunkDone.AmountSatoshi)
	}
	if summary.TotalPurchaseAmountSat != int64(seedDone.AmountSatoshi+chunkDone.AmountSatoshi) {
		t.Fatalf("total amount mismatch: got=%d want=%d", summary.TotalPurchaseAmountSat, seedDone.AmountSatoshi+chunkDone.AmountSatoshi)
	}

	counters, err := dbLoadWalletSummaryCounters(ctx, store)
	if err != nil {
		t.Fatalf("wallet summary counters: %v", err)
	}
	if counters.PurchaseCount != 2 {
		t.Fatalf("wallet purchase count mismatch: got=%d want=2", counters.PurchaseCount)
	}
}

func TestPurchaseHelperRejectsInvalidAmount(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	err := dbAppendPurchaseDone(context.Background(), store, purchaseDoneEntry{
		DemandID:      "dmd_invalid",
		SellerPubHex:  "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArbiterPubHex: "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ChunkIndex:    0,
		ObjectHash:    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		AmountSatoshi: 0,
	})
	if err == nil {
		t.Fatalf("expected invalid amount to be rejected")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_purchases WHERE demand_id=?`, "dmd_invalid").Scan(&count); err != nil {
		t.Fatalf("count biz_purchases: %v", err)
	}
	if count != 0 {
		t.Fatalf("invalid amount should not write rows: %d", count)
	}
}

func TestDirectPurchaseFlowWritesSeedPurchase(t *testing.T) {
	env := newDirectPurchaseFlowEnv(t)
	defer env.cleanup()

	var got purchaseItem
	if err := env.db.QueryRow(`SELECT id,demand_id,seller_pub_hex,arbiter_pub_hex,chunk_index,object_hash,amount_satoshi,status,error_message,created_at_unix,finished_at_unix
		FROM biz_purchases WHERE demand_id=? AND chunk_index=0 ORDER BY id DESC LIMIT 1`, env.demandID).
		Scan(&got.ID, &got.DemandID, &got.SellerPubHex, &got.ArbiterPubHex, &got.ChunkIndex, &got.ObjectHash, &got.AmountSatoshi, &got.Status, &got.ErrorMessage, &got.CreatedAtUnix, &got.FinishedAtUnix); err != nil {
		t.Fatalf("query seed purchase: %v", err)
	}
	if got.Status != "done" || got.ChunkIndex != 0 || got.ObjectHash != env.seedHash {
		t.Fatalf("seed purchase mismatch: %+v", got)
	}
	if got.AmountSatoshi != env.seedPrice {
		t.Fatalf("seed amount mismatch: got=%d want=%d", got.AmountSatoshi, env.seedPrice)
	}
	if got.ErrorMessage != "" {
		t.Fatalf("seed success should not carry error: %q", got.ErrorMessage)
	}
	if got.FinishedAtUnix <= 0 {
		t.Fatalf("seed finished_at_unix should be set: %+v", got)
	}
}

func TestDirectPurchaseFlowWritesChunkPurchase(t *testing.T) {
	env := newDirectPurchaseFlowEnv(t)
	defer env.cleanup()

	chunkHash := env.seedMeta.ChunkHashes[1]
	chunk, _, err := env.workers[0].fetchChunk(context.Background(), 1, chunkHash)
	if err != nil {
		t.Fatalf("fetch chunk: %v", err)
	}
	if len(chunk) == 0 {
		t.Fatalf("chunk bytes should not be empty")
	}

	var got purchaseItem
	if err := env.db.QueryRow(`SELECT id,demand_id,seller_pub_hex,arbiter_pub_hex,chunk_index,object_hash,amount_satoshi,status,error_message,created_at_unix,finished_at_unix
		FROM biz_purchases WHERE demand_id=? AND chunk_index=1 ORDER BY id DESC LIMIT 1`, env.demandID).
		Scan(&got.ID, &got.DemandID, &got.SellerPubHex, &got.ArbiterPubHex, &got.ChunkIndex, &got.ObjectHash, &got.AmountSatoshi, &got.Status, &got.ErrorMessage, &got.CreatedAtUnix, &got.FinishedAtUnix); err != nil {
		t.Fatalf("query chunk purchase: %v", err)
	}
	if got.Status != "done" || got.ChunkIndex != 1 || got.ObjectHash != chunkHash {
		t.Fatalf("chunk purchase mismatch: %+v", got)
	}
	if got.AmountSatoshi != env.chunkPrice {
		t.Fatalf("chunk amount mismatch: got=%d want=%d", got.AmountSatoshi, env.chunkPrice)
	}
	if got.ErrorMessage != "" {
		t.Fatalf("chunk success should not carry error: %q", got.ErrorMessage)
	}
	if got.FinishedAtUnix <= 0 {
		t.Fatalf("chunk finished_at_unix should be set: %+v", got)
	}

	summary, err := dbSummarizeDemandPurchases(context.Background(), newClientDB(env.db, nil), env.demandID)
	if err != nil {
		t.Fatalf("summary biz_purchases: %v", err)
	}
	if summary.ChunkPurchaseCount != 1 {
		t.Fatalf("chunk summary mismatch: %+v", summary)
	}
}

func TestPrePaymentFailureDoesNotWritePurchase(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	demandID := "dmd_no_write"
	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, demandID, "seed_no_write", 1700000010); err != nil {
		t.Fatalf("insert demand: %v", err)
	}

	w := &transferSellerWorker{
		store: store,
		quote: DirectQuoteItem{
			DemandID:     demandID,
			SellerPubHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SeedPrice:    120,
			ChunkPrice:   33,
		},
		arbiterPubHex: "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		seedHash:      "seed_no_write",
	}

	if err := w.ensureSession(ctx); err == nil {
		t.Fatalf("expected ensureSession to fail")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_purchases WHERE demand_id=?`, demandID).Scan(&count); err != nil {
		t.Fatalf("count biz_purchases: %v", err)
	}
	if count != 0 {
		t.Fatalf("unexpected purchase rows written on pre-payment failure: %d", count)
	}
}

type directPurchaseFlowEnv struct {
	db         *sql.DB
	store      *clientDB
	buyer      *Runtime
	workers    []*transferSellerWorker
	seedMeta   seedV1Meta
	seedHash   string
	demandID   string
	seedPrice  uint64
	chunkPrice uint64
	cleanup    func()
}

func newDirectPurchaseFlowEnv(t *testing.T) directPurchaseFlowEnv {
	t.Helper()

	base := t.TempDir()
	wsDir := filepath.Join(base, "ws")
	dataDir := filepath.Join(base, "data")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	sellerHost, sellerPubHex, sellerPrivHex, sellerPriv := newPurchaseFlowHost(t)
	arbHost, arbPubHex, _, arbPriv := newPurchaseFlowHost(t)
	buyerHost, buyerPubHex, buyerPrivHex, buyerPriv := newPurchaseFlowHost(t)
	t.Cleanup(func() {
		_ = sellerHost.Close()
		_ = arbHost.Close()
		_ = buyerHost.Close()
	})

	sellerCfg := Config{}
	sellerCfg.Keys.PrivkeyHex = sellerPrivHex
	sellerCfg.Storage.DataDir = dataDir
	sellerCfg.Storage.WorkspaceDir = wsDir
	if err := ApplyConfigDefaults(&sellerCfg); err != nil {
		t.Fatalf("apply seller defaults: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = wsDir
	cfg.Storage.DataDir = dataDir
	cfg.Seller.Pricing.FloorPriceSatPer64K = 11
	cfg.Seller.Pricing.ResaleDiscountBPS = 8000
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	mgr := newTestWorkspaceManager(context.Background(), &cfg, db)
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure workspace: %v", err)
	}

	fullPath := filepath.Join(wsDir, "sample.bin")
	full := make([]byte, int(seedBlockSize*2+19))
	for i := range full {
		full[i] = byte((i * 17) % 251)
	}
	if err := os.WriteFile(fullPath, full, 0o644); err != nil {
		t.Fatalf("write sample file: %v", err)
	}
	seedBytes, seedHash, chunkCount, err := buildSeedV1(fullPath)
	if err != nil {
		t.Fatalf("build seed: %v", err)
	}
	if chunkCount < 2 {
		t.Fatalf("test seed needs at least 2 chunks, got=%d", chunkCount)
	}
	if _, err := mgr.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              fullPath,
		Seed:                  seedBytes,
		AvailableChunkIndexes: []uint32{0, 1},
		RecommendedFileName:   "sample.bin",
		MIMEHint:              "application/octet-stream",
	}); err != nil {
		t.Fatalf("register downloaded file: %v", err)
	}

	demandID := "dmd_purchase_flow"
	if err := dbRecordDemand(context.Background(), store, demandID, seedHash); err != nil {
		t.Fatalf("record demand: %v", err)
	}
	seedPrice := uint64(111)
	chunkPrice := uint64(22)
	if err := dbUpsertDirectQuote(context.Background(), store, directQuoteSubmitReq{
		DemandId:             demandID,
		SeedPrice:            seedPrice,
		ChunkPrice:           chunkPrice,
		ChunkCount:           chunkCount,
		FileSize:             uint64(len(full)),
		ExpiresAtUnix:        time.Now().Add(10 * time.Minute).Unix(),
		RecommendedFileName:  "sample.bin",
		MimeHint:             "application/octet-stream",
		ArbiterPubkeyHexes:   []string{arbPubHex},
		AvailableChunkBitmap: []byte{0xff},
	}, sellerPubHex); err != nil {
		t.Fatalf("upsert quote: %v", err)
	}

	if _, err := db.Exec(`INSERT OR REPLACE INTO proc_chain_tip_state(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms) VALUES(1,100,?, '', 'chain_utxo_worker', 'test', 1)`, time.Now().Unix()); err != nil {
		t.Fatalf("seed chain tip state: %v", err)
	}
	buyUTXOID := strings.Repeat("11", 32)
	buyerAddr, err := clientWalletAddress(&Runtime{runIn: RunInput{EffectivePrivKeyHex: buyerPrivHex, BSV: struct{ Network string }{Network: "test"}}})
	if err != nil {
		t.Fatalf("derive buyer wallet address: %v", err)
	}
	if err := seedWalletUTXOsForKernelTest(db, buyerAddr, []poolcore.UTXO{{TxID: buyUTXOID, Vout: 0, Value: 200000}}); err != nil {
		t.Fatalf("seed buyer utxos: %v", err)
	}

	if err := connectPurchaseHosts(buyerHost, sellerHost, arbHost, sellerPriv, arbPriv, buyerPriv); err != nil {
		t.Fatalf("connect purchase hosts: %v", err)
	}
	registerSellerHandlers(sellerHost, store, nil, nil, sellerCfg)

	frontOrderID := "fo_purchase_flow"
	if err := dbUpsertFrontOrder(context.Background(), store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   buyerPubHex,
		TargetObjectType: "demand",
		TargetObjectID:   demandID,
		Status:           "pending",
		Note:             "direct purchase flow test",
		Payload: map[string]any{
			"demand_id": demandID,
			"seed_hash": seedHash,
		},
	}); err != nil {
		t.Fatalf("upsert front order: %v", err)
	}

	buyer := &Runtime{
		Host: buyerHost,
		ActionChain: &directPurchaseMockChain{
			utxos: []poolcore.UTXO{{TxID: buyUTXOID, Vout: 0, Value: 200000}},
		},
		runIn: RunInput{
			ClientID:            buyerPubHex,
			EffectivePrivKeyHex: buyerPrivHex,
			BSV:                 struct{ Network string }{Network: "test"},
		},
	}
	buyer.runIn.Network.Arbiters = []PeerNode{{Enabled: true, Addr: arbHost.Addrs()[0].String() + "/p2p/" + arbHost.ID().String(), Pubkey: arbPubHex}}

	quotes, err := TriggerClientListDirectQuotes(context.Background(), store, demandID)
	if err != nil {
		t.Fatalf("list direct quotes: %v", err)
	}
	if len(quotes) != 1 {
		t.Fatalf("quote count mismatch: got=%d want=1", len(quotes))
	}

	workers, seedMeta, seed, err := prepareSpeedPriceWorkersAndSeed(speedPriceBootstrapParams{
		Ctx:           context.Background(),
		Buyer:         buyer,
		Store:         store,
		FrontOrderID:  frontOrderID,
		Quotes:        quotes,
		SeedHash:      seedHash,
		ArbiterPubHex: arbPubHex,
		PoolAmount:    20000,
	})
	if err != nil {
		t.Fatalf("prepare speed price workers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("worker count mismatch: got=%d want=1", len(workers))
	}
	if len(seed) == 0 || seedMeta.ChunkCount < 2 {
		t.Fatalf("seed meta mismatch: seed=%d meta=%+v", len(seed), seedMeta)
	}

	return directPurchaseFlowEnv{
		db:         db,
		store:      store,
		buyer:      buyer,
		workers:    workers,
		seedMeta:   seedMeta,
		seedHash:   seedHash,
		demandID:   demandID,
		seedPrice:  seedPrice,
		chunkPrice: chunkPrice,
		cleanup: func() {
			_ = closeTransferWorkers(context.Background(), workers)
		},
	}
}

func newPurchaseFlowHost(t *testing.T) (host.Host, string, string, crypto.PrivKey) {
	t.Helper()

	priv, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate secp256k1 key: %v", err)
	}
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	pubRaw, err := h.Peerstore().PubKey(h.ID()).Raw()
	if err != nil {
		_ = h.Close()
		t.Fatalf("host pubkey raw: %v", err)
	}
	privRaw, err := priv.Raw()
	if err != nil {
		_ = h.Close()
		t.Fatalf("host privkey raw: %v", err)
	}
	return h, strings.ToLower(hex.EncodeToString(pubRaw)), strings.ToLower(hex.EncodeToString(privRaw)), priv
}

func connectPurchaseHosts(buyerHost, sellerHost, arbHost host.Host, sellerPriv, arbPriv, buyerPriv crypto.PrivKey) error {
	ctx := context.Background()
	if buyerHost != nil && sellerHost != nil {
		if err := buyerHost.Peerstore().AddPubKey(sellerHost.ID(), sellerPriv.GetPublic()); err != nil {
			return err
		}
		if err := buyerHost.Connect(ctx, peer.AddrInfo{ID: sellerHost.ID(), Addrs: sellerHost.Addrs()}); err != nil {
			return err
		}
	}
	if buyerHost != nil && arbHost != nil {
		if err := buyerHost.Peerstore().AddPubKey(arbHost.ID(), arbPriv.GetPublic()); err != nil {
			return err
		}
		if err := buyerHost.Connect(ctx, peer.AddrInfo{ID: arbHost.ID(), Addrs: arbHost.Addrs()}); err != nil {
			return err
		}
	}
	if sellerHost != nil && buyerHost != nil {
		if err := sellerHost.Peerstore().AddPubKey(buyerHost.ID(), buyerPriv.GetPublic()); err != nil {
			return err
		}
	}
	return nil
}

type directPurchaseMockChain struct {
	utxos []poolcore.UTXO
}

func (m *directPurchaseMockChain) GetUTXOs(address string) ([]poolcore.UTXO, error) {
	return append([]poolcore.UTXO(nil), m.utxos...), nil
}

func (m *directPurchaseMockChain) GetTipHeight() (uint32, error) {
	return 100, nil
}

func (m *directPurchaseMockChain) Broadcast(txHex string) (string, error) {
	txObj, err := transaction.NewTransactionFromHex(txHex)
	if err != nil {
		return "", err
	}
	return strings.ToLower(txObj.TxID().String()), nil
}

func tableIndexNames(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? ORDER BY name ASC`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func tableHasFK(db *sql.DB, table, fromCol, refTable, refCol string) bool {
	rows, err := db.Query(`PRAGMA foreign_key_list(` + table + `)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id, seq                                  int64
			tbl, from, to, onUpdate, onDelete, match string
		)
		if err := rows.Scan(&id, &seq, &tbl, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return false
		}
		if strings.EqualFold(strings.TrimSpace(tbl), refTable) && strings.EqualFold(strings.TrimSpace(from), fromCol) && strings.EqualFold(strings.TrimSpace(to), refCol) {
			return true
		}
	}
	return false
}
