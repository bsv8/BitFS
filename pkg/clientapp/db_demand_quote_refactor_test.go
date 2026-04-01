package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDemandQuoteLegacyMigration(t *testing.T) {
	t.Parallel()

	db := openClientDBForLegacyDemandQuoteTest(t)
	defer func() { _ = db.Close() }()

	if err := createLegacyDemandQuoteSchema(db); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO demand_dedup(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, " dmd_legacy ", " SEED_LEGACY ", 1700000001); err != nil {
		t.Fatalf("insert legacy demand: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO direct_quotes(
			demand_id,seller_pubkey_hex,seed_price,chunk_price,chunk_count,file_size,expires_at_unix,recommended_file_name,mime_hint,available_chunk_bitmap_hex,seller_arbiter_pubkey_hexes_json,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		" dmd_legacy ", "02AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 111, 22, 3, 4096, 1893427200, "legacy.bin", "Text/Plain", "ff", `[" 02CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC ","03DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD","02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]`, 1700000002,
	); err != nil {
		t.Fatalf("insert legacy quote: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db second pass: %v", err)
	}

	var demandCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM demands`).Scan(&demandCount); err != nil {
		t.Fatalf("count demands: %v", err)
	}
	if demandCount != 1 {
		t.Fatalf("unexpected demand count: %d", demandCount)
	}
	var seedHash string
	if err := db.QueryRow(`SELECT seed_hash FROM demands WHERE demand_id=?`, "dmd_legacy").Scan(&seedHash); err != nil {
		t.Fatalf("load demand: %v", err)
	}
	if seedHash != "seed_legacy" {
		t.Fatalf("seed hash mismatch: got=%q want=%q", seedHash, "seed_legacy")
	}

	var quote demandQuoteItem
	if err := db.QueryRow(`SELECT id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix FROM demand_quotes WHERE demand_id=?`, "dmd_legacy").
		Scan(&quote.ID, &quote.DemandID, &quote.SellerPubHex, &quote.SeedPriceSatoshi, &quote.ChunkPriceSatoshi, &quote.ChunkCount, &quote.FileSizeBytes, &quote.RecommendedFileName, &quote.MimeType, &quote.AvailableChunkBitmapHex, &quote.ExpiresAtUnix, &quote.CreatedAtUnix); err != nil {
		t.Fatalf("load quote: %v", err)
	}
	if quote.SellerPubHex != "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("seller pub hex mismatch: %s", quote.SellerPubHex)
	}
	if quote.MimeType != "text/plain" {
		t.Fatalf("mime type mismatch: %s", quote.MimeType)
	}
	var arbiterRows []string
	rows, err := db.Query(`SELECT arbiter_pub_hex FROM demand_quote_arbiters WHERE quote_id=? ORDER BY arbiter_pub_hex`, quote.ID)
	if err != nil {
		t.Fatalf("query arbiters: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var arb string
		if err := rows.Scan(&arb); err != nil {
			t.Fatalf("scan arbiter: %v", err)
		}
		arbiterRows = append(arbiterRows, arb)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iter arbiters: %v", err)
	}
	if !reflect.DeepEqual(arbiterRows, []string{
		"02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}) {
		t.Fatalf("arbiter migration mismatch: %+v", arbiterRows)
	}
}

func TestDemandQuoteOverwriteAndArbiters(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	seller := "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	quoteReq := directQuoteSubmitReq{
		DemandID:            "dmd_overwrite",
		SellerPeerID:        seller,
		SeedPrice:           100,
		ChunkPrice:          10,
		ChunkCount:          4,
		FileSize:            1234,
		ExpiresAtUnix:       1893427200,
		RecommendedFileName: "first.bin",
		MIMEHint:            "application/octet-stream",
		ArbiterPeerIDs: []string{
			"02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		},
		AvailableChunkBitmap: []byte{0xff},
	}
	if err := dbUpsertDirectQuote(context.Background(), store, quoteReq, seller); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	quoteReq.SeedPrice = 200
	quoteReq.ChunkPrice = 12
	quoteReq.FileSize = 4321
	quoteReq.RecommendedFileName = "second.bin"
	quoteReq.MIMEHint = "text/plain"
	quoteReq.ArbiterPeerIDs = []string{
		"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		"02eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}
	quoteReq.AvailableChunkBitmap = []byte{0x0f}
	if err := dbUpsertDirectQuote(context.Background(), store, quoteReq, seller); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var got demandQuoteItem
	if err := db.QueryRow(`SELECT id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix FROM demand_quotes WHERE demand_id=? AND seller_pub_hex=?`, "dmd_overwrite", seller).
		Scan(&got.ID, &got.DemandID, &got.SellerPubHex, &got.SeedPriceSatoshi, &got.ChunkPriceSatoshi, &got.ChunkCount, &got.FileSizeBytes, &got.RecommendedFileName, &got.MimeType, &got.AvailableChunkBitmapHex, &got.ExpiresAtUnix, &got.CreatedAtUnix); err != nil {
		t.Fatalf("load quote: %v", err)
	}
	if got.SeedPriceSatoshi != 200 || got.ChunkPriceSatoshi != 12 || got.FileSizeBytes != 4321 {
		t.Fatalf("quote not overwritten: %+v", got)
	}
	rows, err := db.Query(`SELECT arbiter_pub_hex FROM demand_quote_arbiters WHERE quote_id=? ORDER BY arbiter_pub_hex`, got.ID)
	if err != nil {
		t.Fatalf("load arbiters: %v", err)
	}
	defer rows.Close()
	var arbiters []string
	for rows.Next() {
		var arb string
		if err := rows.Scan(&arb); err != nil {
			t.Fatalf("scan arbiter: %v", err)
		}
		arbiters = append(arbiters, arb)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iter arbiters: %v", err)
	}
	if !reflect.DeepEqual(arbiters, []string{
		"02eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}) {
		t.Fatalf("arbiter overwrite mismatch: %+v", arbiters)
	}
}

func TestDemandQuoteReadPathsAndArbiterSelection(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	seller := "02ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	arb1 := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	arb2 := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := db.Exec(`INSERT INTO demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, "dmd_read", "seed_read", 1700000001); err != nil {
		t.Fatalf("insert demand: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO demand_quotes(
			demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"dmd_read", seller, 123, 11, 5, 8192, "read.bin", "application/json", "ff", 1893427200, 1700000002,
	); err != nil {
		t.Fatalf("insert quote: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO demand_quote_arbiters(quote_id,arbiter_pub_hex) VALUES(?,?),(?,?)`, 1, arb1, 1, arb2); err != nil {
		t.Fatalf("insert arbiters: %v", err)
	}

	quotes, err := TriggerClientListDirectQuotes(context.Background(), store, "dmd_read")
	if err != nil {
		t.Fatalf("list quotes: %v", err)
	}
	if len(quotes) != 1 {
		t.Fatalf("unexpected quote count: %d", len(quotes))
	}
	if quotes[0].SellerPeerID != seller {
		t.Fatalf("seller mismatch: %s", quotes[0].SellerPeerID)
	}
	if !reflect.DeepEqual(quotes[0].SellerArbiterPeerIDs, []string{arb1, arb2}) {
		t.Fatalf("arbiter list mismatch: %+v", quotes[0].SellerArbiterPeerIDs)
	}

	item, err := dbGetDemandQuoteItem(context.Background(), store, 1)
	if err != nil {
		t.Fatalf("get quote detail: %v", err)
	}
	if item.SellerPubHex != seller || item.MimeType != "application/json" {
		t.Fatalf("detail mismatch: %+v", item)
	}
	if !reflect.DeepEqual(item.SellerArbiterPubHexes, []string{arb1, arb2}) {
		t.Fatalf("detail arbiter mismatch: %+v", item.SellerArbiterPubHexes)
	}

	buyer := &Runtime{}
	buyer.runIn.Network.Arbiters = []PeerNode{
		{Enabled: true, Pubkey: arb2},
		{Enabled: true, Pubkey: "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
	}
	gotArbiter, err := resolveDealArbiter(buyer, quotes[0].SellerArbiterPeerIDs, "")
	if err != nil {
		t.Fatalf("resolve arbiter: %v", err)
	}
	if gotArbiter != arb2 {
		t.Fatalf("unexpected arbiter: got=%s want=%s", gotArbiter, arb2)
	}
}

func openClientDBForLegacyDemandQuoteTest(t *testing.T) *sql.DB {
	t.Helper()

	base := t.TempDir()
	dbPath := filepath.Join(base, "index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	return db
}

func createLegacyDemandQuoteSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE demand_dedup(
			demand_id TEXT PRIMARY KEY,
			seed_hash TEXT,
			created_at_unix INTEGER
		)`,
		`CREATE TABLE direct_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			seed_price INTEGER NOT NULL,
			chunk_price INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0,
			file_size INTEGER NOT NULL DEFAULT 0,
			expires_at_unix INTEGER NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT '',
			available_chunk_bitmap_hex TEXT NOT NULL DEFAULT '',
			seller_arbiter_pubkey_hexes_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
