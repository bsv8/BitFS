package clientapp

import (
	"context"
	"reflect"
	"testing"
)

func TestClientDBInitSkipsLegacyDemandTables(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	legacyTables := []string{
		"demand_dedup",
		"direct_quotes",
		"direct_sessions",
		"sale_records",
	}
	for _, table := range legacyTables {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("has table %s: %v", table, err)
		}
		if exists {
			t.Fatalf("legacy table should not exist anymore: %s", table)
		}
	}
}

func TestDemandQuoteCurrentSchema(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	seller := "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, "dmd_overwrite", "seed_overwrite", 1700000001); err != nil {
		t.Fatalf("insert demand: %v", err)
	}
	quoteReq := directQuoteSubmitReq{
		DemandId:            "dmd_overwrite",
		SellerPubkeyHex:     seller,
		SeedPrice:           100,
		ChunkPrice:          10,
		ChunkCount:          4,
		FileSize:            1234,
		ExpiresAtUnix:       1893427200,
		RecommendedFileName: "first.bin",
		MimeHint:            "application/octet-stream",
		ArbiterPubkeyHexes: []string{
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
	quoteReq.MimeHint = "text/plain"
	quoteReq.ArbiterPubkeyHexes = []string{
		"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		"02eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}
	quoteReq.AvailableChunkBitmap = []byte{0x0f}
	if err := dbUpsertDirectQuote(context.Background(), store, quoteReq, seller); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var got demandQuoteItem
	if err := db.QueryRow(`SELECT id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix FROM biz_demand_quotes WHERE demand_id=? AND seller_pub_hex=?`, "dmd_overwrite", seller).
		Scan(&got.ID, &got.DemandID, &got.SellerPubHex, &got.SeedPriceSatoshi, &got.ChunkPriceSatoshi, &got.ChunkCount, &got.FileSizeBytes, &got.RecommendedFileName, &got.MimeType, &got.AvailableChunkBitmapHex, &got.ExpiresAtUnix, &got.CreatedAtUnix); err != nil {
		t.Fatalf("load quote: %v", err)
	}
	if got.SeedPriceSatoshi != 200 || got.ChunkPriceSatoshi != 12 || got.FileSizeBytes != 4321 {
		t.Fatalf("quote not overwritten: %+v", got)
	}
	rows, err := db.Query(`SELECT arbiter_pub_hex FROM biz_demand_quote_arbiters WHERE quote_id=? ORDER BY arbiter_pub_hex`, got.ID)
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
	arbHost1, arb1 := newSecpHost(t)
	defer arbHost1.Close()
	arbHost2, arb2 := newSecpHost(t)
	defer arbHost2.Close()
	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, "dmd_read", "seed_read", 1700000001); err != nil {
		t.Fatalf("insert demand: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quotes(
			demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"dmd_read", seller, 123, 11, 5, 8192, "read.bin", "application/json", "ff", 1893427200, 1700000002,
	); err != nil {
		t.Fatalf("insert quote: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quote_arbiters(quote_id,arbiter_pub_hex) VALUES(?,?),(?,?)`, 1, arb1, 1, arb2); err != nil {
		t.Fatalf("insert arbiters: %v", err)
	}

	quotes, err := TriggerClientListDirectQuotes(context.Background(), store, "dmd_read")
	if err != nil {
		t.Fatalf("list quotes: %v", err)
	}
	if len(quotes) != 1 {
		t.Fatalf("unexpected quote count: %d", len(quotes))
	}
	if quotes[0].SellerPubHex != seller {
		t.Fatalf("seller mismatch: %s", quotes[0].SellerPubHex)
	}
	if !reflect.DeepEqual(quotes[0].SellerArbiterPubHexes, []string{arb1, arb2}) {
		t.Fatalf("arbiter list mismatch: %+v", quotes[0].SellerArbiterPubHexes)
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

	buyer := newRuntimeForTest(t, Config{}, "")
	mustUpdateRuntimeConfigMemoryOnly(t, buyer, func(cfg *Config) {
		cfg.Network.Arbiters = []PeerNode{
			{Enabled: true, Addr: arbHost2.Addrs()[0].String() + "/p2p/" + arbHost2.ID().String(), Pubkey: arb2},
			{Enabled: true, Addr: arbHost1.Addrs()[0].String() + "/p2p/" + arbHost1.ID().String(), Pubkey: arb1},
		}
	})
	gotArbiter, err := resolveDealArbiter(buyer, quotes[0].SellerArbiterPubHexes, "")
	if err != nil {
		t.Fatalf("resolve arbiter: %v", err)
	}
	if gotArbiter != arb2 {
		t.Fatalf("unexpected arbiter: got=%s want=%s", gotArbiter, arb2)
	}
}
