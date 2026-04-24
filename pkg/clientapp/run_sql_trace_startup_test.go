package clientapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/bsv8/BitFS/pkg/clientapp/storeactor"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

// 启动回归只盯组装层，不让 SQL trace 再被空日志路径卡死。
func TestRun_SQLTraceStartupBackfillsEmptyLogFile(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.DataDir = dataDir
	cfg.Storage.MinFreeBytes = 1
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.Enabled = true
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Debug = true
	cfg.Log.File = ""
	cfg.Seller.Enabled = false
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"

	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeTest); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	// 这里故意保留空路径，回归点就是 Run 需要自己补齐。
	cfg.Log.File = ""
	listenEnabled := false
	cfg.Listen.Enabled = &listenEnabled
	autoAnnounceEnabled := false
	cfg.Reachability.AutoAnnounceEnabled = &autoAnnounceEnabled
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	// 这条回归只盯 SQL trace 初始化，不需要把 HTTP/FSHTTP 一起拉起来，
	// 否则整包并跑时容易把收尾卡在无关的监听线程上。
	cfg.HTTP.Enabled = false
	cfg.FSHTTP.Enabled = false
	cfg.Storage.MinFreeBytes = 1

	dbPath := cfg.Index.SQLitePath
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(cfg.Storage.DataDir, dbPath)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	openedDB, err := sqliteactor.Open(dbPath, true)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = openedDB.Actor.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ensureClientSchemaOnDB(ctx, openedDB.DB); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	rt, err := Run(ctx, cfg, RunDeps{
		Store:   NewClientStore(openedDB.DB, openedDB.Actor),
		RawDB:   openedDB.DB,
		DBActor: openedDB.Actor,
		OwnsDB:  true,
	}, RunOptions{
		StartupMode:         StartupModeTest,
		EffectivePrivKeyHex: cfg.Keys.PrivkeyHex,
		ActionChain:         startupTestChain{},
		WalletChain:         startupTestWalletChain{},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	traceDir := rt.SQLTraceDir()
	if traceDir == "" {
		t.Fatalf("expected sql trace dir to be initialized")
	}
	eventsPath := filepath.Join(traceDir, "sql_trace.jsonl")
	if _, err := os.Stat(eventsPath); err != nil {
		t.Fatalf("stat sql trace events: %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
	if _, err := os.Stat(eventsPath); err != nil {
		t.Fatalf("sql trace events should remain after close: %v", err)
	}
}

// 启动测试只需要最小费用池能力，不让它触发外部链。
type startupTestChain struct{}

func (startupTestChain) GetUTXOs(string) ([]poolcore.UTXO, error) { return nil, nil }
func (startupTestChain) GetTipHeight() (uint32, error)            { return 0, nil }
func (startupTestChain) Broadcast(string) (string, error)         { return "", nil }

// 启动测试只需要钱包链接口能挂住，不走真实 WOC。
type startupTestWalletChain struct{}

func (startupTestWalletChain) BaseURL() string { return "" }
func (startupTestWalletChain) GetAddressConfirmedUnspent(context.Context, string) ([]whatsonchain.UTXO, error) {
	return nil, nil
}
func (startupTestWalletChain) GetAddressBSV21TokenUnspent(context.Context, string) ([]whatsonchain.BSV21TokenUTXO, error) {
	return nil, nil
}
func (startupTestWalletChain) GetChainInfo(context.Context) (uint32, error) { return 0, nil }
func (startupTestWalletChain) GetAddressConfirmedHistory(context.Context, string) ([]whatsonchain.AddressHistoryItem, error) {
	return nil, nil
}
func (startupTestWalletChain) GetAddressConfirmedHistoryPage(context.Context, string, whatsonchain.ConfirmedHistoryQuery) (whatsonchain.ConfirmedHistoryPage, error) {
	return whatsonchain.ConfirmedHistoryPage{}, nil
}
func (startupTestWalletChain) GetAddressUnconfirmedHistory(context.Context, string) ([]string, error) {
	return nil, nil
}
func (startupTestWalletChain) GetTxHash(context.Context, string) (whatsonchain.TxDetail, error) {
	return whatsonchain.TxDetail{}, nil
}
func (startupTestWalletChain) GetTxHex(context.Context, string) (string, error) { return "", nil }
