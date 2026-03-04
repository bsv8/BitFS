package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/woc"
)

func main() {
	cfgPath := flag.String("config", "", "path to production client config yaml")
	privHex := flag.String("privkey_hex", "", "optional libp2p private key hex (MarshalPrivateKey bytes)")
	flag.Parse()
	if strings.TrimSpace(*cfgPath) == "" {
		log.Fatal("-config is required")
	}

	cfg, raw, err := clientapp.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	logFile, logConsoleMinLevel := clientapp.ResolveLogConfig(&cfg)
	if err := obs.Init(logFile, logConsoleMinLevel); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = obs.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	guardURL, stopGuard, err := woc.EnsureGuardRunning(ctx, woc.GuardRuntimeOptions{
		Network: cfg.BSV.Network,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer stopGuard()

	rt, err := clientapp.Run(ctx, cfg, clientapp.RunOptions{
		PrivKeyHexOverride: *privHex,
		ConfigRaw:          raw,
		WebAssets:          webAssets,
		Chain:              woc.NewGuardClient(guardURL),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = rt.Close() }()

	<-ctx.Done()
}
