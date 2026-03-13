package clientapp

import "testing"

func TestNormalizeBSVNetwork(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "empty->test", in: "", want: "test"},
		{name: "test", in: "test", want: "test"},
		{name: "testnet", in: "testnet", want: "test"},
		{name: "main", in: "main", want: "main"},
		{name: "mainnet", in: "mainnet", want: "main"},
		{name: "trim and case", in: "  MAIN  ", want: "main"},
		{name: "invalid", in: "dev", wantErr: true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeBSVNetwork(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeBSVNetwork(%q) want error", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeBSVNetwork(%q) error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeBSVNetwork(%q)=%q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNetworkInitDefaults(t *testing.T) {
	t.Parallel()

	testDefaults, err := networkInitDefaults("test")
	if err != nil {
		t.Fatalf("networkInitDefaults(test): %v", err)
	}
	if got, want := testDefaults.ListenRenewThresholdSeconds, uint32(5); got != want {
		t.Fatalf("test renew_threshold=%d, want %d", got, want)
	}
	if got, want := testDefaults.ListenTickSeconds, uint32(1); got != want {
		t.Fatalf("test tick=%d, want %d", got, want)
	}
	if got, want := testDefaults.IndexBackend, "sqlite"; got != want {
		t.Fatalf("test index.backend=%q, want %q", got, want)
	}
	if got, want := testDefaults.HTTPListenAddr, "127.0.0.1:18080"; got != want {
		t.Fatalf("test http.listen_addr=%q, want %q", got, want)
	}
	if got, want := testDefaults.FSHTTPListenAddr, "127.0.0.1:18090"; got != want {
		t.Fatalf("test fs_http.listen_addr=%q, want %q", got, want)
	}
	if got, want := testDefaults.FSHTTPPrefetchDistanceChunks, uint32(8); got != want {
		t.Fatalf("test fs_http.prefetch_distance_chunks=%d, want %d", got, want)
	}
	if got := len(testDefaults.DefaultGateways); got != 1 {
		t.Fatalf("test default_gateways len=%d, want 1", got)
	}
	if got := len(testDefaults.DefaultArbiters); got != 1 {
		t.Fatalf("test default_arbiters len=%d, want 1", got)
	}

	mainDefaults, err := networkInitDefaults("main")
	if err != nil {
		t.Fatalf("networkInitDefaults(main): %v", err)
	}
	if got, want := mainDefaults.ListenRenewThresholdSeconds, uint32(1800); got != want {
		t.Fatalf("main renew_threshold=%d, want %d", got, want)
	}
	if got, want := mainDefaults.ListenTickSeconds, uint32(30); got != want {
		t.Fatalf("main tick=%d, want %d", got, want)
	}
	if got, want := mainDefaults.IndexBackend, "sqlite"; got != want {
		t.Fatalf("main index.backend=%q, want %q", got, want)
	}
	if got, want := mainDefaults.HTTPListenAddr, "127.0.0.1:18080"; got != want {
		t.Fatalf("main http.listen_addr=%q, want %q", got, want)
	}
	if got, want := mainDefaults.FSHTTPListenAddr, "127.0.0.1:18090"; got != want {
		t.Fatalf("main fs_http.listen_addr=%q, want %q", got, want)
	}
	if got, want := mainDefaults.FSHTTPPrefetchDistanceChunks, uint32(8); got != want {
		t.Fatalf("main fs_http.prefetch_distance_chunks=%d, want %d", got, want)
	}
	if got := len(mainDefaults.DefaultGateways); got != 1 {
		t.Fatalf("main default_gateways len=%d, want 1", got)
	}
	if got := len(mainDefaults.DefaultArbiters); got != 1 {
		t.Fatalf("main default_arbiters len=%d, want 1", got)
	}
}

func TestInitPeerNodesToPeerNodes(t *testing.T) {
	t.Parallel()

	in := []InitPeerNode{
		{Addr: " /ip4/127.0.0.1/tcp/7001/p2p/12D3KooWxxx ", Pubkey: " 02abcdef ", Note: "gw"},
	}
	got := initPeerNodesToPeerNodes(in)
	if len(got) != 1 {
		t.Fatalf("len=%d, want 1", len(got))
	}
	if !got[0].Enabled {
		t.Fatalf("enabled should be true")
	}
	if got[0].Addr != "/ip4/127.0.0.1/tcp/7001/p2p/12D3KooWxxx" {
		t.Fatalf("addr trimmed failed: %q", got[0].Addr)
	}
	if got[0].Pubkey != "02abcdef" {
		t.Fatalf("pubkey trimmed failed: %q", got[0].Pubkey)
	}
}
