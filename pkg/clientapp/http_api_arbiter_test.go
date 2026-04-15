package clientapp

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func newSecpHost(t *testing.T) (host.Host, string) {
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
		t.Fatalf("new secp host: %v", err)
	}
	pubRaw, err := h.Peerstore().PubKey(h.ID()).Raw()
	if err != nil {
		h.Close()
		t.Fatalf("host pubkey raw: %v", err)
	}
	return h, strings.ToLower(hex.EncodeToString(pubRaw))
}

func TestHandleArbitersCRUD(t *testing.T) {
	t.Parallel()

	hClient, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new client host: %v", err)
	}
	defer hClient.Close()

	hArb1, arb1Pub := newSecpHost(t)
	defer hArb1.Close()
	arb1Addr := fmt.Sprintf("%s/p2p/%s", hArb1.Addrs()[0].String(), hArb1.ID().String())

	cfg := Config{}
	cfg.Network.Arbiters = []PeerNode{}
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(hClient))
	srv := &httpAPIServer{rt: rt, h: hClient, cfgSource: staticConfigSnapshot(cfg)}

	// POST: 新增仲裁节点
	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/arbiters", strings.NewReader(fmt.Sprintf(`{"addr":"%s","pubkey":"%s","enabled":true}`, arb1Addr, strings.ToLower(arb1Pub))))
	postRec := httptest.NewRecorder()
	srv.handleArbiters(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("post status mismatch: got=%d want=%d body=%s", postRec.Code, http.StatusOK, postRec.Body.String())
	}

	// GET: 列表应包含 1 条
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/arbiters", nil)
	getRec := httptest.NewRecorder()
	srv.handleArbiters(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status mismatch: got=%d want=%d body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var listBody struct {
		Total int `json:"total"`
		Items []struct {
			ID      int    `json:"id"`
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
		} `json:"items"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("decode list body: %v", err)
	}
	if listBody.Total != 1 || len(listBody.Items) != 1 {
		t.Fatalf("arbiter list mismatch: total=%d len=%d", listBody.Total, len(listBody.Items))
	}
	if !listBody.Items[0].Enabled {
		t.Fatalf("arbiter enabled mismatch: got=%v want=true", listBody.Items[0].Enabled)
	}

	// PUT: 禁用仲裁节点
	putReq := httptest.NewRequest(http.MethodPut, "/api/v1/arbiters?id=0", strings.NewReader(fmt.Sprintf(`{"addr":"%s","pubkey":"%s","enabled":false}`, arb1Addr, strings.ToLower(arb1Pub))))
	putRec := httptest.NewRecorder()
	srv.handleArbiters(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status mismatch: got=%d want=%d body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	// DELETE: 禁用后可删除
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/arbiters?id=0", nil)
	delRec := httptest.NewRecorder()
	srv.handleArbiters(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete status mismatch: got=%d want=%d body=%s", delRec.Code, http.StatusOK, delRec.Body.String())
	}
	if len(rt.ConfigSnapshot().Network.Arbiters) != 0 {
		t.Fatalf("arbiter count mismatch after delete: got=%d want=0", len(rt.ConfigSnapshot().Network.Arbiters))
	}
}

func TestHandleArbiterHealth(t *testing.T) {
	t.Parallel()

	hClient, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new client host: %v", err)
	}
	defer hClient.Close()

	hArb1, arb1Pub := newSecpHost(t)
	defer hArb1.Close()
	arb1Addr := fmt.Sprintf("%s/p2p/%s", hArb1.Addrs()[0].String(), hArb1.ID().String())

	cfg := Config{}
	cfg.Network.Arbiters = []PeerNode{
		{Enabled: true, Addr: arb1Addr, Pubkey: strings.ToLower(arb1Pub)},
	}
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(hClient))
	srv := &httpAPIServer{rt: rt, h: hClient, cfgSource: staticConfigSnapshot(cfg)}

	healthReq := httptest.NewRequest(http.MethodGet, "/api/v1/arbiters/health", nil)
	healthRec := httptest.NewRecorder()
	srv.handleArbiterHealth(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("health status mismatch: got=%d want=%d body=%s", healthRec.Code, http.StatusOK, healthRec.Body.String())
	}
	var healthBody struct {
		Total          int `json:"total"`
		EnabledTotal   int `json:"enabled_total"`
		ConnectedTotal int `json:"connected_total"`
		HealthyTotal   int `json:"healthy_total"`
		Items          []struct {
			PeerID        string `json:"transport_peer_id"`
			Connected     bool   `json:"connected"`
			InHealthyArbs bool   `json:"in_healthy_arbiters"`
		} `json:"items"`
	}
	if err := json.Unmarshal(healthRec.Body.Bytes(), &healthBody); err != nil {
		t.Fatalf("decode health body: %v", err)
	}
	if healthBody.Total != 1 || healthBody.EnabledTotal != 1 {
		t.Fatalf("health counters mismatch: %+v", healthBody)
	}
	if len(healthBody.Items) != 1 || healthBody.Items[0].PeerID == "" {
		t.Fatalf("health items mismatch: %+v", healthBody.Items)
	}
	if !healthBody.Items[0].Connected {
		t.Fatalf("arbiter should be connected: %+v", healthBody.Items[0])
	}
}

func TestHandleArbitersBuiltInProtection(t *testing.T) {
	t.Parallel()

	hClient, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new client host: %v", err)
	}
	defer hClient.Close()

	cfg := Config{}
	cfg.BSV.Network = "test"
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := &Runtime{}
	svc, err := newRuntimeConfigService(cfg, "", StartupModeProduct)
	if err != nil {
		t.Fatalf("runtime config service: %v", err)
	}
	rt.config = svc
	srv := &httpAPIServer{rt: rt, h: hClient}

	defaults, err := networkInitDefaults("test")
	if err != nil {
		t.Fatalf("network defaults: %v", err)
	}
	target := defaults.DefaultArbiters[0]

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/arbiters", strings.NewReader(fmt.Sprintf(`{"addr":"%s","pubkey":"%s","enabled":true}`, target.Addr, target.Pubkey)))
	postRec := httptest.NewRecorder()
	srv.handleArbiters(postRec, postReq)
	if postRec.Code != http.StatusBadRequest || !strings.Contains(strings.ToLower(postRec.Body.String()), "cannot modify built-in arbiter") {
		t.Fatalf("built-in arbiter add should fail: code=%d body=%s", postRec.Code, postRec.Body.String())
	}

	putReq := httptest.NewRequest(http.MethodPut, "/api/v1/arbiters?id=0", strings.NewReader(fmt.Sprintf(`{"addr":"%s","pubkey":"%s","enabled":false}`, target.Addr, target.Pubkey)))
	putRec := httptest.NewRecorder()
	srv.handleArbiters(putRec, putReq)
	if putRec.Code != http.StatusBadRequest || !strings.Contains(strings.ToLower(putRec.Body.String()), "cannot disable built-in arbiter") {
		t.Fatalf("built-in arbiter disable should fail: code=%d body=%s", putRec.Code, putRec.Body.String())
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/arbiters?id=0", nil)
	delRec := httptest.NewRecorder()
	srv.handleArbiters(delRec, delReq)
	if delRec.Code != http.StatusBadRequest || !strings.Contains(strings.ToLower(delRec.Body.String()), "cannot delete built-in arbiter") {
		t.Fatalf("built-in arbiter delete should fail: code=%d body=%s", delRec.Code, delRec.Body.String())
	}
}
