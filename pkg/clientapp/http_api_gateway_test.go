package clientapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func TestHandleGatewayMasterPostAndHealth(t *testing.T) {
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

	hGw1, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new gw1 host: %v", err)
	}
	defer hGw1.Close()

	hGw2, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new gw2 host: %v", err)
	}
	defer hGw2.Close()

	gw1Pub, err := localPubKeyHex(hGw1)
	if err != nil {
		t.Fatalf("gw1 pubkey: %v", err)
	}
	gw2Pub, err := localPubKeyHex(hGw2)
	if err != nil {
		t.Fatalf("gw2 pubkey: %v", err)
	}
	gw1Addr := fmt.Sprintf("%s/p2p/%s", hGw1.Addrs()[0].String(), hGw1.ID().String())
	gw2Addr := fmt.Sprintf("%s/p2p/%s", hGw2.Addrs()[0].String(), hGw2.ID().String())

	cfg := Config{}
	cfg.Network.Gateways = []PeerNode{
		{Enabled: true, Addr: gw1Addr, Pubkey: strings.ToLower(gw1Pub)},
		{Enabled: true, Addr: gw2Addr, Pubkey: strings.ToLower(gw2Pub)},
	}
	rt := &Runtime{Host: hClient, Config: cfg}
	rt.gwManager = newGatewayManager(rt, hClient)
	if err := rt.gwManager.InitFromConfig(t.Context(), cfg.Network.Gateways); err != nil {
		t.Fatalf("init gateways: %v", err)
	}
	rt.HealthyGWs = rt.gwManager.GetConnectedGateways()

	srv := &httpAPIServer{rt: rt, h: hClient}

	healthReq := httptest.NewRequest(http.MethodGet, "/api/v1/gateways/health", nil)
	healthRec := httptest.NewRecorder()
	srv.handleGatewayHealth(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("health status mismatch: got=%d want=%d body=%s", healthRec.Code, http.StatusOK, healthRec.Body.String())
	}
	var healthBody struct {
		Total          int    `json:"total"`
		ConnectedTotal int    `json:"connected_total"`
		MasterPeerID   string `json:"master_peer_id"`
		Items          []struct {
			PeerID        string `json:"peer_id"`
			Connected     bool   `json:"connected"`
			Connectedness string `json:"connectedness"`
		} `json:"items"`
	}
	if err := json.Unmarshal(healthRec.Body.Bytes(), &healthBody); err != nil {
		t.Fatalf("decode health body: %v", err)
	}
	if healthBody.Total != 2 || healthBody.ConnectedTotal != 2 {
		t.Fatalf("unexpected health counters: %+v", healthBody)
	}
	if len(healthBody.Items) != 2 || !healthBody.Items[0].Connected || !healthBody.Items[1].Connected {
		t.Fatalf("unexpected health items: %+v", healthBody.Items)
	}

	postBadReq := httptest.NewRequest(http.MethodPost, "/api/v1/gateways/master", strings.NewReader(`{"master_peer_id":"12D3KooWBadPeerID"}`))
	postBadRec := httptest.NewRecorder()
	srv.handleGatewayMaster(postBadRec, postBadReq)
	if postBadRec.Code != http.StatusBadRequest {
		t.Fatalf("set master bad status mismatch: got=%d want=%d", postBadRec.Code, http.StatusBadRequest)
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/gateways/master", strings.NewReader(`{"master_peer_id":"`+hGw2.ID().String()+`"}`))
	postRec := httptest.NewRecorder()
	srv.handleGatewayMaster(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("set master status mismatch: got=%d want=%d body=%s", postRec.Code, http.StatusOK, postRec.Body.String())
	}
	if got := rt.gwManager.GetMasterGateway().String(); got != hGw2.ID().String() {
		t.Fatalf("master gateway mismatch: got=%s want=%s", got, hGw2.ID().String())
	}

	// 确保健康接口反映新的 master。
	time.Sleep(20 * time.Millisecond)
	healthRec2 := httptest.NewRecorder()
	srv.handleGatewayHealth(healthRec2, healthReq)
	var healthBody2 struct {
		MasterPeerID string `json:"master_peer_id"`
	}
	if err := json.Unmarshal(healthRec2.Body.Bytes(), &healthBody2); err != nil {
		t.Fatalf("decode health body2: %v", err)
	}
	if healthBody2.MasterPeerID != hGw2.ID().String() {
		t.Fatalf("master_peer_id mismatch in health: got=%s want=%s", healthBody2.MasterPeerID, hGw2.ID().String())
	}
}
