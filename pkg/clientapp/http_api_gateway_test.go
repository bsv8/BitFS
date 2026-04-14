package clientapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleGatewayMasterPostAndHealth(t *testing.T) {
	t.Parallel()

	hClient, _ := newSecpHost(t)
	defer hClient.Close()

	hGw1, _ := newSecpHost(t)
	defer hGw1.Close()

	hGw2, gw2Pub := newSecpHost(t)
	defer hGw2.Close()

	gw1Pub, err := localPubKeyHex(hGw1)
	if err != nil {
		t.Fatalf("gw1 pubkey: %v", err)
	}
	gw1Addr := fmt.Sprintf("%s/p2p/%s", hGw1.Addrs()[0].String(), hGw1.ID().String())
	gw2Addr := fmt.Sprintf("%s/p2p/%s", hGw2.Addrs()[0].String(), hGw2.ID().String())

	cfg := Config{}
	cfg.Network.Gateways = []PeerNode{
		{Enabled: true, Addr: gw1Addr, Pubkey: strings.ToLower(gw1Pub)},
		{Enabled: true, Addr: gw2Addr, Pubkey: strings.ToLower(gw2Pub)},
	}
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(hClient))
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
		MasterPeerID   string `json:"master_gateway_pubkey_hex"`
		Items          []struct {
			PeerID        string `json:"transport_peer_id"`
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

	postBadReq := httptest.NewRequest(http.MethodPost, "/api/v1/gateways/master", strings.NewReader(`{"master_gateway_pubkey_hex":"12D3KooWBadPeerID"}`))
	postBadRec := httptest.NewRecorder()
	srv.handleGatewayMaster(postBadRec, postBadReq)
	if postBadRec.Code != http.StatusBadRequest {
		t.Fatalf("set master bad status mismatch: got=%d want=%d", postBadRec.Code, http.StatusBadRequest)
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/gateways/master", strings.NewReader(`{"master_gateway_pubkey_hex":"`+hGw2.ID().String()+`"}`))
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
		MasterPeerID string `json:"master_gateway_pubkey_hex"`
	}
	if err := json.Unmarshal(healthRec2.Body.Bytes(), &healthBody2); err != nil {
		t.Fatalf("decode health body2: %v", err)
	}
	if healthBody2.MasterPeerID != hGw2.ID().String() {
		t.Fatalf("master_gateway_pubkey_hex mismatch in health: got=%s want=%s", healthBody2.MasterPeerID, hGw2.ID().String())
	}
}

func TestHandleGatewaysListAndAddFailedConnectionDoesNotBreakExisting(t *testing.T) {
	t.Parallel()

	hClient, _ := newSecpHost(t)
	defer hClient.Close()

	hGw1, _ := newSecpHost(t)
	defer hGw1.Close()

	gw1Pub, err := localPubKeyHex(hGw1)
	if err != nil {
		t.Fatalf("gw1 pubkey: %v", err)
	}
	gw1Addr := fmt.Sprintf("%s/p2p/%s", hGw1.Addrs()[0].String(), hGw1.ID().String())

	cfg := Config{}
	cfg.Network.Gateways = []PeerNode{{Enabled: true, Addr: gw1Addr, Pubkey: strings.ToLower(gw1Pub)}}
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(hClient))
	rt.gwManager = newGatewayManager(rt, hClient)
	if err := rt.gwManager.InitFromConfig(t.Context(), cfg.Network.Gateways); err != nil {
		t.Fatalf("init gateways: %v", err)
	}
	rt.HealthyGWs = rt.gwManager.GetConnectedGateways()

	srv := &httpAPIServer{rt: rt, h: hClient}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
	listRec := httptest.NewRecorder()
	srv.handleGateways(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	var listBody struct {
		Items []struct {
			ID            int    `json:"id"`
			PeerID        string `json:"transport_peer_id"`
			Connected     bool   `json:"connected"`
			Connectedness string `json:"connectedness"`
			FeePoolReady  bool   `json:"fee_pool_ready"`
			LastError     string `json:"last_error"`
			RuntimeError  string `json:"last_runtime_error"`
			RuntimeStage  string `json:"last_runtime_error_stage"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("decode list body: %v", err)
	}
	if len(listBody.Items) != 1 || !listBody.Items[0].Connected {
		t.Fatalf("unexpected list body: %+v", listBody.Items)
	}
	if listBody.Items[0].Connectedness != "connected" {
		t.Fatalf("unexpected connectedness: got=%s want=connected", listBody.Items[0].Connectedness)
	}
	if !listBody.Items[0].FeePoolReady {
		t.Fatalf("fee_pool_ready should be true before runtime error")
	}
	rt.gwManager.SetRuntimeError(hGw1.ID(), "fee_pool_open", fmt.Errorf("query utxos failed"))

	hGwFail, gwFailPub := newSecpHost(t)
	// 关闭 gw2，构造一个可解析但不可连接的地址，验证失败网关不会影响已连接网关。
	gw2PeerID := hGwFail.ID().String()
	_ = hGwFail.Close()
	gw2Addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/1/p2p/%s", gw2PeerID)
	_, err = rt.gwManager.AddGateway(t.Context(), PeerNode{
		Enabled: true,
		Addr:    gw2Addr,
		Pubkey:  strings.ToLower(gwFailPub),
	})
	if err != nil {
		t.Fatalf("add gateway via manager: %v", err)
	}

	rt.HealthyGWs = rt.gwManager.GetConnectedGateways()
	if len(rt.HealthyGWs) != 1 || rt.HealthyGWs[0].ID != hGw1.ID() {
		t.Fatalf("healthy gateways changed unexpectedly: %+v", rt.HealthyGWs)
	}

	listRec2 := httptest.NewRecorder()
	srv.handleGateways(listRec2, listReq)
	if listRec2.Code != http.StatusOK {
		t.Fatalf("list2 status mismatch: got=%d want=%d body=%s", listRec2.Code, http.StatusOK, listRec2.Body.String())
	}
	var listBody2 struct {
		Items []struct {
			PeerID        string `json:"transport_peer_id"`
			Connected     bool   `json:"connected"`
			Connectedness string `json:"connectedness"`
			FeePoolReady  bool   `json:"fee_pool_ready"`
			LastError     string `json:"last_error"`
			RuntimeError  string `json:"last_runtime_error"`
			RuntimeStage  string `json:"last_runtime_error_stage"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listRec2.Body.Bytes(), &listBody2); err != nil {
		t.Fatalf("decode list body2: %v", err)
	}
	if len(listBody2.Items) != 2 {
		t.Fatalf("unexpected list item count: got=%d want=2", len(listBody2.Items))
	}
	var okFirst, badSecond bool
	for _, it := range listBody2.Items {
		switch it.PeerID {
		case hGw1.ID().String():
			okFirst = it.Connected && it.Connectedness == "connected" && !it.FeePoolReady && it.RuntimeStage == "fee_pool_open" && it.RuntimeError != ""
		case gw2PeerID:
			badSecond = !it.Connected && it.LastError != ""
		}
	}
	if !okFirst || !badSecond {
		t.Fatalf("unexpected gateway states after add fail: %+v", listBody2.Items)
	}
}
