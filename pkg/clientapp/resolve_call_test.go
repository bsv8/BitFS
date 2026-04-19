//go:build with_indexresolve

package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage"
	oldproto "github.com/golang/protobuf/proto"
)

const defaultNodeResolveRoute = "index"

func TestCallAndResolveRoundTripOverP2P(t *testing.T) {
	t.Parallel()

	senderDB := openResolveCallTestDB(t)
	defer senderDB.Close()
	receiverDB := openResolveCallTestDB(t)
	defer receiverDB.Close()

	senderHost, _ := newSecpHost(t)
	defer senderHost.Close()
	receiverHost, receiverPubKeyHex := newSecpHost(t)
	defer receiverHost.Close()

	senderRT := &Runtime{Host: senderHost}
	receiverRT := &Runtime{Host: receiverHost, ctx: t.Context(), modules: newModuleRegistry()}
	senderStore := newClientDB(senderDB, nil)
	receiverStore := newClientDB(receiverDB, nil)
	closeModule, err := registerOptionalModules(t.Context(), receiverRT, receiverStore)
	if err != nil {
		t.Fatalf("register module failed: %v", err)
	}
	if closeModule != nil {
		t.Cleanup(closeModule)
	}
	registerNodeRouteHandlers(receiverRT, receiverStore)
	receiverSrv := &httpAPIServer{rt: receiverRT, db: receiverDB, store: receiverStore}
	receiverHandler, err := receiverSrv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	senderHost.Peerstore().AddAddrs(receiverHost.ID(), receiverHost.Addrs(), time.Minute)

	if _, err := receiverDB.Exec(
		`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("ab", 32),
		1,
		4096,
		"/tmp/movie.mp4",
		"movie.mp4",
		"video/mp4",
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"`+defaultNodeResolveRoute+`","seed_hash":"`+strings.Repeat("ab", 32)+`"}`))
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upsert route index status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	callOut, err := TriggerPeerCall(context.Background(), senderRT, TriggerPeerCallParams{
		To:          receiverPubKeyHex,
		Route:       inboxmessage.InboxMessageRoute,
		ContentType: "application/json",
		Body:        []byte(`{"subject":"hello","message":"world"}`),
		Store:       senderStore,
	})
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
	if !callOut.Ok || callOut.Code != "OK" {
		t.Fatalf("unexpected call response: %+v", callOut)
	}
	senderPubKeyHex, err := localPubKeyHex(senderHost)
	if err != nil {
		t.Fatalf("sender pubkey hex: %v", err)
	}

	var gotSenderPubKeyHex string
	var gotTargetInput string
	if err := receiverDB.QueryRow(`SELECT sender_pubkey_hex,target_input FROM proc_inbox_messages ORDER BY id DESC LIMIT 1`).Scan(&gotSenderPubKeyHex, &gotTargetInput); err != nil {
		t.Fatalf("select inbox row: %v", err)
	}
	if gotSenderPubKeyHex != senderPubKeyHex || gotTargetInput != receiverPubKeyHex {
		t.Fatalf("unexpected inbox row: sender=%s target=%s", gotSenderPubKeyHex, gotTargetInput)
	}

	capOut, err := TriggerPeerCall(context.Background(), senderRT, TriggerPeerCallParams{
		To:          receiverPubKeyHex,
		Route:       string(contractroute.RouteNodeV1CapabilitiesShow),
		ContentType: contractmessage.ContentTypeProto,
		Store:       senderStore,
	})
	if err != nil {
		t.Fatalf("capabilities_show failed: %v", err)
	}
	if !capOut.Ok {
		t.Fatalf("capabilities_show response not ok: %+v", capOut)
	}
	var capBody contractmessage.CapabilitiesShowBody
	if err := oldproto.Unmarshal(capOut.Body, &capBody); err != nil {
		t.Fatalf("decode capabilities_show body: %v", err)
	}
	foundIndexResolve := false
	for _, item := range capBody.Capabilities {
		if item != nil && strings.EqualFold(strings.TrimSpace(item.ID), "index_resolve") && item.Version == 1 {
			foundIndexResolve = true
			break
		}
	}
	if !foundIndexResolve {
		t.Fatalf("expected index_resolve capability, got: %+v", capBody.Capabilities)
	}
}

func TestHTTPAPICallResolveInboxAndRouteIndex(t *testing.T) {
	t.Parallel()

	senderDB := openResolveCallTestDB(t)
	defer senderDB.Close()
	receiverDB := openResolveCallTestDB(t)
	defer receiverDB.Close()

	senderHost, _ := newSecpHost(t)
	defer senderHost.Close()
	receiverHost, receiverPubKeyHex := newSecpHost(t)
	defer receiverHost.Close()

	senderRT := &Runtime{Host: senderHost}
	receiverRT := &Runtime{Host: receiverHost, ctx: t.Context(), modules: newModuleRegistry()}
	senderStore := newClientDB(senderDB, nil)
	receiverStore := newClientDB(receiverDB, nil)
	closeModule, err := registerOptionalModules(t.Context(), receiverRT, receiverStore)
	if err != nil {
		t.Fatalf("register module failed: %v", err)
	}
	if closeModule != nil {
		t.Cleanup(closeModule)
	}
	registerNodeRouteHandlers(receiverRT, receiverStore)
	receiverSrv := &httpAPIServer{rt: receiverRT, db: receiverDB, store: receiverStore}
	receiverHandler, err := receiverSrv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	senderHost.Peerstore().AddAddrs(receiverHost.ID(), receiverHost.Addrs(), time.Minute)

	senderSrv := &httpAPIServer{rt: senderRT, db: senderDB, store: senderStore}

	if _, err := receiverDB.Exec(
		`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("cd", 32),
		1,
		1024,
		"/tmp/song.mp3",
		"song.mp3",
		"audio/mpeg",
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"index.mp3","seed_hash":"`+strings.Repeat("cd", 32)+`"}`))
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("settings index resolve status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
			t.Fatalf("expected unified ok body: %s", rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/call", strings.NewReader(`{"to":"`+receiverPubKeyHex+`","route":"`+inboxmessage.InboxMessageRoute+`","content_type":"application/json","body":{"hello":"world"}}`))
		rec := httptest.NewRecorder()
		senderSrv.handleCall(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("call api status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode call api response: %v", err)
		}
		if ok, _ := body["ok"].(bool); !ok {
			t.Fatalf("expected call ok response: %s", rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/settings/inbox/messages", nil)
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("inbox list status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
			t.Fatalf("expected unified ok body: %s", rec.Body.String())
		}
		var body struct {
			Status string `json:"status"`
			Data   struct {
				Total int `json:"total"`
				Items []struct {
					ID int64 `json:"id"`
				} `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode inbox list: %v", err)
		}
		if body.Data.Total != 1 || len(body.Data.Items) != 1 {
			t.Fatalf("unexpected inbox list: %s", rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/inbox/messages", nil)
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("inbox list method check mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		var body struct {
			Status string `json:"status"`
			Error  struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode inbox list 405 response: %v", err)
		}
		if body.Status != "error" || body.Error.Code != "METHOD_NOT_ALLOWED" || body.Error.Message != "method not allowed" {
			t.Fatalf("unexpected inbox list 405 body: %s", rec.Body.String())
		}
	}

	{
		detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/settings/inbox/messages/detail?id="+strconv.FormatInt(1, 10), nil)
		detailRec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(detailRec, detailReq)
		if detailRec.Code != http.StatusOK {
			t.Fatalf("inbox detail status mismatch: got=%d body=%s", detailRec.Code, detailRec.Body.String())
		}
		if !strings.Contains(detailRec.Body.String(), `"body_json"`) {
			t.Fatalf("expected decoded json body in detail: %s", detailRec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/inbox/messages/detail?id=1", nil)
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("inbox detail method check mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		var body struct {
			Status string `json:"status"`
			Error  struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode inbox detail 405 response: %v", err)
		}
		if body.Status != "error" || body.Error.Code != "METHOD_NOT_ALLOWED" || body.Error.Message != "method not allowed" {
			t.Fatalf("unexpected inbox detail 405 body: %s", rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/inbox/messages", nil)
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("old inbox path should return 404, got=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"","seed_hash":"`+strings.Repeat("cd", 32)+`"}`))
		rec := httptest.NewRecorder()
		receiverHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("default route settings status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
	}

}

func TestDecorateQuotedPaymentOptionUsesRealQuoteStatus(t *testing.T) {
	t.Parallel()

	option := &ncall.PaymentOption{
		Scheme:        ncall.PaymentSchemePool2of2V1,
		PaymentDomain: "bitcast-domain",
		PricingMode:   "fixed_price",
	}
	quoted := feePoolServiceQuoteBuilt{
		QuoteStatus: "countered",
		ServiceQuote: payflow.ServiceQuote{
			ChargeAmountSatoshi: 25,
		},
		ChargeReason: "domain_query_fee",
	}

	got := decorateQuotedPeerCallPaymentOption(option, quoted.ServiceQuote.ChargeAmountSatoshi, quoted.ChargeReason, quoted.QuoteStatus, 1, "call")
	if got == nil {
		t.Fatalf("decorateQuotedPaymentOption returned nil")
	}
	if got.QuoteStatus != "countered" {
		t.Fatalf("quote status mismatch: got=%q", got.QuoteStatus)
	}
	if got.PricingMode != "fixed_price" {
		t.Fatalf("pricing mode mismatch: got=%q", got.PricingMode)
	}
	if got.ServiceQuantity != 1 || got.ServiceQuantityUnit != "call" {
		t.Fatalf("service quantity mismatch: %+v", got)
	}
}
