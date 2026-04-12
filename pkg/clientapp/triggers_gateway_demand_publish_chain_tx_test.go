package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	"github.com/bsv8/gateway/pkg/gatewayapp"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	_ "modernc.org/sqlite"
)

type gatewayDemandPublishMockChain struct{}

func (gatewayDemandPublishMockChain) GetUTXOs(address string) ([]poolcore.UTXO, error) {
	return nil, nil
}
func (gatewayDemandPublishMockChain) GetTipHeight() (uint32, error)          { return 0, nil }
func (gatewayDemandPublishMockChain) Broadcast(txHex string) (string, error) { return "", nil }

type gatewayDemandPublishMockState struct {
	mu               sync.Mutex
	lastOffer        payflow.ServiceOffer
	lastServiceQuote []byte
	gatewayPubHex    string
}

func TestTriggerGatewayDemandPublishChainTxQuotePay_Success(t *testing.T) {
	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	clientPrivHex := "1111111111111111111111111111111111111111111111111111111111111111"
	gatewayPrivHex := "2222222222222222222222222222222222222222222222222222222222222222"

	clientHost, _ := newFixedSecpHost(t, clientPrivHex)
	defer clientHost.Close()
	gatewayHost, gatewayPubHex := newFixedSecpHostWithPub(t, gatewayPrivHex)
	defer gatewayHost.Close()

	gatewayDB := openGatewayStoreTestDB(t)
	gatewayStore := poolcoreGatewayStoreForTest(gatewayDB)
	registerGatewayDemandPublishMock(t, gatewayHost, gatewayStore, gatewayPrivHex, gatewayPubHex, 1, "dmd_gateway_demand_publish_1", false)

	clientID, err := clientIDFromPrivHex(clientPrivHex)
	if err != nil {
		t.Fatalf("derive client id failed: %v", err)
	}
	cfg := Config{}
	cfg.ClientID = clientID
	cfg.BSV.Network = "test"
	cfg.Network.Gateways = []PeerNode{
		{
			Enabled: true,
			Addr:    fmt.Sprintf("%s/p2p/%s", gatewayHost.Addrs()[0].String(), gatewayHost.ID().String()),
			Pubkey:  gatewayPubHex,
		},
	}

	rt := &Runtime{
		Host:        clientHost,
		runIn:       NewRunInputFromConfig(cfg, clientPrivHex),
		ActionChain: gatewayDemandPublishMockChain{},
	}
	rt.HealthyGWs = []peer.AddrInfo{{ID: gatewayHost.ID(), Addrs: gatewayHost.Addrs()}}
	if err := clientHost.Connect(context.Background(), peer.AddrInfo{ID: gatewayHost.ID(), Addrs: gatewayHost.Addrs()}); err != nil {
		t.Fatalf("connect client to gateway failed: %v", err)
	}

	clientAddr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("client wallet address failed: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, clientAddr, strings.ToLower(strings.TrimSpace(clientAddr)), 10000); err != nil {
		t.Fatalf("seed wallet rows failed: %v", err)
	}

	res, err := TriggerGatewayDemandPublishChainTxQuotePay(context.Background(), store, rt, PublishDemandParams{
		SeedHash:      strings.Repeat("ab", 32),
		ChunkCount:    1,
		GatewayPeerID: gatewayPubHex,
	})
	if err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	if !res.DemandPublished {
		t.Fatalf("demand should be published: %+v", res)
	}
	if res.DemandID == "" || res.PaymentTxID == "" || res.ServiceQuoteHash == "" {
		t.Fatalf("result fields should not be empty: %+v", res)
	}
	if res.GatewayPubkeyHex != gatewayPubHex {
		t.Fatalf("gateway pubkey mismatch: got=%s want=%s", res.GatewayPubkeyHex, gatewayPubHex)
	}
	if res.Status != "submitted" {
		t.Fatalf("status mismatch: got=%s want=submitted", res.Status)
	}

	var seedHash string
	if err := db.QueryRow(`SELECT seed_hash FROM biz_demands WHERE demand_id=?`, res.DemandID).Scan(&seedHash); err != nil {
		t.Fatalf("query biz_demands failed: %v", err)
	}
	if seedHash != strings.Repeat("ab", 32) {
		t.Fatalf("seed hash mismatch: got=%s", seedHash)
	}

	var chainID int64
	var paymentAttemptRef string
	if err := db.QueryRow(`SELECT id, settlement_payment_attempt_id FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, res.PaymentTxID).Scan(&chainID, &paymentAttemptRef); err != nil {
		t.Fatalf("query fact_settlement_channel_chain_quote_pay failed: %v", err)
	}
	if chainID == 0 {
		t.Fatal("chain payment id should not be zero")
	}
	var paymentAttemptID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_payment_attempts WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", chainID)).Scan(&paymentAttemptID); err != nil {
		t.Fatalf("query fact_settlement_payment_attempts failed: %v", err)
	}
	if paymentAttemptID == 0 {
		t.Fatal("settlement payment attempt id should not be zero")
	}
	if paymentAttemptRef != fmt.Sprintf("%d", paymentAttemptID) {
		t.Fatalf("payment attempt id mismatch: row=%s cycle=%d", paymentAttemptRef, paymentAttemptID)
	}

	row, found, err := gatewayStore.LoadServiceQuotePayment(res.ServiceQuoteHash)
	if err != nil {
		t.Fatalf("load gateway service quote payment failed: %v", err)
	}
	if !found {
		t.Fatal("gateway service quote payment should exist")
	}
	if !strings.EqualFold(strings.TrimSpace(row.PaymentTxID), res.PaymentTxID) {
		t.Fatalf("gateway payment txid mismatch: got=%s want=%s", row.PaymentTxID, res.PaymentTxID)
	}
	if !strings.EqualFold(strings.TrimSpace(row.ClientPubkeyHex), res.ClientPubkeyHex) {
		t.Fatalf("gateway client pubkey mismatch: got=%s want=%s", row.ClientPubkeyHex, res.ClientPubkeyHex)
	}
}

func TestTriggerGatewayDemandPublishChainTxQuotePay_NoHealthyGateway(t *testing.T) {
	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	clientHost, _ := newFixedSecpHost(t, "5555555555555555555555555555555555555555555555555555555555555555")
	defer clientHost.Close()
	rt := &Runtime{Host: clientHost}
	rt.runIn.BSV.Network = "test"
	if _, err := TriggerGatewayDemandPublishChainTxQuotePay(context.Background(), store, rt, PublishDemandParams{
		SeedHash:   strings.Repeat("ab", 32),
		ChunkCount: 1,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "gateway_pubkey_hex is required") {
		t.Fatalf("expected gateway_pubkey_hex is required, got=%v", err)
	}
}

func TestTriggerGatewayDemandPublishChainTxQuotePay_PayAfterValidationFailure(t *testing.T) {
	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	clientPrivHex := "3333333333333333333333333333333333333333333333333333333333333333"
	gatewayPrivHex := "4444444444444444444444444444444444444444444444444444444444444444"
	clientHost, _ := newFixedSecpHost(t, clientPrivHex)
	defer clientHost.Close()
	gatewayHost, gatewayPubHex := newFixedSecpHostWithPub(t, gatewayPrivHex)
	defer gatewayHost.Close()

	gatewayDB := openGatewayStoreTestDB(t)
	gatewayStore := poolcoreGatewayStoreForTest(gatewayDB)
	registerGatewayDemandPublishMock(t, gatewayHost, gatewayStore, gatewayPrivHex, gatewayPubHex, 1, "", true)

	clientID, err := clientIDFromPrivHex(clientPrivHex)
	if err != nil {
		t.Fatalf("derive client id failed: %v", err)
	}
	cfg := Config{}
	cfg.ClientID = clientID
	cfg.BSV.Network = "test"
	cfg.Network.Gateways = []PeerNode{
		{
			Enabled: true,
			Addr:    fmt.Sprintf("%s/p2p/%s", gatewayHost.Addrs()[0].String(), gatewayHost.ID().String()),
			Pubkey:  gatewayPubHex,
		},
	}
	rt := &Runtime{
		Host:        clientHost,
		runIn:       NewRunInputFromConfig(cfg, clientPrivHex),
		ActionChain: gatewayDemandPublishMockChain{},
	}
	rt.HealthyGWs = []peer.AddrInfo{{ID: gatewayHost.ID(), Addrs: gatewayHost.Addrs()}}
	if err := clientHost.Connect(context.Background(), peer.AddrInfo{ID: gatewayHost.ID(), Addrs: gatewayHost.Addrs()}); err != nil {
		t.Fatalf("connect client to gateway failed: %v", err)
	}

	clientAddr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("client wallet address failed: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, clientAddr, strings.ToLower(strings.TrimSpace(clientAddr)), 10000); err != nil {
		t.Fatalf("seed wallet rows failed: %v", err)
	}

	res, err := TriggerGatewayDemandPublishChainTxQuotePay(context.Background(), store, rt, PublishDemandParams{
		SeedHash:      strings.Repeat("cd", 32),
		ChunkCount:    1,
		GatewayPeerID: gatewayPubHex,
	})
	if err == nil {
		t.Fatalf("expected failure, got success: %+v", res)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "demand publish") {
		t.Fatalf("expected validation failure, got=%v", err)
	}
}

func TestValidateGatewayDemandPublishChainTxPreflight(t *testing.T) {
	if err := validateGatewayDemandPublishChainTxPreflight(ncall.CallResp{
		Ok:   false,
		Code: "PAYMENT_REQUIRED",
		PaymentSchemes: []*ncall.PaymentOption{
			{Scheme: ncall.PaymentSchemePool2of2V1},
		},
	}); err == nil || !strings.Contains(err.Error(), "chain_tx_v1 not offered") {
		t.Fatalf("expected chain_tx_v1 missing error, got=%v", err)
	}
	if err := validateGatewayDemandPublishChainTxPreflight(ncall.CallResp{
		Ok:   false,
		Code: "PAYMENT_REQUIRED",
		PaymentSchemes: []*ncall.PaymentOption{
			{Scheme: ncall.PaymentSchemeChainTxV1},
		},
	}); err != nil {
		t.Fatalf("expected preflight success, got=%v", err)
	}
}

func TestValidateGatewayDemandPublishChainTxQuoteBuilt(t *testing.T) {
	if err := validateGatewayDemandPublishChainTxQuoteBuilt(peerCallChainTxQuoteBuilt{}); err == nil || !strings.Contains(err.Error(), "service quote empty") {
		t.Fatalf("expected quote empty error, got=%v", err)
	}
	if err := validateGatewayDemandPublishChainTxQuoteBuilt(peerCallChainTxQuoteBuilt{
		ServiceQuoteRaw:  []byte("quote"),
		ServiceQuoteHash: "hash",
	}); err != nil {
		t.Fatalf("expected quote validation success, got=%v", err)
	}
}

func TestParseDemandPublishPaidResp_MissingTxID(t *testing.T) {
	respBody, err := oldproto.Marshal(&broadcastmodule.DemandPublishPaidResp{
		Success:   true,
		Status:    "ok",
		DemandID:  "dmd_1",
		Published: true,
	})
	if err != nil {
		t.Fatalf("marshal demand resp failed: %v", err)
	}
	receiptBody, err := oldproto.Marshal(&ncall.ChainTxV1Receipt{AcceptedAtUnix: time.Now().Unix(), PaymentTxID: ""})
	if err != nil {
		t.Fatalf("marshal receipt failed: %v", err)
	}
	_, _, err = parseDemandPublishPaidResp(ncall.CallResp{
		Ok:                   true,
		Code:                 "OK",
		Body:                 respBody,
		PaymentReceiptScheme: ncall.PaymentSchemeChainTxV1,
		PaymentReceipt:       receiptBody,
	})
	if err == nil || !strings.Contains(err.Error(), "payment txid missing") {
		t.Fatalf("expected payment txid missing error, got=%v", err)
	}
}

func newFixedSecpHost(t *testing.T, privHex string) (host.Host, string) {
	t.Helper()
	h, pubHex := newFixedSecpHostWithPub(t, privHex)
	return h, pubHex
}

func newFixedSecpHostWithPub(t *testing.T, privHex string) (host.Host, string) {
	t.Helper()
	privRaw, err := hex.DecodeString(strings.TrimSpace(privHex))
	if err != nil {
		t.Fatalf("decode priv hex failed: %v", err)
	}
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(privRaw)
	if err != nil {
		t.Fatalf("unmarshal secp256k1 private key failed: %v", err)
	}
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new host failed: %v", err)
	}
	pubRaw, err := h.Peerstore().PubKey(h.ID()).Raw()
	if err != nil {
		_ = h.Close()
		t.Fatalf("host pubkey raw failed: %v", err)
	}
	pubHex := strings.ToLower(hex.EncodeToString(pubRaw))
	return h, pubHex
}

func openGatewayStoreTestDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open gateway sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas failed: %v", err)
	}
	if err := poolcore.InitGatewayStore(db); err != nil {
		t.Fatalf("init gateway store failed: %v", err)
	}
	return db
}

type gatewayQuotePaymentStore interface {
	InsertServiceQuotePayment(poolcore.ServiceQuotePaymentRow) error
	LoadServiceQuotePayment(string) (poolcore.ServiceQuotePaymentRow, bool, error)
}

func poolcoreGatewayStoreForTest(db *sql.DB) gatewayQuotePaymentStore {
	return gatewayapp.NewGatewayStore(db, nil)
}

func registerGatewayDemandPublishMock(t *testing.T, h host.Host, store gatewayQuotePaymentStore, gatewayPrivHex string, gatewayPubHex string, quoteAmount uint64, demandID string, brokenDemandResp bool) {
	t.Helper()
	if h == nil || store == nil {
		t.Fatal("gateway mock dependencies are required")
	}
	state := &gatewayDemandPublishMockState{gatewayPubHex: strings.ToLower(strings.TrimSpace(gatewayPubHex))}
	gatewayPub, err := ec.PublicKeyFromString(state.gatewayPubHex)
	if err != nil {
		t.Fatalf("parse gateway pubkey failed: %v", err)
	}
	ncall.Register(h, nodeSecForRuntime(nil), func(ctx context.Context, meta ncall.CallContext, req ncall.CallReq) (ncall.CallResp, error) {
		switch strings.TrimSpace(req.Route) {
		case ncall.RoutePaymentV1Quote:
			var quoteReq poolcore.ServiceQuoteReq
			if err := oldproto.Unmarshal(req.Body, &quoteReq); err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			offer, err := payflow.UnmarshalServiceOffer(quoteReq.ServiceOffer)
			if err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			if err := offer.Validate(); err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			gatewayActor, err := poolcore.BuildActor("gateway", gatewayPrivHex, false)
			if err != nil {
				return ncall.CallResp{}, err
			}
			quote, err := payflow.SignServiceQuote(payflow.ServiceQuote{
				OfferHash:           mustHashServiceOfferForTest(offer),
				ChargeAmountSatoshi: quoteAmount,
				ExpiresAtUnix:       time.Now().Add(30 * time.Second).Unix(),
			}, gatewayActor.PrivKey)
			if err != nil {
				return ncall.CallResp{}, err
			}
			rawQuote, err := payflow.MarshalServiceQuote(quote)
			if err != nil {
				return ncall.CallResp{}, err
			}
			state.mu.Lock()
			state.lastOffer = offer
			state.lastServiceQuote = append([]byte(nil), rawQuote...)
			state.mu.Unlock()
			resp := poolcore.ServiceQuoteResp{
				Success:      true,
				Status:       "accepted",
				ServiceQuote: rawQuote,
			}
			return marshalNodeCallProto(&resp)
		case broadcastmodule.RouteBroadcastV1DemandPublish:
			if strings.TrimSpace(req.PaymentScheme) == "" {
				return ncall.CallResp{
					Ok:      false,
					Code:    "PAYMENT_REQUIRED",
					Message: "payment required",
					PaymentSchemes: []*ncall.PaymentOption{
						{Scheme: ncall.PaymentSchemePool2of2V1},
						{Scheme: ncall.PaymentSchemeChainTxV1},
					},
				}, nil
			}
			if strings.TrimSpace(req.PaymentScheme) != ncall.PaymentSchemeChainTxV1 {
				return ncall.CallResp{Ok: false, Code: "PAYMENT_SCHEME_UNSUPPORTED", Message: "payment scheme unsupported"}, nil
			}
			var payment ncall.ChainTxV1Payment
			if err := oldproto.Unmarshal(req.PaymentPayload, &payment); err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: "invalid payment payload"}, nil
			}
			tx, err := txsdk.NewTransactionFromBytes(payment.RawTx)
			if err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: "invalid payment raw_tx"}, nil
			}
			quoteRaw, err := extractDataPayloadFromTxBytes(payment.RawTx)
			if err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(quoteRaw, gatewayPub)
			if err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			var body broadcastmodule.DemandPublishReq
			if err := oldproto.Unmarshal(req.Body, &body); err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			payloadRaw, err := broadcastmodule.MarshalDemandPublishQuotePayload(body.SeedHash, body.ChunkCount, body.BuyerAddrs)
			if err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			state.mu.Lock()
			offer := state.lastOffer
			state.mu.Unlock()
			if err := poolcore.ValidateServiceQuoteBinding(quote, offer, state.gatewayPubHex, strings.ToLower(strings.TrimSpace(meta.SenderPubkeyHex)), broadcastmodule.QuoteServiceTypeDemandPublish, payloadRaw, time.Now().Unix()); err != nil {
				return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
			}
			demandResp := broadcastmodule.DemandPublishPaidResp{
				Success:   true,
				Status:    "ok",
				DemandID:  demandID,
				Published: true,
			}
			if brokenDemandResp {
				demandResp.DemandID = ""
			}
			demandRespRaw, err := oldproto.Marshal(&demandResp)
			if err != nil {
				return ncall.CallResp{}, err
			}
			servicePayloadRaw, err := broadcastmodule.MarshalDemandPublishServicePayload(demandResp)
			if err != nil {
				return ncall.CallResp{}, err
			}
			serviceReceiptRaw, err := poolcore.BuildSignedServiceReceipt(gatewayPrivHex, false, quote.OfferHash, broadcastmodule.ServiceTypeDemandPublish, servicePayloadRaw)
			if err != nil {
				return ncall.CallResp{}, err
			}
			if err := store.InsertServiceQuotePayment(poolcore.ServiceQuotePaymentRow{
				QuoteHash:       quoteHash,
				OfferHash:       quote.OfferHash,
				PaymentScheme:   ncall.PaymentSchemeChainTxV1,
				PaymentTxID:     strings.ToLower(strings.TrimSpace(tx.TxID().String())),
				ClientPubkeyHex: strings.ToLower(strings.TrimSpace(meta.SenderPubkeyHex)),
				AcceptedAtUnix:  time.Now().Unix(),
			}); err != nil {
				return ncall.CallResp{}, err
			}
			paymentReceiptRaw, err := oldproto.Marshal(&ncall.ChainTxV1Receipt{
				PaymentTxID:    strings.ToLower(strings.TrimSpace(tx.TxID().String())),
				AcceptedAtUnix: time.Now().Unix(),
			})
			if err != nil {
				return ncall.CallResp{}, err
			}
			return ncall.CallResp{
				Ok:                   true,
				Code:                 "OK",
				ContentType:          ncall.ContentTypeProto,
				Body:                 demandRespRaw,
				PaymentReceiptScheme: ncall.PaymentSchemeChainTxV1,
				PaymentReceipt:       paymentReceiptRaw,
				ServiceReceipt:       serviceReceiptRaw,
			}, nil
		default:
			return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
	}, nil)
}

func extractDataPayloadFromTxBytes(rawTx []byte) ([]byte, error) {
	tx, err := txsdk.NewTransactionFromBytes(rawTx)
	if err != nil {
		return nil, err
	}
	for _, out := range tx.Outputs {
		if out == nil || out.LockingScript == nil || !out.LockingScript.IsData() {
			continue
		}
		return payflow.ExtractDataPayloadFromScript(out.LockingScript)
	}
	return nil, fmt.Errorf("op_return payload missing")
}

func mustHashServiceOfferForTest(offer payflow.ServiceOffer) string {
	hash, err := payflow.HashServiceOffer(offer)
	if err != nil {
		return ""
	}
	return hash
}
