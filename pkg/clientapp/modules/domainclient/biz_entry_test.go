package domainclient

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type fakeBusinessStore struct {
	createCalled     bool
	createInput      CreateBusinessWithFrontTriggerAndPendingSettlementInput
	settlementUpdate []struct {
		settlementID string
		status       string
		targetType   string
		targetID     string
		errMsg       string
	}
}

type fakeRuntimePorts struct {
	clientID       string
	queryResp      DomainQueryResponse
	lockResp       DomainRegisterLockResponse
	submitResp     DomainRegisterSubmitResponse
	setTargetResp  DomainSetTargetResponse
	verifyQuote    DomainRegisterQuote
	resolvePubkey  string
	resolveErr     error
	resolveCalls   int
	lastRegisterTx []byte
	applyCount     int
	accountCount   int
	walletMu       *sync.Mutex
}

func (s *fakeBusinessStore) CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, input CreateBusinessWithFrontTriggerAndPendingSettlementInput) error {
	s.createCalled = true
	s.createInput = input
	return nil
}

func (s *fakeBusinessStore) UpsertFrontOrder(context.Context, FrontOrderEntry) error { return nil }
func (s *fakeBusinessStore) UpsertBusiness(context.Context, BusinessEntry) error     { return nil }
func (s *fakeBusinessStore) UpsertBusinessSettlement(context.Context, BusinessSettlementEntry) error {
	return nil
}
func (s *fakeBusinessStore) AppendBusinessTrigger(context.Context, BusinessTriggerEntry) error {
	return nil
}

func (s *fakeBusinessStore) UpdateOrderSettlement(_ context.Context, settlementID, status, targetType, targetID, errMsg string, _ int64) error {
	s.settlementUpdate = append(s.settlementUpdate, struct {
		settlementID string
		status       string
		targetType   string
		targetID     string
		errMsg       string
	}{
		settlementID: settlementID,
		status:       status,
		targetType:   targetType,
		targetID:     targetID,
		errMsg:       errMsg,
	})
	return nil
}

func (r *fakeRuntimePorts) ClientID(context.Context) (string, error) { return r.clientID, nil }

func (r *fakeRuntimePorts) EnsureDomainPeerConnected(context.Context, string, string) (string, peer.ID, error) {
	return "03" + strings.Repeat("11", 32), peer.ID("peer-1"), nil
}

func (r *fakeRuntimePorts) TriggerDomainQueryName(context.Context, string, peer.ID, string) (DomainQueryResponse, error) {
	return r.queryResp, nil
}

func (r *fakeRuntimePorts) TriggerDomainRegisterLock(context.Context, string, string, string, uint64) (DomainRegisterLockResponse, error) {
	return r.lockResp, nil
}

func (r *fakeRuntimePorts) VerifyRegisterQuote(string, []byte) (DomainRegisterQuote, error) {
	return r.verifyQuote, nil
}

func (r *fakeRuntimePorts) BuildDomainRegisterTx(context.Context, []byte, DomainRegisterQuote) (BuiltDomainRegisterTx, error) {
	return BuiltDomainRegisterTx{RawTx: []byte("tx"), TxID: "register_tx_id"}, nil
}

func (r *fakeRuntimePorts) TriggerDomainRegisterSubmit(context.Context, string, []byte) (DomainRegisterSubmitResponse, error) {
	return r.submitResp, nil
}

func (r *fakeRuntimePorts) TriggerDomainSetTarget(context.Context, string, string, string, uint64) (DomainSetTargetResponse, error) {
	return r.setTargetResp, nil
}

func (r *fakeRuntimePorts) ApplyLocalBroadcastWalletTxBytes(context.Context, []byte, string) error {
	r.applyCount++
	return nil
}

func (r *fakeRuntimePorts) RecordChainPaymentAccountingAfterBroadcast(context.Context, []byte, string, string, string, string, string) error {
	r.accountCount++
	return nil
}

func (r *fakeRuntimePorts) WalletAllocMutex() sync.Locker {
	if r.walletMu != nil {
		return r.walletMu
	}
	r.walletMu = &sync.Mutex{}
	return r.walletMu
}

func (r *fakeRuntimePorts) ResolveDomainToPubkeyDirect(context.Context, string) (string, error) {
	r.resolveCalls++
	return r.resolvePubkey, r.resolveErr
}

func (r *fakeRuntimePorts) GetFrontOrderSettlementSummary(context.Context, string) (FrontOrderSettlementSummary, error) {
	return FrontOrderSettlementSummary{FrontOrderID: "front-1"}, nil
}

func TestBizResolveNormalizesAndReturnsPubkey(t *testing.T) {
	t.Parallel()

	out, err := BizResolve(context.Background(), resolverStub{
		pubkeyHex: "021111111111111111111111111111111111111111111111111111111111111111",
	}, " Movie.David ")
	if err != nil {
		t.Fatalf("biz resolve failed: %v", err)
	}
	if out.Domain != "movie.david" {
		t.Fatalf("domain mismatch: got=%s want=movie.david", out.Domain)
	}
}

type resolverStub struct {
	pubkeyHex string
	err       error
}

func (r resolverStub) ResolveDomainToPubkeyDirect(context.Context, string) (string, error) {
	return r.pubkeyHex, r.err
}

func TestTriggerDomainRegisterNameMainFlow(t *testing.T) {
	t.Parallel()

	store := &fakeBusinessStore{}
	rt := &fakeRuntimePorts{
		clientID: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		queryResp: DomainQueryResponse{
			Success:           true,
			Available:         true,
			RegisterLockFee:   100,
			SetTargetFee:      50,
			OwnerPubkeyHex:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ExpireAtUnix:      time.Now().Add(time.Hour).Unix(),
			ChargedAmount:     10,
			LockExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
		},
		lockResp: DomainRegisterLockResponse{
			Success:           true,
			Status:            "LOCKED",
			ChargedAmount:     20,
			LockExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
			SignedQuoteJSON:   []byte("signed-quote"),
		},
		verifyQuote: DomainRegisterQuote{
			QuoteID:                  "quote-1",
			Name:                     "movie.david",
			OwnerPubkeyHex:           "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			TargetPubkeyHex:          "021111111111111111111111111111111111111111111111111111111111111111",
			PayToAddress:             "1test",
			RegisterPriceSatoshi:     1000,
			RegisterSubmitFeeSatoshi: 200,
			TotalPaySatoshi:          1200,
			LockExpiresAtUnix:        time.Now().Add(time.Hour).Unix(),
			TermSeconds:              3600,
		},
		submitResp: DomainRegisterSubmitResponse{
			Success:         true,
			Status:          "OK",
			Name:            "movie.david",
			OwnerPubkeyHex:  "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			TargetPubkeyHex: "021111111111111111111111111111111111111111111111111111111111111111",
			ExpireAtUnix:    time.Now().Add(time.Hour).Unix(),
			RegisterTxID:    "register_tx_id",
		},
	}

	out, err := TriggerDomainRegisterName(context.Background(), store, rt, TriggerDomainRegisterNameParams{
		ResolverPubkeyHex: "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Name:              " Movie.David ",
		TargetPubkeyHex:   "021111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("register name failed: %v", err)
	}
	if !out.Ok || out.Code != "OK" {
		t.Fatalf("unexpected register result: %+v", out)
	}
	if !store.createCalled {
		t.Fatal("expected business chain to be created")
	}
	if !strings.EqualFold(store.createInput.TargetObjectID, "movie.david") {
		t.Fatalf("unexpected target object id: %+v", store.createInput.TargetObjectID)
	}
	if strings.TrimSpace(store.createInput.SettlementTargetType) != "chain_tx" {
		t.Fatalf("unexpected settlement target type: %+v", store.createInput.SettlementTargetType)
	}
}

func TestFinalizeDomainRegisterSettlementUsesTxIDDirectly(t *testing.T) {
	t.Parallel()

	store := &fakeBusinessStore{}
	if err := FinalizeDomainRegisterSettlement(context.Background(), store, "set_domain_reg_123", true, "TX123", ""); err != nil {
		t.Fatalf("finalize settlement failed: %v", err)
	}
	if len(store.settlementUpdate) != 1 {
		t.Fatalf("expected one settlement update, got %d", len(store.settlementUpdate))
	}
	got := store.settlementUpdate[0]
	if got.settlementID != "set_domain_reg_123" {
		t.Fatalf("unexpected settlement id: %q", got.settlementID)
	}
	if got.status != "settled" {
		t.Fatalf("unexpected status: %q", got.status)
	}
	if got.targetType != "chain_tx" {
		t.Fatalf("unexpected target type: %q", got.targetType)
	}
	if got.targetID != "tx123" {
		t.Fatalf("unexpected target id: %q", got.targetID)
	}
}
