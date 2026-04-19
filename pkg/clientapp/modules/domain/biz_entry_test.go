package domain

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type fakeBusinessStore struct {
	createInput      CreateBusinessWithFrontTriggerAndPendingSettlementInput
	createCalled     bool
	settlementByTxID map[string]int64
	settlementUpdate []struct {
		settlementID string
		status       string
		targetType   string
		targetID     string
		errMsg       string
	}
}

type resolverStub struct {
	pubkeyHex string
	err       error
}

func (r resolverStub) ResolveDomainToPubkey(ctx context.Context, domain string) (string, error) {
	return r.pubkeyHex, r.err
}

func (s *fakeBusinessStore) CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, input CreateBusinessWithFrontTriggerAndPendingSettlementInput) error {
	s.createCalled = true
	s.createInput = input
	return nil
}

func (s *fakeBusinessStore) UpsertFrontOrder(ctx context.Context, entry FrontOrderEntry) error {
	return nil
}

func (s *fakeBusinessStore) UpsertBusiness(ctx context.Context, entry BusinessEntry) error {
	return nil
}

func (s *fakeBusinessStore) UpsertBusinessSettlement(ctx context.Context, entry BusinessSettlementEntry) error {
	return nil
}

func (s *fakeBusinessStore) AppendBusinessTrigger(ctx context.Context, entry BusinessTriggerEntry) error {
	return nil
}

func (s *fakeBusinessStore) GetChainPaymentByTxID(ctx context.Context, txID string) (int64, error) {
	if s.settlementByTxID != nil {
		if id, ok := s.settlementByTxID[txID]; ok {
			return id, nil
		}
	}
	return 0, sql.ErrNoRows
}

func (s *fakeBusinessStore) UpdateOrderSettlement(ctx context.Context, settlementID, status, targetType, targetID, errMsg string, updatedAtUnix int64) error {
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

type fakeRuntimePorts struct {
	clientID       string
	resolvePubkey  string
	resolvePeerID  peer.ID
	queryResp      DomainQueryResponse
	lockResp       DomainRegisterLockResponse
	submitResp     DomainRegisterSubmitResponse
	setTargetResp  DomainSetTargetResponse
	verifyQuote    DomainRegisterQuote
	lastRegisterTx []byte
	lastTxID       string
	lastApplyTag   string
	applyCount     int
	accountCount   int
	walletMu       *sync.Mutex
}

func (r *fakeRuntimePorts) ClientID(ctx context.Context) (string, error) {
	return r.clientID, nil
}

func (r *fakeRuntimePorts) EnsureDomainPeerConnected(ctx context.Context, resolverPubkeyHex string, resolverAddr string) (string, peer.ID, error) {
	if strings.TrimSpace(r.resolvePubkey) != "" {
		return r.resolvePubkey, r.resolvePeerID, nil
	}
	return strings.ToLower(strings.TrimSpace(resolverPubkeyHex)), peer.ID("peer-1"), nil
}

func (r *fakeRuntimePorts) TriggerDomainQueryName(ctx context.Context, resolverPubkeyHex string, resolverPeerID peer.ID, name string) (DomainQueryResponse, error) {
	return r.queryResp, nil
}

func (r *fakeRuntimePorts) TriggerDomainRegisterLock(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, registerLockFee uint64) (DomainRegisterLockResponse, error) {
	return r.lockResp, nil
}

func (r *fakeRuntimePorts) VerifyRegisterQuote(resolverPubkeyHex string, raw []byte) (DomainRegisterQuote, error) {
	return r.verifyQuote, nil
}

func (r *fakeRuntimePorts) BuildDomainRegisterTx(ctx context.Context, signedQuoteJSON []byte, quote DomainRegisterQuote) (BuiltDomainRegisterTx, error) {
	return BuiltDomainRegisterTx{
		RawTx:           []byte("register-tx"),
		TxID:            "register_tx_id",
		MinerFeeSatoshi: 123,
		ChangeSatoshi:   456,
	}, nil
}

func (r *fakeRuntimePorts) TriggerDomainRegisterSubmit(ctx context.Context, resolverPubkeyHex string, registerTx []byte) (DomainRegisterSubmitResponse, error) {
	r.lastRegisterTx = append([]byte(nil), registerTx...)
	return r.submitResp, nil
}

func (r *fakeRuntimePorts) TriggerDomainSetTarget(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, setTargetFee uint64) (DomainSetTargetResponse, error) {
	return r.setTargetResp, nil
}

func (r *fakeRuntimePorts) ApplyLocalBroadcastWalletTxBytes(ctx context.Context, rawTx []byte, trigger string) error {
	r.applyCount++
	r.lastApplyTag = trigger
	r.lastRegisterTx = append([]byte(nil), rawTx...)
	return nil
}

func (r *fakeRuntimePorts) RecordChainPaymentAccountingAfterBroadcast(ctx context.Context, rawTx []byte, txID string, accountingScene string, accountingSubType string, fromPartyID string, toPartyID string) error {
	r.accountCount++
	r.lastTxID = txID
	return nil
}

func (r *fakeRuntimePorts) WalletAllocMutex() sync.Locker {
	if r.walletMu != nil {
		return r.walletMu
	}
	r.walletMu = &sync.Mutex{}
	return r.walletMu
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
	if out.PubkeyHex != "021111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("pubkey mismatch: got=%s", out.PubkeyHex)
	}
}

func TestBizResolveRejectsNilResolver(t *testing.T) {
	t.Parallel()

	if _, err := BizResolve(context.Background(), nil, "movie.david"); CodeOf(err) != CodeModuleDisabled {
		t.Fatalf("expected module disabled, got=%v code=%s", err, CodeOf(err))
	}
}

func TestTriggerDomainRegisterNameMainFlow(t *testing.T) {
	t.Parallel()

	store := &fakeBusinessStore{settlementByTxID: map[string]int64{
		"register_tx_id": 42,
	}}
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
	if store.createInput.TargetObjectID != "movie.david" {
		t.Fatalf("unexpected target object id: %+v", store.createInput.TargetObjectID)
	}
	if out.RegisterTxID != "register_tx_id" {
		t.Fatalf("unexpected register txid: %+v", out)
	}
	if rt.applyCount != 1 || rt.accountCount != 1 {
		t.Fatalf("submit should broadcast and account once, got apply=%d account=%d", rt.applyCount, rt.accountCount)
	}
}

func TestTriggerDomainRegisterLockAndSetTarget(t *testing.T) {
	t.Parallel()

	store := &fakeBusinessStore{}
	rt := &fakeRuntimePorts{
		clientID:      "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		resolvePubkey: "03resolver",
		queryResp: DomainQueryResponse{
			Success:           true,
			Available:         true,
			Registered:        true,
			Locked:            false,
			RegisterLockFee:   100,
			SetTargetFee:      66,
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
		setTargetResp: DomainSetTargetResponse{
			Success:         true,
			Status:          "OK",
			ChargedAmount:   66,
			Name:            "movie.david",
			OwnerPubkeyHex:  "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			TargetPubkeyHex: "021111111111111111111111111111111111111111111111111111111111111111",
			ExpireAtUnix:    time.Now().Add(time.Hour).Unix(),
		},
	}

	lockOut, err := TriggerDomainRegisterLock(context.Background(), store, rt, TriggerDomainRegisterLockParams{
		ResolverPubkeyHex: "03resolver",
		Name:              " Movie.David ",
		TargetPubkeyHex:   "021111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("register lock failed: %v", err)
	}
	if !lockOut.Ok || lockOut.Code != "LOCKED" {
		t.Fatalf("unexpected lock result: %+v", lockOut)
	}
	if lockOut.Name != "movie.david" {
		t.Fatalf("name should be normalized: %+v", lockOut)
	}

	setTargetOut, err := TriggerDomainSetTarget(context.Background(), store, rt, TriggerDomainSetTargetParams{
		ResolverPubkeyHex: "03resolver",
		Name:              " Movie.David ",
		TargetPubkeyHex:   "021111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("set target failed: %v", err)
	}
	if !setTargetOut.Ok || setTargetOut.Code != "OK" {
		t.Fatalf("unexpected set target result: %+v", setTargetOut)
	}
	if setTargetOut.TargetPubkeyHex != "021111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("unexpected target pubkey: %+v", setTargetOut)
	}
}

func TestTriggerDomainSubmitPreparedRegisterBroadcastsAndAccounts(t *testing.T) {
	t.Parallel()

	rt := &fakeRuntimePorts{
		clientID: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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

	out, err := TriggerDomainSubmitPreparedRegister(context.Background(), nil, rt, TriggerDomainSubmitPreparedRegisterParams{
		ResolverPubkeyHex: "03resolver",
		RegisterTxHex:     "010203",
	})
	if err != nil {
		t.Fatalf("submit prepared register failed: %v", err)
	}
	if !out.Ok || out.Code != "OK" {
		t.Fatalf("unexpected submit result: %+v", out)
	}
	if rt.applyCount != 1 || rt.accountCount != 1 {
		t.Fatalf("broadcast/account should run once, got apply=%d account=%d", rt.applyCount, rt.accountCount)
	}
}

func TestFinalizeDomainRegisterSettlement(t *testing.T) {
	t.Parallel()

	store := &fakeBusinessStore{settlementByTxID: map[string]int64{
		"tx_success": 42,
	}}
	if err := FinalizeDomainRegisterSettlement(context.Background(), store, "set_1", true, "tx_success", ""); err != nil {
		t.Fatalf("finalize success failed: %v", err)
	}
	if len(store.settlementUpdate) != 1 {
		t.Fatalf("expected one settlement update, got %d", len(store.settlementUpdate))
	}
	if store.settlementUpdate[0].status != "settled" || store.settlementUpdate[0].targetID != "42" {
		t.Fatalf("unexpected settlement update: %+v", store.settlementUpdate[0])
	}
	if err := FinalizeDomainRegisterSettlement(context.Background(), store, "set_2", false, "", "payment timeout"); err != nil {
		t.Fatalf("finalize failed settlement failed: %v", err)
	}
	if len(store.settlementUpdate) != 2 || store.settlementUpdate[1].status != "failed" {
		t.Fatalf("unexpected failed settlement update: %+v", store.settlementUpdate[1])
	}
}

func TestDomainErrorCodeMapping(t *testing.T) {
	t.Parallel()

	err := NewError(CodeBadRequest, "test error")
	if CodeOf(err) != CodeBadRequest {
		t.Fatalf("unexpected code: %s", CodeOf(err))
	}
	if MessageOf(err) != "test error" {
		t.Fatalf("unexpected message: %s", MessageOf(err))
	}
	if CodeOf(moduleDisabledErr()) != CodeModuleDisabled {
		t.Fatalf("unexpected module disabled code: %s", CodeOf(moduleDisabledErr()))
	}
}

func TestDomainRegisterSubmitHelpers(t *testing.T) {
	t.Parallel()

	prepared := TriggerDomainPrepareRegisterResult{
		Ok:                         true,
		Code:                       "PREPARED",
		Name:                       "example.eth",
		OwnerPubkeyHex:             "owner",
		TargetPubkeyHex:            "target",
		RegisterTxID:               "prepared_txid",
		QueryFeeChargedSatoshi:     1,
		RegisterLockChargedSatoshi: 1,
		RegisterPriceSatoshi:       1,
		RegisterSubmitFeeSatoshi:   1,
		TotalRegisterPaySatoshi:    2,
	}
	base := DomainRegisterNameResultFromPrepared(prepared)
	if base.Name != "example.eth" || base.RegisterTxID != "prepared_txid" {
		t.Fatalf("prepared result not copied correctly: %+v", base)
	}
	next := ApplyDomainRegisterSubmitResult(base, TriggerDomainSubmitPreparedRegisterResult{
		Ok:              true,
		Name:            "example.eth",
		OwnerPubkeyHex:  "owner_after",
		TargetPubkeyHex: "target_after",
		ExpireAtUnix:    123,
		RegisterTxID:    "final_txid",
	})
	if !next.Ok || next.Code != "OK" {
		t.Fatalf("submit success not applied: %+v", next)
	}
	if next.OwnerPubkeyHex != "owner_after" || next.TargetPubkeyHex != "target_after" {
		t.Fatalf("submit fields mismatch: %+v", next)
	}
	if strings.TrimSpace(next.RegisterTxID) != "final_txid" {
		t.Fatalf("submit txid mismatch: %+v", next)
	}
}

func TestDomainQuoteVersionMatchesModuleSpec(t *testing.T) {
	t.Parallel()

	if DomainQuoteVersion != "bsv8-domain-register-quote-v1" {
		t.Fatalf("quote version mismatch: got=%s", DomainQuoteVersion)
	}
}

func TestTriggerDomainRegisterNameRejectsNilRuntime(t *testing.T) {
	t.Parallel()

	_, err := TriggerDomainRegisterName(context.Background(), &fakeBusinessStore{}, nil, TriggerDomainRegisterNameParams{
		ResolverPubkeyHex: "03resolver",
		Name:              "movie.david",
		TargetPubkeyHex:   "021111111111111111111111111111111111111111111111111111111111111111",
	})
	if err == nil || CodeOf(err) != CodeModuleDisabled {
		t.Fatalf("expected module disabled, got=%v code=%s", err, CodeOf(err))
	}
}

func TestTriggerDomainRegisterLockRejectsEmptyTarget(t *testing.T) {
	t.Parallel()

	_, err := TriggerDomainRegisterLock(context.Background(), &fakeBusinessStore{}, &fakeRuntimePorts{
		clientID: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		queryResp: DomainQueryResponse{
			Success:   true,
			Available: true,
		},
	}, TriggerDomainRegisterLockParams{
		ResolverPubkeyHex: "03resolver",
		Name:              "movie.david",
		TargetPubkeyHex:   "",
	})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request, got=%v code=%s", err, CodeOf(err))
	}
}

func TestTriggerDomainSubmitPreparedRegisterRejectsBadHex(t *testing.T) {
	t.Parallel()

	_, err := TriggerDomainSubmitPreparedRegister(context.Background(), &fakeBusinessStore{}, &fakeRuntimePorts{
		clientID: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, TriggerDomainSubmitPreparedRegisterParams{
		ResolverPubkeyHex: "03resolver",
		RegisterTxHex:     "zz",
	})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request, got=%v code=%s", err, CodeOf(err))
	}
}

func TestFinalizeDomainRegisterSettlementRejectsMissingTxForSuccess(t *testing.T) {
	t.Parallel()

	err := FinalizeDomainRegisterSettlement(context.Background(), &fakeBusinessStore{}, "set_1", true, "", "")
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request, got=%v code=%s", err, CodeOf(err))
	}
}

func TestTriggerDomainRegisterNameStopsOnLockFailure(t *testing.T) {
	t.Parallel()

	store := &fakeBusinessStore{}
	rt := &fakeRuntimePorts{
		clientID: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		queryResp: DomainQueryResponse{
			Success:   true,
			Available: true,
		},
		lockResp: DomainRegisterLockResponse{
			Success:         false,
			Status:          "LOCK_FAILED",
			Error:           "denied",
			SignedQuoteJSON: []byte("signed-quote"),
		},
	}
	out, err := TriggerDomainRegisterName(context.Background(), store, rt, TriggerDomainRegisterNameParams{
		ResolverPubkeyHex: "03resolver",
		Name:              "movie.david",
		TargetPubkeyHex:   "021111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("main flow should not fail when lock is denied: %v", err)
	}
	if out.Ok || out.Code != "LOCK_FAILED" {
		t.Fatalf("unexpected lock failure result: %+v", out)
	}
	if !store.createCalled {
		t.Fatalf("expected business chain to be created before lock failure")
	}
	if rt.applyCount != 0 || rt.accountCount != 0 {
		t.Fatalf("unexpected submit when lock denied")
	}
}
