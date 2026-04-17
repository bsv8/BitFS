package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/obs"
	te "github.com/bsv8/MultisigPool/pkg/triple_endpoint"
	"github.com/libp2p/go-libp2p/core/host"
)

// triplePoolSession 是 buyer 侧维护的“3-of-3 费用池会话”状态。
// 这里的金额变化来自真实可签名交易，不是数据库记账余额。
type triplePoolSession struct {
	DemandID         string
	SessionID        string
	DealID           string
	BusinessID       string // 正式下载 business_id（biz_download_pool_*），用于 pay 时挂 tx_breakdown
	SettlementID     string // 关联的 settlement_id，用于 pay 时回写 settlement 状态
	SellerPubHex     string
	ArbiterPubHex    string
	PoolAmountSat    uint64
	SpendTxFeeSat    uint64
	OpenSequence     uint32
	Sequence         uint32
	SellerAmount     uint64
	BuyerAmount      uint64
	CurrentTxHex     string
	BaseTxHex        string
	BaseTxID         string
	FeeRateSatByte   float64
	LockBlocks       uint32
	SellerPubKeyHex  string
	BuyerPubKeyHex   string
	ArbiterPubKeyHex string
	PayCount         uint32
	LastPaySequence  uint32
	FinalTxID        string
}

type directTransferPoolRow struct {
	SessionID        string
	DealID           string
	BuyerPubHex      string
	SellerPubHex     string
	ArbiterPubHex    string
	BuyerPubKeyHex   string
	SellerPubKeyHex  string
	ArbiterPubKeyHex string
	PoolAmount       uint64
	SpendTxFee       uint64
	SequenceNum      uint32
	SellerAmount     uint64
	BuyerAmount      uint64
	CurrentTxHex     string
	BaseTxHex        string
	BaseTxID         string
	Status           string
	FeeRateSatByte   float64
	LockBlocks       uint32
	CreatedAtUnix    int64
	UpdatedAtUnix    int64
}

func (r *Runtime) getTriplePool(sessionID string) (*triplePoolSession, bool) {
	if r == nil {
		return nil, false
	}
	key := strings.TrimSpace(sessionID)
	if key == "" {
		return nil, false
	}
	r.tripleMu.RLock()
	defer r.tripleMu.RUnlock()
	s, ok := r.triplePool[key]
	return s, ok
}

func (r *Runtime) setTriplePool(s *triplePoolSession) {
	if r == nil || s == nil {
		return
	}
	key := strings.TrimSpace(s.SessionID)
	if key == "" {
		return
	}
	r.tripleMu.Lock()
	defer r.tripleMu.Unlock()
	if r.triplePool == nil {
		r.triplePool = map[string]*triplePoolSession{}
	}
	r.triplePool[key] = s
}

func (r *Runtime) deleteTriplePool(sessionID string) {
	if r == nil {
		return
	}
	key := strings.TrimSpace(sessionID)
	if key == "" {
		return
	}
	r.tripleMu.Lock()
	defer r.tripleMu.Unlock()
	delete(r.triplePool, key)
}

func (r *Runtime) transferPoolOpenMutex() *sync.Mutex {
	if r == nil {
		return &sync.Mutex{}
	}
	return &r.transferPoolOpenMu
}

func (r *Runtime) transferPoolSessionMutex(sessionID string) *sync.Mutex {
	if r == nil {
		return &sync.Mutex{}
	}
	key := strings.TrimSpace(sessionID)
	if key == "" {
		return &r.transferPoolOpenMu
	}
	r.transferPoolSessionLocksMu.Lock()
	defer r.transferPoolSessionLocksMu.Unlock()
	if r.transferPoolSessionLocks == nil {
		r.transferPoolSessionLocks = map[string]*sync.Mutex{}
	}
	mu, ok := r.transferPoolSessionLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		r.transferPoolSessionLocks[key] = mu
	}
	return mu
}

func (r *Runtime) releaseTransferPoolSessionMutex(sessionID string) {
	if r == nil {
		return
	}
	key := strings.TrimSpace(sessionID)
	if key == "" {
		return
	}
	r.transferPoolSessionLocksMu.Lock()
	defer r.transferPoolSessionLocksMu.Unlock()
	delete(r.transferPoolSessionLocks, key)
}

func handleDirectTransferPoolOpen(h host.Host, store *clientDB, cfg Config, req directTransferPoolOpenReq) (directTransferPoolOpenResp, error) {
	sessionID := strings.TrimSpace(req.SessionId)
	dealID := strings.TrimSpace(req.DealId)
	buyerPubHex, err := normalizeCompressedPubKeyHex(req.BuyerPubkeyHex)
	if err != nil {
		return directTransferPoolOpenResp{SessionId: strings.TrimSpace(req.SessionId), Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPeerID := strings.TrimSpace(req.ArbiterPubkeyHex)
	arbiterPubHex := strings.ToLower(strings.TrimSpace(req.ArbiterPubkey))
	if sessionID == "" || dealID == "" || buyerPubHex == "" || arbiterPeerID == "" || arbiterPubHex == "" {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "invalid open request"}, nil
	}
	if req.Sequence == 0 {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "sequence must be >= 1"}, nil
	}
	if req.PoolAmount == 0 || len(req.CurrentTx) == 0 || len(req.BuyerSig) == 0 || len(req.BaseTx) == 0 {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "missing tx payload"}, nil
	}
	if req.SellerAmount+req.BuyerAmount+req.SpendTxFee != req.PoolAmount {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "amounts do not match pool"}, nil
	}

	sellerPubHex := strings.ToLower(strings.TrimSpace(localPubHex(h)))
	dealBuyerPubHex, dealSellerPubHex, dealArbiterPubHex, err := dbLoadDirectDealParties(context.Background(), store, dealID)
	if err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "deal not found"}, nil
	}
	if buyerPubHex != strings.ToLower(strings.TrimSpace(dealBuyerPubHex)) {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "buyer mismatch"}, nil
	}
	if sellerPubHex == "" || sellerPubHex != strings.ToLower(strings.TrimSpace(dealSellerPubHex)) {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "seller mismatch"}, nil
	}
	if arbiterPeerID != strings.TrimSpace(dealArbiterPubHex) {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "arbiter mismatch"}, nil
	}

	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(buyerPubHex)
	if err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "invalid buyer secp256k1 pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(arbiterPubHex)
	if err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "invalid arbiter secp256k1 pubkey"}, nil
	}
	currentTxHex := strings.ToLower(hex.EncodeToString(req.CurrentTx))
	baseTxHex := strings.ToLower(hex.EncodeToString(req.BaseTx))
	parsedTx, err := te.TripleFeePoolLoadTx(
		currentTxHex,
		nil,
		req.Sequence,
		req.SellerAmount,
		arbiterPub,
		buyerPub,
		sellerPriv.PubKey(),
		req.PoolAmount,
	)
	if err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	buyerSig := append([]byte(nil), req.BuyerSig...)
	ok, err := te.ServerVerifyClientASig(parsedTx, req.PoolAmount, arbiterPub, buyerPub, sellerPriv.PubKey(), &buyerSig)
	if err != nil || !ok {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "buyer signature verify failed"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}

	if err := dbUpsertDirectTransferPoolOpen(context.Background(), store, req, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, currentTxHex, baseTxHex); err != nil {
		return directTransferPoolOpenResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	obs.Business("bitcast-client", "direct_transfer_pool_open_ok", map[string]any{
		"session_id":    sessionID,
		"deal_id":       dealID,
		"pool_amount":   req.PoolAmount,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
	})
	return directTransferPoolOpenResp{
		SessionId: sessionID,
		Status:    "active",
		SellerSig: append([]byte(nil), (*sellerSig)...),
	}, nil
}

func handleDirectTransferChunkGet(_ host.Host, store *clientDB, _ Config, req directTransferChunkGetReq) (directTransferChunkGetResp, error) {
	sessionID := strings.TrimSpace(req.SessionId)
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	chunkHash := strings.ToLower(strings.TrimSpace(req.ChunkHash))
	if sessionID == "" || seedHash == "" || chunkHash == "" {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "invalid chunk_get request"}, nil
	}
	row, err := dbLoadDirectTransferPoolRow(context.Background(), store, sessionID)
	if err != nil {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool not found"}, nil
	}
	status := strings.ToLower(strings.TrimSpace(row.Status))
	if status == "expired" || status == "closed" || status == "arbitrated" || status == "arbitration_pending" || status == "arbitration_rejected" {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool status invalid"}, nil
	}
	dealSeedHash, err := dbLoadDirectDealSeedHash(context.Background(), store, row.DealID)
	if err != nil {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "deal not found"}, nil
	}
	if seedHash != strings.ToLower(strings.TrimSpace(dealSeedHash)) {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "seed hash mismatch"}, nil
	}
	seedBytes, err := dbLoadSeedBytesBySeedHash(context.Background(), store, seedHash)
	if err != nil {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	resolvedChunkIndex, err := resolveChunkIndexByHashInSeed(seedBytes, chunkHash)
	if err != nil {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	if req.ChunkIndex != resolvedChunkIndex {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "chunk index mismatch"}, nil
	}
	chunk, err := dbLoadChunkBytesBySeedHash(context.Background(), store, seedHash, resolvedChunkIndex)
	if err != nil {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	sum := sha256.Sum256(chunk)
	if hex.EncodeToString(sum[:]) != chunkHash {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: "chunk hash verify failed"}, nil
	}
	if err := dbUpdateDirectTransferPoolStatus(context.Background(), store, sessionID, "delivering"); err != nil {
		return directTransferChunkGetResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	obs.Business("bitcast-client", "direct_transfer_chunk_get_ok", map[string]any{
		"session_id":  sessionID,
		"chunk_hash":  chunkHash,
		"chunk_index": resolvedChunkIndex,
		"sequence":    req.Sequence,
	})
	return directTransferChunkGetResp{
		SessionId: sessionID,
		Status:    "delivering",
		Chunk:     append([]byte(nil), chunk...),
	}, nil
}

func handleDirectTransferPoolPay(rt *Runtime, _ host.Host, store *clientDB, cfg Config, req directTransferPoolPayReq) (directTransferPoolPayResp, error) {
	sessionID := strings.TrimSpace(req.SessionId)
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	chunkHash := strings.ToLower(strings.TrimSpace(req.ChunkHash))
	if sessionID == "" || seedHash == "" || chunkHash == "" || req.Sequence == 0 || len(req.CurrentTx) == 0 || len(req.BuyerSig) == 0 {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "invalid pay request"}, nil
	}
	row, err := dbLoadDirectTransferPoolRow(context.Background(), store, sessionID)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool not found"}, nil
	}
	status := strings.ToLower(strings.TrimSpace(row.Status))
	if status == "expired" || status == "closed" || status == "arbitrated" || status == "arbitration_pending" || status == "arbitration_rejected" {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool status invalid"}, nil
	}
	if req.Sequence <= row.SequenceNum {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "sequence must increase"}, nil
	}
	if req.SellerAmount <= row.SellerAmount {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "seller amount must increase"}, nil
	}
	dealSeedHash, err := dbLoadDirectDealSeedHash(context.Background(), store, row.DealID)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "deal not found"}, nil
	}
	if seedHash != strings.ToLower(strings.TrimSpace(dealSeedHash)) {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "seed hash mismatch"}, nil
	}
	seedBytes, err := dbLoadSeedBytesBySeedHash(context.Background(), store, seedHash)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	resolvedChunkIndex, err := resolveChunkIndexByHashInSeed(seedBytes, chunkHash)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	if req.ChunkIndex != resolvedChunkIndex {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "chunk index mismatch"}, nil
	}
	if rt.directTransferTestOptions().ForceRejectPay {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "simulated pay reject"}, nil
	}

	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(row.BuyerPubKeyHex)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(row.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "invalid arbiter pubkey"}, nil
	}
	currentTxHex := strings.ToLower(hex.EncodeToString(req.CurrentTx))
	parsedTx, err := te.TripleFeePoolLoadTx(
		currentTxHex,
		nil,
		req.Sequence,
		req.SellerAmount,
		arbiterPub,
		buyerPub,
		sellerPriv.PubKey(),
		row.PoolAmount,
	)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	buyerSig := append([]byte(nil), req.BuyerSig...)
	ok, err := te.ServerVerifyClientASig(parsedTx, row.PoolAmount, arbiterPub, buyerPub, sellerPriv.PubKey(), &buyerSig)
	if err != nil || !ok {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "buyer signature verify failed"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}

	delta := req.SellerAmount - row.SellerAmount
	if err := dbUpdateDirectTransferPoolPay(context.Background(), store, sessionID, req.Sequence, req.SellerAmount, req.BuyerAmount, currentTxHex, delta, "paid"); err != nil {
		return directTransferPoolPayResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	obs.Business("bitcast-client", "direct_transfer_pool_pay_ok", map[string]any{
		"session_id":    sessionID,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
		"delta":         delta,
		"chunk_hash":    chunkHash,
		"chunk_index":   resolvedChunkIndex,
	})
	return directTransferPoolPayResp{
		SessionId: sessionID,
		Status:    "active",
		SellerSig: append([]byte(nil), (*sellerSig)...),
	}, nil
}

func handleDirectTransferArbitrate(ctx context.Context, rt *Runtime, h host.Host, store *clientDB, cfg Config, req directTransferArbitrateReq) (directTransferArbitrateResp, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID := strings.TrimSpace(req.SessionId)
	if sessionID == "" {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "session_id required"}, nil
	}
	row, err := dbLoadDirectTransferPoolRow(ctx, store, sessionID)
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool not found"}, nil
	}
	status := strings.ToLower(strings.TrimSpace(row.Status))
	if status == "closed" || status == "expired" || status == "arbitrated" {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool status invalid"}, nil
	}
	if strings.ToLower(strings.TrimSpace(req.SellerPubkeyHex)) != strings.ToLower(strings.TrimSpace(row.SellerPubHex)) {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "seller mismatch"}, nil
	}
	if strings.ToLower(strings.TrimSpace(req.BuyerPubkeyHex)) != strings.ToLower(strings.TrimSpace(row.BuyerPubHex)) {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "buyer mismatch"}, nil
	}
	expectedArbiterPID, err := peerIDFromClientID(strings.TrimSpace(row.ArbiterPubHex))
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid arbiter in session"}, nil
	}
	if strings.TrimSpace(req.ArbiterPubkeyHex) != expectedArbiterPID.String() {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "arbiter mismatch"}, nil
	}
	if row.SellerAmount+req.ArbiterFee+row.SpendTxFee > row.PoolAmount {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "pool_insufficient", Error: "pool_insufficient"}, nil
	}
	currentTxBytes, err := hex.DecodeString(strings.TrimSpace(row.CurrentTxHex))
	if err != nil || len(currentTxBytes) == 0 {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid current tx in session"}, nil
	}

	evidenceKey := fmt.Sprintf("%s:%d:%s:%d", sessionID, req.ChunkIndex, strings.ToLower(strings.TrimSpace(req.ChunkHash)), req.Sequence)
	arbiterPID := expectedArbiterPID
	evidenceRaw := append([]byte(nil), req.EvidencePayload...)
	if len(evidenceRaw) == 0 {
		evidenceRaw, _ = json.Marshal(map[string]any{
			"demand_id":   strings.TrimSpace(req.DemandId),
			"session_id":  sessionID,
			"seed_hash":   strings.ToLower(strings.TrimSpace(req.SeedHash)),
			"chunk_index": req.ChunkIndex,
			"chunk_hash":  strings.ToLower(strings.TrimSpace(req.ChunkHash)),
			"sequence":    req.Sequence,
		})
	}
	evHash := sha256.Sum256(evidenceRaw)
	arbReq := directTransferArbitrateReq{
		DemandId:         strings.TrimSpace(req.DemandId),
		SessionId:        sessionID,
		SeedHash:         strings.ToLower(strings.TrimSpace(req.SeedHash)),
		ChunkIndex:       req.ChunkIndex,
		ChunkHash:        strings.ToLower(strings.TrimSpace(req.ChunkHash)),
		Sequence:         req.Sequence,
		SellerPubkeyHex:  strings.ToLower(strings.TrimSpace(req.SellerPubkeyHex)),
		BuyerPubkeyHex:   strings.ToLower(strings.TrimSpace(req.BuyerPubkeyHex)),
		ArbiterPubkeyHex: strings.ToLower(strings.TrimSpace(req.ArbiterPubkeyHex)),
		SellerAmount:     row.SellerAmount,
		BuyerAmount:      row.BuyerAmount,
		ArbiterFee:       req.ArbiterFee,
		SpendTxFee:       row.SpendTxFee,
		CurrentTx:        append([]byte(nil), currentTxBytes...),
		EvidenceHash:     evHash[:],
		EvidencePayload:  append([]byte(nil), evidenceRaw...),
	}
	var arbResp directTransferArbitrateResp
	if err := pproto.CallProto(
		ctx,
		h,
		arbiterPID,
		ProtoTransferArbitrate,
		arbSec(nil),
		arbReq,
		&arbResp,
	); err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	if strings.TrimSpace(arbResp.Status) == "fee_rejected" {
		_ = dbUpsertDirectArbitrationState(ctx, store, sessionID, evidenceKey, "arbitration_rejected", "")
		emitDirectTransferEvent(rt, "direct_transfer_arbitration_fee_rejected", map[string]any{
			"event_id":            fmt.Sprintf("%s:arb:%d", sessionID, req.Sequence),
			"demand_id":           strings.TrimSpace(req.DemandId),
			"deal_id":             strings.TrimSpace(row.DealID),
			"session_id":          sessionID,
			"seller_pubkey_hex":   strings.TrimSpace(row.SellerPubHex),
			"buyer_pubkey_hex":    strings.TrimSpace(row.BuyerPubHex),
			"arbiter_pubkey_hex":  strings.TrimSpace(row.ArbiterPubHex),
			"sequence":            req.Sequence,
			"arbiter_fee_satoshi": arbResp.ArbiterFee,
		})
		return directTransferArbitrateResp{
			SessionId:  sessionID,
			Status:     "fee_rejected",
			Error:      "fee_rejected",
			ArbiterFee: arbResp.ArbiterFee,
		}, nil
	}
	if strings.TrimSpace(arbResp.Status) != "arbitrated" {
		return directTransferArbitrateResp{SessionId: sessionID, Status: strings.TrimSpace(arbResp.Status), Error: strings.TrimSpace(arbResp.Error)}, nil
	}
	if rt.directTransferTestOptions().ForceRejectArbFee {
		_ = dbUpsertDirectArbitrationState(ctx, store, sessionID, evidenceKey, "arbitration_rejected", "")
		emitDirectTransferEvent(rt, "direct_transfer_arbitration_fee_rejected", map[string]any{
			"event_id":            fmt.Sprintf("%s:arb:%d", sessionID, req.Sequence),
			"demand_id":           strings.TrimSpace(req.DemandId),
			"deal_id":             strings.TrimSpace(row.DealID),
			"session_id":          sessionID,
			"seller_pubkey_hex":   strings.TrimSpace(row.SellerPubHex),
			"buyer_pubkey_hex":    strings.TrimSpace(row.BuyerPubHex),
			"arbiter_pubkey_hex":  strings.TrimSpace(row.ArbiterPubHex),
			"sequence":            req.Sequence,
			"arbiter_fee_satoshi": arbResp.ArbiterFee,
		})
		return directTransferArbitrateResp{
			SessionId:  sessionID,
			Status:     "fee_rejected",
			Error:      "fee_rejected",
			ArbiterFee: arbResp.ArbiterFee,
		}, nil
	}
	if rt == nil || rt.ActionChain == nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "runtime not initialized"}, nil
	}
	if len(arbResp.AwardTx) == 0 || len(arbResp.ArbiterSig) == 0 {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid arbitration award payload"}, nil
	}
	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(row.BuyerPubKeyHex)
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(row.ArbiterPubKeyHex)
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid arbiter pubkey"}, nil
	}
	awardTxHex := strings.ToLower(hex.EncodeToString(arbResp.AwardTx))
	awardTxObj, err := tx.NewTransactionFromHex(awardTxHex)
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "invalid award tx"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(awardTxObj, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}
	arbiterSig := append([]byte(nil), arbResp.ArbiterSig...)
	merged, err := te.MergeTripleFeePoolSigForSpendTx(awardTxObj.Hex(), &arbiterSig, sellerSig)
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: "merge arbitration signatures failed"}, nil
	}
	finalTxID, err := rt.ActionChain.Broadcast(merged.Hex())
	if err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: fmt.Sprintf("broadcast arbitration award failed: %v", err)}, nil
	}
	if err := applyLocalBroadcastWalletTx(ctx, store, rt, merged.Hex(), "direct_transfer_pool_arbitration_award"); err != nil {
		obs.Error("bitcast-client", "direct_transfer_arbitration_wallet_projection_failed", map[string]any{
			"session_id": sessionID,
			"error":      err.Error(),
		})
	}
	sequenceNum := req.Sequence
	if len(merged.Inputs) > 0 && merged.Inputs[0].SequenceNumber != 0 {
		sequenceNum = merged.Inputs[0].SequenceNumber
	}
	sellerAmount := req.SellerAmount
	buyerAmount := req.BuyerAmount
	if len(merged.Outputs) >= 2 {
		sellerAmount = merged.Outputs[0].Satoshis
		buyerAmount = merged.Outputs[1].Satoshis
	}
	if err := dbApplyDirectTransferPoolArbitrated(ctx, store, sessionID, sequenceNum, sellerAmount, buyerAmount, merged.Hex()); err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	if err := dbUpsertDirectArbitrationState(ctx, store, sessionID, evidenceKey, "arbitrated", strings.TrimSpace(finalTxID)); err != nil {
		return directTransferArbitrateResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	emitDirectTransferEvent(rt, "direct_transfer_arbitration_award_broadcasted", map[string]any{
		"event_id":             fmt.Sprintf("%s:arb:%d", sessionID, req.Sequence),
		"demand_id":            strings.TrimSpace(req.DemandId),
		"deal_id":              strings.TrimSpace(row.DealID),
		"session_id":           sessionID,
		"seller_pubkey_hex":    strings.TrimSpace(row.SellerPubHex),
		"buyer_pubkey_hex":     strings.TrimSpace(row.BuyerPubHex),
		"arbiter_pubkey_hex":   strings.TrimSpace(row.ArbiterPubHex),
		"sequence":             sequenceNum,
		"seller_amount_sat":    sellerAmount,
		"buyer_amount_sat":     buyerAmount,
		"arbiter_fee_satoshi":  arbResp.ArbiterFee,
		"award_broadcast_txid": strings.TrimSpace(finalTxID),
	})
	mergedTxBytes, _ := hex.DecodeString(merged.Hex())
	return directTransferArbitrateResp{
		SessionId:  sessionID,
		Status:     "arbitrated",
		AwardTxid:  strings.TrimSpace(finalTxID),
		AwardTx:    mergedTxBytes,
		ArbiterSig: arbiterSig,
		ArbiterFee: arbResp.ArbiterFee,
	}, nil
}

func resolveChunkIndexByHashInSeed(seed []byte, chunkHash string) (uint32, error) {
	if len(seed) < 22 {
		return 0, fmt.Errorf("invalid seed")
	}
	if string(seed[:4]) != "BSE1" {
		return 0, fmt.Errorf("invalid seed")
	}
	chunkCount := binary.BigEndian.Uint32(seed[18:22])
	expect := 22 + int(chunkCount)*32
	if len(seed) != expect {
		return 0, fmt.Errorf("invalid seed")
	}
	offset := 22
	for i := uint32(0); i < chunkCount; i++ {
		if hex.EncodeToString(seed[offset:offset+32]) == chunkHash {
			return i, nil
		}
		offset += 32
	}
	return 0, fmt.Errorf("chunk hash not found")
}

func handleDirectTransferPoolClose(_ host.Host, store *clientDB, cfg Config, req directTransferPoolCloseReq) (directTransferPoolCloseResp, error) {
	sessionID := strings.TrimSpace(req.SessionId)
	if sessionID == "" || len(req.CurrentTx) == 0 || len(req.BuyerSig) == 0 {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "invalid close request"}, nil
	}
	row, err := dbLoadDirectTransferPoolRow(context.Background(), store, sessionID)
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool not found"}, nil
	}
	// 关单要支持丢包重试：第一次成功后库里会落成 closed，第二次还要能按同一请求重放。
	rowStatus := strings.ToLower(strings.TrimSpace(row.Status))
	if rowStatus != "active" && rowStatus != "delivering" && rowStatus != "paid" && rowStatus != "arbitrated" && rowStatus != "closing" && rowStatus != "closed" {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool status invalid"}, nil
	}
	if req.SellerAmount != row.SellerAmount {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "seller amount mismatch"}, nil
	}

	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(row.BuyerPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(row.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "invalid arbiter pubkey"}, nil
	}
	currentTxHex := strings.ToLower(hex.EncodeToString(req.CurrentTx))
	rawTx, err := tx.NewTransactionFromHex(currentTxHex)
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	seq := req.Sequence
	if len(rawTx.Inputs) > 0 && rawTx.Inputs[0].SequenceNumber != 0 {
		seq = rawTx.Inputs[0].SequenceNumber
	}
	if rowStatus == "closed" {
		if strings.TrimSpace(row.CurrentTxHex) != currentTxHex || row.SequenceNum != req.Sequence || row.BuyerAmount != req.BuyerAmount {
			return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "transfer pool status invalid"}, nil
		}
	}
	locktime := rawTx.LockTime
	parsedTx, err := te.TripleFeePoolLoadTx(
		currentTxHex,
		&locktime,
		seq,
		req.SellerAmount,
		arbiterPub,
		buyerPub,
		sellerPriv.PubKey(),
		row.PoolAmount,
	)
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	buyerSig := append([]byte(nil), req.BuyerSig...)
	ok, err := te.ServerVerifyClientASig(parsedTx, row.PoolAmount, arbiterPub, buyerPub, sellerPriv.PubKey(), &buyerSig)
	if err != nil || !ok {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "buyer signature verify failed"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}
	if err := dbUpdateDirectTransferPoolClosing(context.Background(), store, sessionID, req.Sequence, req.SellerAmount, req.BuyerAmount, currentTxHex); err != nil {
		return directTransferPoolCloseResp{SessionId: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	obs.Business("bitcast-client", "direct_transfer_pool_close_sign_ok", map[string]any{
		"session_id": sessionID,
		"sequence":   req.Sequence,
	})
	return directTransferPoolCloseResp{
		SessionId: sessionID,
		Status:    "closed",
		SellerSig: append([]byte(nil), (*sellerSig)...),
	}, nil

}
