package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
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
	sessionID := strings.TrimSpace(req.SessionID)
	dealID := strings.TrimSpace(req.DealID)
	buyerPubHex, err := normalizeCompressedPubKeyHex(req.BuyerPeerID)
	if err != nil {
		return directTransferPoolOpenResp{SessionID: strings.TrimSpace(req.SessionID), Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPeerID := strings.TrimSpace(req.ArbiterPeerID)
	arbiterPubHex := strings.ToLower(strings.TrimSpace(req.ArbiterPubKey))
	if sessionID == "" || dealID == "" || buyerPubHex == "" || arbiterPeerID == "" || arbiterPubHex == "" {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "invalid open request"}, nil
	}
	if req.Sequence == 0 {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "sequence must be >= 1"}, nil
	}
	if req.PoolAmount == 0 || len(req.CurrentTx) == 0 || len(req.BuyerSig) == 0 || len(req.BaseTx) == 0 {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "missing tx payload"}, nil
	}
	if req.SellerAmount+req.BuyerAmount+req.SpendTxFee != req.PoolAmount {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "amounts do not match pool"}, nil
	}

	sellerPubHex := strings.ToLower(strings.TrimSpace(localPubHex(h)))
	dealBuyerPubHex, dealSellerPubHex, dealArbiterPubHex, err := dbLoadDirectDealParties(context.Background(), store, dealID)
	if err != nil {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "deal not found"}, nil
	}
	if buyerPubHex != strings.ToLower(strings.TrimSpace(dealBuyerPubHex)) {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "buyer mismatch"}, nil
	}
	if sellerPubHex == "" || sellerPubHex != strings.ToLower(strings.TrimSpace(dealSellerPubHex)) {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "seller mismatch"}, nil
	}
	if arbiterPeerID != strings.TrimSpace(dealArbiterPubHex) {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "arbiter mismatch"}, nil
	}

	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(buyerPubHex)
	if err != nil {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "invalid buyer secp256k1 pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(arbiterPubHex)
	if err != nil {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "invalid arbiter secp256k1 pubkey"}, nil
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
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	buyerSig := append([]byte(nil), req.BuyerSig...)
	ok, err := te.ServerVerifyClientASig(parsedTx, req.PoolAmount, arbiterPub, buyerPub, sellerPriv.PubKey(), &buyerSig)
	if err != nil || !ok {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "buyer signature verify failed"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}

	if err := dbUpsertDirectTransferPoolOpen(context.Background(), store, req, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, currentTxHex, baseTxHex); err != nil {
		return directTransferPoolOpenResp{SessionID: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	obs.Business("bitcast-client", "direct_transfer_pool_open_ok", map[string]any{
		"session_id":    sessionID,
		"deal_id":       dealID,
		"pool_amount":   req.PoolAmount,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
	})
	return directTransferPoolOpenResp{
		SessionID: sessionID,
		Status:    "active",
		SellerSig: append([]byte(nil), (*sellerSig)...),
	}, nil
}

func handleDirectTransferPoolPay(_ host.Host, store *clientDB, cfg Config, req directTransferPoolPayReq) (directTransferPoolPayResp, error) {
	sessionID := strings.TrimSpace(req.SessionID)
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	chunkHash := strings.ToLower(strings.TrimSpace(req.ChunkHash))
	if sessionID == "" || seedHash == "" || chunkHash == "" || req.Sequence == 0 || len(req.CurrentTx) == 0 || len(req.BuyerSig) == 0 {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "invalid pay request"}, nil
	}
	row, err := dbLoadDirectTransferPoolRow(context.Background(), store, sessionID)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "transfer pool not found"}, nil
	}
	if row.Status != "active" {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "transfer pool not active"}, nil
	}
	if req.Sequence <= row.SequenceNum {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "sequence must increase"}, nil
	}
	if req.SellerAmount <= row.SellerAmount {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "seller amount must increase"}, nil
	}
	dealSeedHash, err := dbLoadDirectDealSeedHash(context.Background(), store, row.DealID)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "deal not found"}, nil
	}
	if seedHash != strings.ToLower(strings.TrimSpace(dealSeedHash)) {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "seed hash mismatch"}, nil
	}
	seedBytes, err := dbLoadSeedBytesBySeedHash(context.Background(), store, seedHash)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	resolvedChunkIndex, err := resolveChunkIndexByHashInSeed(seedBytes, chunkHash)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	if req.ChunkIndex != resolvedChunkIndex {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "chunk index mismatch"}, nil
	}
	chunk, err := dbLoadChunkBytesBySeedHash(context.Background(), store, seedHash, resolvedChunkIndex)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	sum := sha256.Sum256(chunk)
	if hex.EncodeToString(sum[:]) != chunkHash {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "chunk hash verify failed"}, nil
	}

	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(row.BuyerPubKeyHex)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(row.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "invalid arbiter pubkey"}, nil
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
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	buyerSig := append([]byte(nil), req.BuyerSig...)
	ok, err := te.ServerVerifyClientASig(parsedTx, row.PoolAmount, arbiterPub, buyerPub, sellerPriv.PubKey(), &buyerSig)
	if err != nil || !ok {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "buyer signature verify failed"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}

	delta := req.SellerAmount - row.SellerAmount
	if err := dbUpdateDirectTransferPoolPay(context.Background(), store, sessionID, req.Sequence, req.SellerAmount, req.BuyerAmount, currentTxHex, delta); err != nil {
		return directTransferPoolPayResp{SessionID: sessionID, Status: "rejected", Error: err.Error()}, nil
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
		SessionID: sessionID,
		Status:    "active",
		SellerSig: append([]byte(nil), (*sellerSig)...),
		Chunk:     append([]byte(nil), chunk...),
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

func shortHash(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}

func handleDirectTransferPoolClose(_ host.Host, store *clientDB, cfg Config, req directTransferPoolCloseReq) (directTransferPoolCloseResp, error) {
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" || len(req.CurrentTx) == 0 || len(req.BuyerSig) == 0 {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "invalid close request"}, nil
	}
	row, err := dbLoadDirectTransferPoolRow(context.Background(), store, sessionID)
	if err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "transfer pool not found"}, nil
	}
	if row.Status != "active" && row.Status != "closing" {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "transfer pool status invalid"}, nil
	}
	if req.SellerAmount != row.SellerAmount {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "seller amount mismatch"}, nil
	}

	sellerPriv, err := ec.PrivateKeyFromHex(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	if err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "invalid seller key"}, nil
	}
	buyerPub, err := ec.PublicKeyFromString(row.BuyerPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "invalid buyer pubkey"}, nil
	}
	arbiterPub, err := ec.PublicKeyFromString(row.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "invalid arbiter pubkey"}, nil
	}
	currentTxHex := strings.ToLower(hex.EncodeToString(req.CurrentTx))
	rawTx, err := tx.NewTransactionFromHex(currentTxHex)
	if err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	seq := req.Sequence
	if len(rawTx.Inputs) > 0 && rawTx.Inputs[0].SequenceNumber != 0 {
		seq = rawTx.Inputs[0].SequenceNumber
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
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "invalid current tx"}, nil
	}
	buyerSig := append([]byte(nil), req.BuyerSig...)
	ok, err := te.ServerVerifyClientASig(parsedTx, row.PoolAmount, arbiterPub, buyerPub, sellerPriv.PubKey(), &buyerSig)
	if err != nil || !ok {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "buyer signature verify failed"}, nil
	}
	sellerSig, err := te.ClientBTripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPub, buyerPub, sellerPriv)
	if err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: "seller sign failed"}, nil
	}
	if err := dbUpdateDirectTransferPoolClosing(context.Background(), store, sessionID, req.Sequence, req.SellerAmount, req.BuyerAmount, currentTxHex); err != nil {
		return directTransferPoolCloseResp{SessionID: sessionID, Status: "rejected", Error: err.Error()}, nil
	}
	obs.Business("bitcast-client", "direct_transfer_pool_close_sign_ok", map[string]any{
		"session_id": sessionID,
		"sequence":   req.Sequence,
	})
	return directTransferPoolCloseResp{
		SessionID: sessionID,
		Status:    "closing",
		SellerSig: append([]byte(nil), (*sellerSig)...),
	}, nil

}
