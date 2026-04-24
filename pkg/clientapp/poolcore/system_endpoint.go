package poolcore

import (
	"fmt"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	"github.com/bsv8/MultisigPool/pkg/libs"
)

// SystemEndpoint 表示“网关/系统端”对费用池协议的处理器。
// 关键点：Open/Close 会真实广播上链（WOC）。
type SystemEndpoint struct {
	Name     string
	Chain    ChainClient
	FeeRate  float64
	Sessions map[string]*Session // key=spend_txid
}

func NewSystemEndpoint(name string, chain ChainClient, feeRate float64) *SystemEndpoint {
	return &SystemEndpoint{Name: name, Chain: chain, FeeRate: feeRate, Sessions: map[string]*Session{}}
}

func (s *SystemEndpoint) OpenCreate(client *Actor, server *Actor, req *OpenCreateRequest, inputAmount uint64) (*OpenCreateResponse, error) {
	obs.Business("bitcast-gateway", "msg_listen_open_create_request", map[string]any{
		"req":          req,
		"input_amount": inputAmount,
	})
	spendTx, err := tx.NewTransactionFromHex(req.SpendTxHex)
	if err != nil {
		return nil, fmt.Errorf("解析 spend tx 失败: %w", err)
	}
	clientSig := req.ClientSigBytes
	ok, err := ce.ServerVerifyClientSpendSig(spendTx, inputAmount, server.PubKey, client.PubKey, &clientSig)
	if err != nil {
		return nil, fmt.Errorf("验证 client spend 签名失败: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("client spend 签名无效")
	}
	serverSig, err := ce.SpendTXServerSign(spendTx, inputAmount, server.PrivKey, client.PubKey)
	if err != nil {
		return nil, fmt.Errorf("server 回签失败: %w", err)
	}
	spendTxID := spendTx.TxID().String()
	spendTxFee := CalcFeeWithInputAmount(spendTx, inputAmount)
	poolAmount := inputAmount
	if req.SequenceNumber == 0 {
		return nil, fmt.Errorf("initial sequence 必须 >= 1")
	}
	if req.ServerAmount+spendTxFee > poolAmount {
		return nil, fmt.Errorf("初始金额非法: server_amount %d + fee %d > pool_amount %d", req.ServerAmount, spendTxFee, poolAmount)
	}
	s.Sessions[spendTxID] = &Session{
		Client:       client,
		Server:       server,
		InputAmount:  inputAmount,
		PoolAmount:   poolAmount,
		SpendTxFee:   spendTxFee,
		Sequence:     req.SequenceNumber,
		ServerAmount: req.ServerAmount,
		ClientAmount: poolAmount - req.ServerAmount - spendTxFee,
		CurrentTxHex: req.SpendTxHex,
		SpendTxID:    spendTxID,
		Status:       "pending_base_tx",
	}
	resp := &OpenCreateResponse{SpendTxID: spendTxID, ServerSigBytes: *serverSig}
	obs.Business("bitcast-gateway", "msg_listen_open_create_response", map[string]any{
		"resp": resp,
	})
	return resp, nil
}

func (s *SystemEndpoint) OpenBaseTx(req *OpenBaseTxRequest) (string, error) {
	obs.Business("bitcast-gateway", "msg_listen_open_base_tx_request", map[string]any{
		"req": req,
	})
	session, ok := s.Sessions[req.SpendTxID]
	if !ok {
		return "", fmt.Errorf("session not found by spend_txid")
	}
	baseTx, err := tx.NewTransactionFromHex(req.BaseTxHex)
	if err != nil {
		return "", fmt.Errorf("解析 base tx 失败: %w", err)
	}
	multisigScript, err := libs.Lock([]*ec.PublicKey{session.Server.PubKey, session.Client.PubKey}, 2)
	if err != nil {
		return "", fmt.Errorf("构建 multisig 脚本失败: %w", err)
	}
	if len(baseTx.Outputs) == 0 {
		return "", fmt.Errorf("base tx 没有输出")
	}
	if baseTx.Outputs[0].LockingScript.String() != multisigScript.String() {
		return "", fmt.Errorf("base tx output[0] locking script 不匹配")
	}
	baseTxID, err := s.Chain.Broadcast(req.BaseTxHex)
	if err != nil {
		return "", fmt.Errorf("广播 base tx 失败: %w", err)
	}
	obs.Business("bitcast-gateway", "evt_listen_open_base_tx_broadcasted", map[string]any{
		"spend_txid": req.SpendTxID,
		"base_txid":  baseTxID,
	})
	session.BaseTxHex = req.BaseTxHex
	session.BaseTxID = baseTxID
	session.Status = "active"
	obs.Business("bitcast-gateway", "msg_listen_open_base_tx_response", map[string]any{
		"base_txid": baseTxID,
	})
	return baseTxID, nil
}

func (s *SystemEndpoint) PayConfirm(req *PayConfirmRequest) (string, error) {
	obs.Business("bitcast-gateway", "msg_listen_pay_confirm_request", map[string]any{
		"req": req,
	})
	session, ok := s.Sessions[req.SpendTxID]
	if !ok {
		return "", fmt.Errorf("session not found by spend_txid")
	}
	if session.Status != "active" {
		return "", fmt.Errorf("session status %s does not allow pay", session.Status)
	}
	if req.SequenceNumber <= session.Sequence {
		return "", fmt.Errorf("sequence 必须递增, got %d current %d", req.SequenceNumber, session.Sequence)
	}
	if req.ServerAmount <= session.ServerAmount {
		return "", fmt.Errorf("server_amount 必须递增, got %d current %d", req.ServerAmount, session.ServerAmount)
	}
	if req.Fee != session.SpendTxFee {
		return "", fmt.Errorf("fee 不匹配, got %d expect %d", req.Fee, session.SpendTxFee)
	}
	updatedTx, err := ce.LoadTx(
		session.CurrentTxHex,
		nil,
		req.SequenceNumber,
		req.ServerAmount,
		session.Server.PubKey,
		session.Client.PubKey,
		session.PoolAmount,
	)
	if err != nil {
		return "", fmt.Errorf("重建 updated tx 失败: %w", err)
	}
	clientSig := req.ClientSigBytes
	ok, err = ce.ServerVerifyClientUpdateSig(updatedTx, session.Server.PubKey, session.Client.PubKey, &clientSig)
	if err != nil {
		return "", fmt.Errorf("验证 client update 签名失败: %w", err)
	}
	if !ok {
		return "", fmt.Errorf("client update 签名无效")
	}
	serverSig, err := ce.SpendTXServerSign(updatedTx, session.PoolAmount, session.Server.PrivKey, session.Client.PubKey)
	if err != nil {
		return "", fmt.Errorf("server 回签 update 失败: %w", err)
	}
	mergedTx, err := ce.MergeDualPoolSigForSpendTx(updatedTx.Hex(), serverSig, &clientSig)
	if err != nil {
		return "", fmt.Errorf("合并签名失败: %w", err)
	}
	session.CurrentTxHex = mergedTx.Hex()
	session.Sequence = req.SequenceNumber
	session.ServerAmount = req.ServerAmount
	session.ClientAmount = session.PoolAmount - req.ServerAmount - req.Fee
	updatedTxID := mergedTx.TxID().String()
	obs.Business("bitcast-gateway", "msg_listen_pay_confirm_response", map[string]any{
		"updated_txid":  updatedTxID,
		"sequence":      session.Sequence,
		"server_amount": session.ServerAmount,
		"client_amount": session.ClientAmount,
	})
	return updatedTxID, nil
}

func (s *SystemEndpoint) Close(req *CloseRequest) (*CloseResponse, error) {
	obs.Business("bitcast-gateway", "msg_listen_close_request", map[string]any{
		"req": req,
	})
	session, ok := s.Sessions[req.SpendTxID]
	if !ok {
		return nil, fmt.Errorf("session not found by spend_txid")
	}
	if req.ServerAmount != session.ServerAmount {
		return nil, fmt.Errorf("close server_amount 不匹配, got %d expect %d", req.ServerAmount, session.ServerAmount)
	}
	if req.Fee != session.SpendTxFee {
		return nil, fmt.Errorf("close fee 不匹配, got %d expect %d", req.Fee, session.SpendTxFee)
	}
	finalLockTime := uint32(0xffffffff)
	finalSequence := uint32(0xffffffff)
	finalTx, err := ce.LoadTx(
		session.CurrentTxHex,
		&finalLockTime,
		finalSequence,
		req.ServerAmount,
		session.Server.PubKey,
		session.Client.PubKey,
		session.PoolAmount,
	)
	if err != nil {
		return nil, fmt.Errorf("重建 final tx 失败: %w", err)
	}
	clientSig := req.ClientSigBytes
	ok, err = ce.ServerVerifyClientSpendSig(finalTx, session.PoolAmount, session.Server.PubKey, session.Client.PubKey, &clientSig)
	if err != nil {
		return nil, fmt.Errorf("验证 client final 签名失败: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("client final 签名无效")
	}
	serverSig, err := ce.SpendTXServerSign(finalTx, session.PoolAmount, session.Server.PrivKey, session.Client.PubKey)
	if err != nil {
		return nil, fmt.Errorf("server 回签 final 失败: %w", err)
	}
	mergedTx, err := ce.MergeDualPoolSigForSpendTx(finalTx.Hex(), serverSig, &clientSig)
	if err != nil {
		return nil, fmt.Errorf("合并 final 签名失败: %w", err)
	}
	finalTxID, err := s.Chain.Broadcast(mergedTx.Hex())
	if err != nil {
		return nil, fmt.Errorf("广播 final tx 失败: %w", err)
	}
	obs.Business("bitcast-gateway", "evt_listen_close_broadcasted", map[string]any{
		"spend_txid": req.SpendTxID,
		"final_txid": finalTxID,
	})
	session.FinalTxID = finalTxID
	session.Status = "closed"
	resp := &CloseResponse{FinalTxID: finalTxID, ServerSigBytes: *serverSig}
	obs.Business("bitcast-gateway", "msg_listen_close_response", map[string]any{
		"resp": resp,
	})
	return resp, nil
}
