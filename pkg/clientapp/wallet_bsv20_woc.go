package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

// walletBSV20WOCCandidate WOC BSV20 unspent 响应项
type walletBSV20WOCCandidate struct {
	ScriptHash string `json:"scriptHash"`
	Data       struct {
		BSV20 struct {
			ID     string                `json:"id"`
			Symbol string                `json:"sym"`
			Amount walletWOCQuantityText `json:"amt"`
		} `json:"bsv20"`
	} `json:"data"`
	Current struct {
		TxID string `json:"txid"`
	} `json:"current"`
}

// walletBSV20WOCUnspentResp WOC BSV20 unspent 响应
type walletBSV20WOCUnspentResp struct {
	Tokens []walletBSV20WOCCandidate `json:"tokens"`
}

// queryWalletBSV20WOCUnspent 查询 WOC 获取地址的 BSV20 token unspent 列表
// 设计说明：
// - 复用 BSV21 查询的结构和模式，但查询不同的 WOC 端点
// - 用于资产确认流程判断 1-sat UTXO 是否为 BSV20 token
func queryWalletBSV20WOCUnspent(ctx context.Context, rt *Runtime, address string) ([]walletBSV20WOCCandidate, error) {
	if rt == nil || rt.WalletChain == nil {
		return nil, fmt.Errorf("wallet chain not initialized")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(rt.WalletChain.BaseURL()), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("wallet chain base url is empty")
	}
	
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/token/bsv20/"+url.PathEscape(strings.TrimSpace(address))+"/unspent?limit=200&filterMempool=both",
		nil,
	)
	if err != nil {
		return nil, err
	}
	
	auth := whatsonchain.AuthConfig{
		Mode:  "bearer",
		Value: strings.TrimSpace(rt.runIn.ExternalAPI.WOC.APIKey),
	}
	if strings.TrimSpace(auth.Value) == "" {
		auth.Mode = ""
	}
	if err := auth.Apply(req); err != nil {
		return nil, err
	}
	
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound {
		return []walletBSV20WOCCandidate{}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("woc bsv20 unspent http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	
	var parsed walletBSV20WOCUnspentResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	return parsed.Tokens, nil
}

// matchBSV20VoutByScriptHash 通过 scriptHash 匹配 BSV20 token 的 vout
// 设计说明：
// - WOC 返回的 token 候选包含 scriptHash（锁定脚本的 sha256）
// - 需要解析交易 hex，计算每个输出的 scriptHash，匹配到正确的 vout
func matchBSV20VoutByScriptHash(ctx context.Context, txHex string, targetScriptHash string, targetVout uint32) (bool, error) {
	targetScriptHash = strings.ToLower(strings.TrimSpace(targetScriptHash))
	if targetScriptHash == "" || txHex == "" {
		return false, nil
	}
	
	// 解析交易获取输出脚本
	scriptHexes, err := extractOutputScriptsFromTxHex(txHex)
	if err != nil {
		return false, err
	}
	
	if int(targetVout) >= len(scriptHexes) {
		return false, nil
	}
	
	// 计算目标 vout 的 scriptHash
	scriptBytes, err := hex.DecodeString(scriptHexes[targetVout])
	if err != nil {
		return false, err
	}
	
	hash := sha256.Sum256(scriptBytes)
	calculatedHash := strings.ToLower(hex.EncodeToString(hash[:]))
	
	return calculatedHash == targetScriptHash, nil
}

// matchBSV21VoutByScriptHash 复用 BSV20 的匹配逻辑
func matchBSV21VoutByScriptHash(ctx context.Context, txHex string, targetScriptHash string, targetVout uint32) (bool, error) {
	// BSV21 和 BSV20 使用相同的 scriptHash 匹配逻辑
	return matchBSV20VoutByScriptHash(ctx, txHex, targetScriptHash, targetVout)
}

// extractOutputScriptsFromTxHex 从交易 hex 中提取所有输出脚本
// 简化实现：直接解析原始交易 hex，提取 vout 的锁定脚本
func extractOutputScriptsFromTxHex(txHex string) ([]string, error) {
	// 使用 go-sdk 解析交易
	tx, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return nil, fmt.Errorf("parse tx hex: %w", err)
	}
	
	scripts := make([]string, len(tx.Outputs))
	for i, out := range tx.Outputs {
		if out != nil && out.LockingScript != nil {
			scripts[i] = strings.ToLower(hex.EncodeToString(out.LockingScript.Bytes()))
		}
	}
	return scripts, nil
}
