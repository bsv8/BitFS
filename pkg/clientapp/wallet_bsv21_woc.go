package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

type walletBSV21WOCCandidate struct {
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

type walletBSV21WOCUnspentResp struct {
	Tokens []walletBSV21WOCCandidate `json:"tokens"`
}

type walletUTXOBasicRow struct {
	UTXOID           string
	TxID             string
	Vout             uint32
	ValueSatoshi     uint64
	ScriptType       string
	ScriptTypeReason string
	AllocationClass  string
	AllocationReason string
	CreatedAtUnix    int64
}

type walletWOCQuantityText string

// loadWalletBSV21VerifiedIncomingCandidates 只补充“不是本地自广播事实”的外来 token 候选。
// 设计说明：
// - 这里表达的是“外来资产验真”边界，而不是把 WOC 本身写成业务真相；
// - 当前系统只有一个外来验真渠道，所以实现上仍然查询 WOC；
// - 一旦将来验真渠道变化，调用方仍然保持“拿已验真的外来候选”这个领域语义。

func (q *walletWOCQuantityText) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	if value == "" || value == "null" {
		*q = ""
		return nil
	}
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		*q = walletWOCQuantityText(strings.TrimSpace(text))
		return nil
	}
	*q = walletWOCQuantityText(value)
	return nil
}

func (q walletWOCQuantityText) String() string {
	return strings.TrimSpace(string(q))
}

func queryWalletBSV21WOCUnspent(ctx context.Context, rt transferRuntimeCaps, address string) ([]walletBSV21WOCCandidate, error) {
	if rt == nil || rt.WalletChainClient() == nil {
		return nil, fmt.Errorf("wallet chain not initialized")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(rt.WalletChainClient().BaseURL()), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("wallet chain base url is empty")
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/token/bsv21/"+url.PathEscape(strings.TrimSpace(address))+"/unspent?limit=200&filterMempool=both",
		nil,
	)
	if err != nil {
		return nil, err
	}
	cfg := rt.ConfigSnapshot()
	auth := whatsonchain.AuthConfig{
		Mode:  "bearer",
		Value: strings.TrimSpace(cfg.ExternalAPI.WOC.APIKey),
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
		return []walletBSV21WOCCandidate{}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("woc bsv21 unspent http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed walletBSV21WOCUnspentResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	return parsed.Tokens, nil
}

func (s runtimeWalletScriptEvidenceSource) GetAddressBSV21TokenUnspent(ctx context.Context, address string) ([]walletBSV21WOCCandidate, error) {
	if s.rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return queryWalletBSV21WOCUnspent(ctx, s.rt, address)
}

// loadWalletBSV21LocalCandidates 只认“当前钱包自己广播出来、且目前仍未花费”的 token 输出。
// 设计说明：
// - create / send 不能再把 WOC 当作业务前提；
// - 因此本地自己构造并成功广播的 token 输出，应该直接进入可继续 send 的候选集；
// - 第三方打进来的 token 不走这里，而是留给外来资产验真边界单独补充。
