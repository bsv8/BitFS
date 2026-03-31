package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

const (
	walletBSV21CreateStatusPendingExternalVerification = "pending_external_verification"
	walletBSV21CreateStatusExternallyVerified          = "externally_verified"
	walletBSV21CreateAutoCheckDelay                    = 3 * time.Minute
)

type walletTokenCreateStatusRequest struct {
	TokenID string `json:"token_id"`
}

type walletBSV21CreateStatusItem struct {
	TokenID                string `json:"token_id"`
	CreateTxID             string `json:"create_txid"`
	WalletID               string `json:"wallet_id"`
	Address                string `json:"address"`
	TokenStandard          string `json:"token_standard"`
	Symbol                 string `json:"symbol"`
	MaxSupply              string `json:"max_supply"`
	Decimals               int    `json:"decimals"`
	Icon                   string `json:"icon"`
	Status                 string `json:"status"`
	CreatedAtUnix          int64  `json:"created_at_unix"`
	SubmittedAtUnix        int64  `json:"submitted_at_unix"`
	VerifiedAtUnix         int64  `json:"verified_at_unix,omitempty"`
	LastVerificationAtUnix int64  `json:"last_verification_at_unix,omitempty"`
	NextVerificationAtUnix int64  `json:"next_verification_at_unix,omitempty"`
	UpdatedAtUnix          int64  `json:"updated_at_unix"`
	LastVerificationError  string `json:"last_verification_error,omitempty"`
}

type walletBSV21CreateStatusResp struct {
	Ok   bool                        `json:"ok"`
	Item walletBSV21CreateStatusItem `json:"item"`
}

type walletBSV21CreateStatusMeta struct {
	Symbol    string
	MaxSupply string
	Decimals  int
	Icon      string
}

type walletBSV21WOCStatusTokenByIDResp struct {
	Token struct {
		Outpoint string `json:"outpoint"`
		Data     struct {
			BSV20 struct {
				ID string `json:"id"`
			} `json:"bsv20"`
		} `json:"data"`
		Current struct {
			TxID string `json:"txid"`
		} `json:"current"`
	} `json:"token"`
}

// handleWalletTokenCreateStatus 只读返回某个 create 任务的状态。
// 设计说明：
// - create 状态是独立账本，不再混进 wallet_utxo / wallet_utxo_assets；
// - 查询面必须按 token_id 精确命中，避免“顺手刷新整个系统”的副作用。
func (s *httpAPIServer) handleWalletTokenCreateStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	tokenID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("token_id")))
	if tokenID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "token_id is required"})
		return
	}
	item, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletBSV21CreateStatusItem, error) {
		return loadWalletBSV21CreateStatusByTokenID(newClientDB(db, s.dbActor), tokenID)
	})
	if err != nil {
		if err == sql.ErrNoRows {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "token create status not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, walletBSV21CreateStatusResp{
		Ok:   true,
		Item: item,
	})
}

// handleWalletTokenCreateStatusRefresh 只刷新一个指定 token 的 create 状态。
// 设计说明：
// - 手工刷新必须精确到 token，不触发整轮钱包同步；
// - 这样前端和 e2e 都能明确知道“刷新的是哪一个 create”。
func (s *httpAPIServer) handleWalletTokenCreateStatusRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req walletTokenCreateStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	item, err := refreshWalletBSV21CreateStatus(r.Context(), s.rt, req.TokenID, "http_wallet_token_create_status_refresh", false)
	if err != nil {
		if err == sql.ErrNoRows {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "token create status not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, walletBSV21CreateStatusResp{
		Ok:   true,
		Item: item,
	})
}

func normalizeWalletBSV21CreateStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case walletBSV21CreateStatusExternallyVerified:
		return walletBSV21CreateStatusExternallyVerified
	default:
		return walletBSV21CreateStatusPendingExternalVerification
	}
}

func loadWalletBSV21CreateStatusByTokenID(store *clientDB, tokenID string) (walletBSV21CreateStatusItem, error) {
	return dbLoadWalletBSV21CreateStatusByTokenID(context.Background(), store, tokenID)
}

func upsertWalletBSV21CreateStatus(store *clientDB, item walletBSV21CreateStatusItem) error {
	return dbUpsertWalletBSV21CreateStatus(context.Background(), store, item)
}

func recordWalletBSV21CreateSubmitted(ctx context.Context, rt *Runtime, item walletBSV21CreateStatusItem) error {
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	return dbUpsertWalletBSV21CreateStatus(ctx, runtimeStore(rt), item)
}

func scheduleWalletBSV21CreateAutoCheckAfterTipChange(ctx context.Context, rt *Runtime, dueAtUnix int64) error {
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if dueAtUnix <= 0 {
		dueAtUnix = time.Now().Add(walletBSV21CreateAutoCheckDelay).Unix()
	}
	return dbScheduleWalletBSV21CreateAutoCheckAfterTipChange(ctx, runtimeStore(rt), dueAtUnix)
}

func listDueWalletBSV21CreateStatuses(store *clientDB, nowUnix int64) ([]walletBSV21CreateStatusItem, error) {
	return dbListDueWalletBSV21CreateStatuses(context.Background(), store, nowUnix)
}

func refreshWalletBSV21CreateStatus(ctx context.Context, rt *Runtime, tokenID string, trigger string, clearAutoCheck bool) (walletBSV21CreateStatusItem, error) {
	if rt == nil {
		return walletBSV21CreateStatusItem{}, fmt.Errorf("runtime not initialized")
	}
	tokenID = strings.ToLower(strings.TrimSpace(tokenID))
	if tokenID == "" {
		return walletBSV21CreateStatusItem{}, fmt.Errorf("token_id is required")
	}
	item, err := dbLoadWalletBSV21CreateStatusByTokenID(ctx, runtimeStore(rt), tokenID)
	if err != nil {
		return walletBSV21CreateStatusItem{}, err
	}
	if item.Status == walletBSV21CreateStatusExternallyVerified {
		return item, nil
	}
	ready, err := queryWalletBSV21ExternalVerificationReady(ctx, rt, item.TokenID)
	nowUnix := time.Now().Unix()
	item.LastVerificationAtUnix = nowUnix
	item.UpdatedAtUnix = nowUnix
	if clearAutoCheck {
		item.NextVerificationAtUnix = 0
	}
	if err != nil {
		item.LastVerificationError = err.Error()
	} else {
		item.LastVerificationError = ""
		if ready {
			item.Status = walletBSV21CreateStatusExternallyVerified
			item.VerifiedAtUnix = nowUnix
			item.NextVerificationAtUnix = 0
		}
	}
	if saveErr := dbUpsertWalletBSV21CreateStatus(ctx, runtimeStore(rt), item); saveErr != nil {
		return walletBSV21CreateStatusItem{}, saveErr
	}
	return item, nil
}

func refreshDueWalletBSV21CreateStatuses(ctx context.Context, rt *Runtime, trigger string) error {
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	nowUnix := time.Now().Unix()
	items, err := dbListDueWalletBSV21CreateStatuses(ctx, runtimeStore(rt), nowUnix)
	if err != nil {
		return err
	}
	for _, item := range items {
		if _, err := refreshWalletBSV21CreateStatus(ctx, rt, item.TokenID, trigger, true); err != nil {
			return err
		}
	}
	return nil
}

func extractBSV21DeployMintStatusMetaFromTx(tx *txsdk.Transaction) walletBSV21CreateStatusMeta {
	meta := walletBSV21CreateStatusMeta{}
	if tx == nil || len(tx.Outputs) == 0 {
		return meta
	}
	var iconRef string
	for _, output := range tx.Outputs {
		if output == nil || output.LockingScript == nil {
			continue
		}
		payload, ok := decodeWalletTokenEnvelopePayload(output.LockingScript)
		if !ok {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "op"), "deploy+mint") {
			continue
		}
		meta.Symbol = strings.TrimSpace(firstNonEmptyStringField(payload, "sym"))
		meta.MaxSupply = strings.TrimSpace(firstNonEmptyStringField(payload, "amt"))
		iconRef = strings.TrimSpace(firstNonEmptyStringField(payload, "icon"))
		if decText := strings.TrimSpace(firstNonEmptyStringField(payload, "dec")); decText != "" {
			if parsed, err := strconv.Atoi(decText); err == nil && parsed >= 0 {
				meta.Decimals = parsed
			}
		}
		break
	}
	if iconRef == fmt.Sprintf("_%d", walletBSV21CreateIconOutputVout) && len(tx.Outputs) > int(walletBSV21CreateIconOutputVout) {
		output := tx.Outputs[walletBSV21CreateIconOutputVout]
		if output != nil && output.LockingScript != nil {
			if payload, ok := decodeWalletTokenEnvelopePayload(output.LockingScript); ok &&
				strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bitfs") &&
				strings.EqualFold(firstNonEmptyStringField(payload, "type"), "hash") {
				meta.Icon = strings.ToLower(strings.TrimSpace(firstNonEmptyStringField(payload, "hash")))
			}
		}
	}
	return meta
}

func queryWalletBSV21ExternalVerificationReady(ctx context.Context, rt *Runtime, tokenID string) (bool, error) {
	if rt == nil || rt.WalletChain == nil {
		return false, fmt.Errorf("wallet chain not initialized")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(rt.WalletChain.BaseURL()), "/")
	if baseURL == "" {
		return false, fmt.Errorf("wallet chain base url is empty")
	}
	queryURL := baseURL + "/token/bsv21/id/" + url.PathEscape(strings.ToLower(strings.TrimSpace(tokenID)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return false, err
	}
	auth := whatsonchain.AuthConfig{
		Mode:  "bearer",
		Value: strings.TrimSpace(rt.runIn.ExternalAPI.WOC.APIKey),
	}
	if strings.TrimSpace(auth.Value) == "" {
		auth.Mode = ""
	}
	if err := auth.Apply(req); err != nil {
		return false, err
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("woc bsv21 token id http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed walletBSV21WOCStatusTokenByIDResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return false, err
	}
	if strings.TrimSpace(parsed.Token.Outpoint) == "" &&
		strings.TrimSpace(parsed.Token.Data.BSV20.ID) == "" &&
		strings.TrimSpace(parsed.Token.Current.TxID) == "" {
		return false, nil
	}
	return true, nil
}
