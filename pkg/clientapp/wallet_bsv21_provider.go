package clientapp

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/chainbridge"
)

type walletBSV21Provider interface {
	GetTokenDetails(ctx context.Context, tokenID string) (walletBSV21TokenDetails, error)
	ValidateOutputs(ctx context.Context, tokenID string, outpoints []string) ([]walletBSV21IndexedOutput, error)
	SubmitTx(ctx context.Context, tokenID string, tx *txsdk.Transaction) error
}

type walletBSV21TokenDetails struct {
	Token  walletBSV21TokenData   `json:"token"`
	Status walletBSV21TokenStatus `json:"status"`
}

type walletBSV21TokenData struct {
	Symbol string `json:"sym"`
	Icon   string `json:"icon"`
	Dec    int    `json:"dec"`
	Amount string `json:"amt"`
}

type walletBSV21TokenStatus struct {
	IsActive     bool   `json:"is_active"`
	FeePerOutput uint64 `json:"fee_per_output"`
	FeeAddress   string `json:"fee_address"`
}

type walletBSV21IndexedOutput struct {
	Outpoint string  `json:"outpoint"`
	Score    float64 `json:"score,omitempty"`
}

type externalBSV21Provider struct {
	baseURL     string
	auth        chainbridge.AuthConfig
	client      *http.Client
	minInterval time.Duration

	mu            sync.Mutex
	nextRequestAt time.Time
}

func buildWalletBSV21Provider(in RunInput) (walletBSV21Provider, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(in.ExternalAPI.AssetIndex.BaseURL), "/")
	if baseURL == "" {
		return nil, nil
	}
	auth := chainbridge.AuthConfig{
		Mode:  strings.TrimSpace(in.ExternalAPI.AssetIndex.AuthMode),
		Name:  strings.TrimSpace(in.ExternalAPI.AssetIndex.AuthName),
		Value: strings.TrimSpace(in.ExternalAPI.AssetIndex.AuthValue),
	}
	if err := auth.Validate(); err != nil {
		return nil, fmt.Errorf("bsv21 provider auth invalid: %w", err)
	}
	return &externalBSV21Provider{
		baseURL: baseURL,
		auth:    auth,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		minInterval: time.Duration(in.ExternalAPI.AssetIndex.MinIntervalMS) * time.Millisecond,
	}, nil
}

func resolveWalletBSV21Provider(rt *Runtime) walletBSV21Provider {
	if rt == nil {
		return nil
	}
	if rt.WalletBSV21Provider != nil {
		return rt.WalletBSV21Provider
	}
	return rt.runIn.WalletBSV21Provider
}

func (p *externalBSV21Provider) GetTokenDetails(ctx context.Context, tokenID string) (walletBSV21TokenDetails, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return walletBSV21TokenDetails{}, fmt.Errorf("bsv21 token id is required")
	}
	req, err := p.newJSONRequest(ctx, http.MethodGet, "/bsv21/"+url.PathEscape(tokenID), nil)
	if err != nil {
		return walletBSV21TokenDetails{}, err
	}
	var resp walletBSV21TokenDetails
	if err := p.doJSON(req, &resp); err != nil {
		return walletBSV21TokenDetails{}, err
	}
	return resp, nil
}

func (p *externalBSV21Provider) ValidateOutputs(ctx context.Context, tokenID string, outpoints []string) ([]walletBSV21IndexedOutput, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return nil, fmt.Errorf("bsv21 token id is required")
	}
	body, err := json.Marshal(outpoints)
	if err != nil {
		return nil, err
	}
	req, err := p.newJSONRequest(ctx, http.MethodPost, "/bsv21/"+url.PathEscape(tokenID)+"/outputs?unspent=true", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	var resp []walletBSV21IndexedOutput
	if err := p.doJSON(req, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *externalBSV21Provider) SubmitTx(ctx context.Context, tokenID string, tx *txsdk.Transaction) error {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return fmt.Errorf("bsv21 token id is required")
	}
	if tx == nil {
		return fmt.Errorf("bsv21 tx is nil")
	}
	beef, err := tx.AtomicBEEF(true)
	if err != nil {
		return fmt.Errorf("build overlay beef failed: %w", err)
	}
	topics, err := json.Marshal([]string{"tm_" + tokenID})
	if err != nil {
		return err
	}
	if err := p.waitTurn(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/overlay/submit", bytes.NewReader(beef))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Topics", string(topics))
	if err := p.auth.Apply(req); err != nil {
		return err
	}
	if err := p.doNoContent(req); err == nil {
		return nil
	}

	fallbackBody, err := json.Marshal(map[string]any{
		"beef":   hex.EncodeToString(beef),
		"topics": []string{"tm_" + tokenID},
	})
	if err != nil {
		return err
	}
	fallbackReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/overlay/submit", bytes.NewReader(fallbackBody))
	if err != nil {
		return err
	}
	fallbackReq.Header.Set("Content-Type", "application/json")
	if err := p.auth.Apply(fallbackReq); err != nil {
		return err
	}
	if err := p.doNoContent(fallbackReq); err != nil {
		return fmt.Errorf("overlay submit failed: %w", err)
	}
	return nil
}

func (p *externalBSV21Provider) newJSONRequest(ctx context.Context, method string, path string, body io.Reader) (*http.Request, error) {
	if p == nil {
		return nil, fmt.Errorf("bsv21 provider is nil")
	}
	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if err := p.auth.Apply(req); err != nil {
		return nil, err
	}
	return req, nil
}

func (p *externalBSV21Provider) doJSON(req *http.Request, out any) error {
	if p == nil {
		return fmt.Errorf("bsv21 provider is nil")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("bsv21 provider http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (p *externalBSV21Provider) doNoContent(req *http.Request) error {
	if p == nil {
		return fmt.Errorf("bsv21 provider is nil")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (p *externalBSV21Provider) waitTurn(ctx context.Context) error {
	if p == nil || p.minInterval <= 0 {
		return nil
	}
	p.mu.Lock()
	readyAt := p.nextRequestAt
	now := time.Now()
	if readyAt.Before(now) {
		readyAt = now
	}
	p.nextRequestAt = readyAt.Add(p.minInterval)
	p.mu.Unlock()
	wait := time.Until(readyAt)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
