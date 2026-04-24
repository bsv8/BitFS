package chainbridge

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type rawTxSubmitClient interface {
	BaseURL() string
	PostTxRaw(ctx context.Context, txHex string) (string, error)
}

type rawTxClient struct {
	baseURL    string
	submitPath string
	auth       AuthConfig
	http       *http.Client
}

func newRawTxClient(baseURL, submitPath string, auth AuthConfig) *rawTxClient {
	return &rawTxClient{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		submitPath: normalizeSubmitPath(submitPath),
		auth:       auth,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *rawTxClient) BaseURL() string {
	if c == nil {
		return ""
	}
	return c.baseURL
}

func (c *rawTxClient) PostTxRaw(ctx context.Context, txHex string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("client is nil")
	}
	txHex = strings.TrimSpace(txHex)
	if txHex == "" {
		return "", fmt.Errorf("tx_hex is required")
	}
	raw, err := hex.DecodeString(txHex)
	if err != nil {
		return "", fmt.Errorf("decode tx_hex: %w", err)
	}
	req, err := http.NewRequestWithContext(ctxOrBackground(ctx), http.MethodPost, c.baseURL+c.submitPath, bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if err := c.auth.Apply(req); err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return parseRawTxID(body)
}

func normalizeSubmitPath(path string) string {
	p := strings.TrimSpace(path)
	if p == "" {
		return "/v1/tx"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

func parseRawTxID(body []byte) (string, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "", fmt.Errorf("broadcast response missing txid")
	}
	var txid string
	if err := json.Unmarshal(body, &txid); err == nil {
		if normalized := normalizeRawHexID(txid); normalized != "" {
			return normalized, nil
		}
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err == nil {
		for _, key := range []string{"txid", "txId", "hash", "data", "result"} {
			if v, ok := obj[key].(string); ok {
				if normalized := normalizeRawHexID(v); normalized != "" {
					return normalized, nil
				}
			}
		}
	}
	if normalized := normalizeRawHexID(trimmed); normalized != "" {
		return normalized, nil
	}
	return "", fmt.Errorf("unexpected broadcast response: %s", trimmed)
}

func normalizeRawHexID(in string) string {
	v := strings.ToLower(strings.TrimSpace(in))
	if len(v) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(v); err != nil {
		return ""
	}
	return v
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
