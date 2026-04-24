package pproto

import (
	"encoding/json"
	"strings"
	"time"
)

// TraceSink 用于记录 pproto 层的“收发报文”审计日志。
// 注意：这是可选能力；正常程序运行默认不启用（Trace=nil 时不记录）。
type TraceSink interface {
	Handle(ev TraceEvent)
}

type TraceEvent struct {
	TS        string `json:"ts"`
	Direction string `json:"direction"` // send / recv

	Domain  string `json:"domain"`
	Network string `json:"network"`
	ProtoID string `json:"proto_id"`
	MsgID   string `json:"msg_id"`

	LocalPeerID  string `json:"local_peer_id,omitempty"`
	RemotePeerID string `json:"remote_peer_id,omitempty"`

	SenderPubkeyHex string `json:"sender_pubkey_hex,omitempty"`
	ExpireAt        int64  `json:"expire_at,omitempty"`

	ReplayHit bool `json:"replay_hit,omitempty"`

	Request  any    `json:"request,omitempty"`
	Response any    `json:"response,omitempty"`
	Error    string `json:"error,omitempty"`

	DurationMS int64 `json:"duration_ms,omitempty"`

	// 以下字段用于 raw 抓包索引：
	// - sha256 / bytes / blob 是落盘后的稳定引用；
	// - *Wire 仅在进程内 sink 里使用，不会进入 JSON。
	RequestEnvelopeSHA256 string `json:"request_envelope_sha256,omitempty"`
	RequestEnvelopeBytes  int    `json:"request_envelope_bytes,omitempty"`
	RequestEnvelopeBlob   string `json:"request_envelope_blob,omitempty"`

	RequestPayloadSHA256 string `json:"request_payload_sha256,omitempty"`
	RequestPayloadBytes  int    `json:"request_payload_bytes,omitempty"`
	RequestPayloadBlob   string `json:"request_payload_blob,omitempty"`

	ResponsePayloadSHA256 string `json:"response_payload_sha256,omitempty"`
	ResponsePayloadBytes  int    `json:"response_payload_bytes,omitempty"`
	ResponsePayloadBlob   string `json:"response_payload_blob,omitempty"`

	RequestEnvelopeWire []byte `json:"-"`
	RequestPayloadWire  []byte `json:"-"`
	ResponsePayloadWire []byte `json:"-"`
}

type NormalizeOptions struct {
	MaxStringLen int
}

func normalizeOptionsOrDefault(opt NormalizeOptions) NormalizeOptions {
	if opt.MaxStringLen <= 0 {
		opt.MaxStringLen = 32
	}
	return opt
}

func NormalizeJSONForTrace(b []byte, opt NormalizeOptions) any {
	opt = normalizeOptionsOrDefault(opt)
	var v any
	if len(b) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, &v); err != nil {
		// 解析失败时仍保留原始内容（但按字符串缩短）
		return shortenString(string(b), opt.MaxStringLen)
	}
	return normalizeValue(v, opt, "")
}

func normalizeValue(v any, opt NormalizeOptions, keyHint string) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, vv := range x {
			if isSensitiveKey(k) {
				out[k] = "<redacted>"
				continue
			}
			out[k] = normalizeValue(vv, opt, k)
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, it := range x {
			out = append(out, normalizeValue(it, opt, keyHint))
		}
		return out
	case string:
		if isSensitiveKey(keyHint) {
			return "<redacted>"
		}
		return shortenString(x, opt.MaxStringLen)
	default:
		return v
	}
}

func shortenString(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	if len(s) <= 8 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}

func isSensitiveKey(k string) bool {
	k = strings.ToLower(strings.TrimSpace(k))
	if k == "" {
		return false
	}
	// 这些字段一律脱敏，避免真实链上资金凭据或控制权进入日志。
	// 注意：不要把业务字段（如 seed_hash/seed_price/release_token 等）误判为敏感字段。
	if strings.Contains(k, "privkey") || strings.Contains(k, "private_key") || strings.Contains(k, "privatekey") {
		return true
	}
	if strings.Contains(k, "mnemonic") {
		return true
	}
	if strings.Contains(k, "xprv") {
		return true
	}
	if strings.Contains(k, "password") || strings.Contains(k, "passphrase") {
		return true
	}
	if strings.Contains(k, "secret") {
		return true
	}
	// 更精确的 token：只脱敏 auth_token（不要把 release_token 之类也抹掉）
	if k == "auth_token" || strings.HasSuffix(k, "_auth_token") {
		return true
	}
	return false
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }
