package pproto

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	bftpv1 "github.com/bsv8/BFTP-contract/gen/go/v1"
	"github.com/bsv8/BFTP-contract/pkg/v1/envelope"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	coreprotocol "github.com/libp2p/go-libp2p/core/protocol"
)

type ErrorResponsePB struct {
	// IsError 作为错误响应标记位，避免普通响应字段误判为错误。
	IsError bool   `protobuf:"varint,1,opt,name=is_error,json=isError,proto3" json:"is_error,omitempty"`
	Error   string `protobuf:"bytes,127,opt,name=error,proto3" json:"error,omitempty"`
}

func (m *ErrorResponsePB) Reset()         { *m = ErrorResponsePB{} }
func (m *ErrorResponsePB) String() string { return oldproto.CompactTextString(m) }
func (*ErrorResponsePB) ProtoMessage()    {}

// legacyErrorResponsePB 兼容旧版错误响应（field #1 直接承载 error 字符串）。
type legacyErrorResponsePB struct {
	Error string `protobuf:"bytes,1,opt,name=error,proto3" json:"error,omitempty"`
}

func (m *legacyErrorResponsePB) Reset()         { *m = legacyErrorResponsePB{} }
func (m *legacyErrorResponsePB) String() string { return oldproto.CompactTextString(m) }
func (*legacyErrorResponsePB) ProtoMessage()    {}

// HandleProto 使用 protobuf 二进制编解码处理 p2p RPC。
// 说明：TReq/TResp 需要在对应包内实现 protobuf Message（手写 tag + ProtoMessage）。
func HandleProto[TReq any, TResp any](h host.Host, protoID coreprotocol.ID, cfg SecurityConfig, fn func(context.Context, TReq) (TResp, error)) {
	h.SetStreamHandler(protoID, func(s network.Stream) {
		defer s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		localPID := ""
		if h != nil {
			localPID = h.ID().String()
		}
		remotePID := ""
		if s != nil && s.Conn() != nil {
			remotePID = s.Conn().RemotePeer().String()
		}

		body, err := io.ReadAll(bufio.NewReader(s))
		if err != nil {
			respBody := writeErrProto(s, "decode envelope: "+err.Error())
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					Error:               "decode envelope: " + err.Error(),
					RequestEnvelopeWire: body,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		var env bftpv1.SignedEnvelopePB
		if err := oldproto.Unmarshal(body, &env); err != nil {
			respBody := writeErrProto(s, "decode envelope: "+err.Error())
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					Error:               "decode envelope: " + err.Error(),
					RequestEnvelopeWire: body,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		if err := envelope.VerifyEnvelopePB(&env, time.Now()); err != nil {
			respBody := writeErrProto(s, err.Error())
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					ExpireAt:            env.ExpireAtUnix,
					Request:             normalizeProtoTracePayload(env.Payload),
					Error:               err.Error(),
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		if env.Domain != cfg.Domain || env.Network != cfg.Network {
			respBody := writeErrProto(s, "domain/network mismatch")
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					ExpireAt:            env.ExpireAtUnix,
					Request:             normalizeProtoTracePayload(env.Payload),
					Error:               "domain/network mismatch",
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		if env.MsgType != string(protoID) {
			respBody := writeErrProto(s, "msg_type mismatch")
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					ExpireAt:            env.ExpireAtUnix,
					Request:             normalizeProtoTracePayload(env.Payload),
					Error:               "msg_type mismatch",
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		if err := verifyRemotePeerBytes(s.Conn().RemotePeer(), env.SenderPubkey); err != nil {
			respBody := writeErrProto(s, err.Error())
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					SenderPubkeyHex:     strings.ToLower(hex.EncodeToString(env.SenderPubkey)),
					ExpireAt:            env.ExpireAtUnix,
					Request:             normalizeProtoTracePayload(env.Payload),
					Error:               err.Error(),
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}

		trace := cfg.Trace
		senderPubHex := senderPubkeyHex(env.SenderPubkey)
		ctx = context.WithValue(ctx, senderPubkeyHexContextKey, senderPubHex)
		ctx = context.WithValue(ctx, messageIDContextKey, strings.TrimSpace(env.MsgId))
		traceReq := normalizeProtoTracePayload(env.Payload)

		payloadHash := envelope.EnvelopePayloadHashBytes(env.Payload)
		if cfg.Replay != nil {
			entry, found, err := cfg.Replay.Get(string(protoID), env.MsgId)
			if err != nil {
				respBody := writeErrProto(s, err.Error())
				if trace != nil {
					trace.Handle(TraceEvent{
						TS:                  nowRFC3339(),
						Direction:           "recv",
						Domain:              cfg.Domain,
						Network:             cfg.Network,
						ProtoID:             string(protoID),
						MsgID:               env.MsgId,
						LocalPeerID:         localPID,
						RemotePeerID:        remotePID,
						SenderPubkeyHex:     senderPubHex,
						ExpireAt:            env.ExpireAtUnix,
						Request:             traceReq,
						Error:               err.Error(),
						RequestEnvelopeWire: body,
						RequestPayloadWire:  env.Payload,
						ResponsePayloadWire: respBody,
					})
				}
				return
			}
			if found {
				if entry.PayloadHash != string(payloadHash) {
					respBody := writeErrProto(s, "ERR_IDEMPOTENCY_REPLAY")
					if trace != nil {
						trace.Handle(TraceEvent{
							TS:                  nowRFC3339(),
							Direction:           "recv",
							Domain:              cfg.Domain,
							Network:             cfg.Network,
							ProtoID:             string(protoID),
							MsgID:               env.MsgId,
							LocalPeerID:         localPID,
							RemotePeerID:        remotePID,
							SenderPubkeyHex:     senderPubHex,
							ExpireAt:            env.ExpireAtUnix,
							ReplayHit:           true,
							Request:             traceReq,
							Error:               "ERR_IDEMPOTENCY_REPLAY",
							RequestEnvelopeWire: body,
							RequestPayloadWire:  env.Payload,
							ResponsePayloadWire: respBody,
						})
					}
					return
				}
				_, _ = s.Write(entry.Response)
				if trace != nil {
					trace.Handle(TraceEvent{
						TS:                  nowRFC3339(),
						Direction:           "recv",
						Domain:              cfg.Domain,
						Network:             cfg.Network,
						ProtoID:             string(protoID),
						MsgID:               env.MsgId,
						LocalPeerID:         localPID,
						RemotePeerID:        remotePID,
						SenderPubkeyHex:     senderPubHex,
						ExpireAt:            env.ExpireAtUnix,
						ReplayHit:           true,
						Request:             traceReq,
						Response:            normalizeProtoTracePayload(entry.Response),
						RequestEnvelopeWire: body,
						RequestPayloadWire:  env.Payload,
						ResponsePayloadWire: entry.Response,
					})
				}
				return
			}
		}

		req, err := decodeProtoValue[TReq](env.Payload)
		if err != nil {
			respBody := writeErrProto(s, "decode payload: "+err.Error())
			if trace != nil {
				trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					SenderPubkeyHex:     senderPubHex,
					ExpireAt:            env.ExpireAtUnix,
					Request:             traceReq,
					Error:               "decode payload: " + err.Error(),
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		resp, err := fn(ctx, req)
		if err != nil {
			respBody := writeErrProto(s, err.Error())
			if trace != nil {
				trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					SenderPubkeyHex:     senderPubHex,
					ExpireAt:            env.ExpireAtUnix,
					Request:             traceReq,
					Error:               err.Error(),
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: respBody,
				})
			}
			return
		}
		respBody, err := encodeProtoValue(resp)
		if err != nil {
			errBody := writeErrProto(s, err.Error())
			if trace != nil {
				trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "recv",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         localPID,
					RemotePeerID:        remotePID,
					SenderPubkeyHex:     senderPubHex,
					ExpireAt:            env.ExpireAtUnix,
					Request:             traceReq,
					Error:               err.Error(),
					RequestEnvelopeWire: body,
					RequestPayloadWire:  env.Payload,
					ResponsePayloadWire: errBody,
				})
			}
			return
		}
		if cfg.Replay != nil {
			_ = cfg.Replay.Put(string(protoID), env.MsgId, string(payloadHash), respBody, env.ExpireAtUnix)
		}
		_, _ = s.Write(respBody)
		if trace != nil {
			trace.Handle(TraceEvent{
				TS:                  nowRFC3339(),
				Direction:           "recv",
				Domain:              cfg.Domain,
				Network:             cfg.Network,
				ProtoID:             string(protoID),
				MsgID:               env.MsgId,
				LocalPeerID:         localPID,
				RemotePeerID:        remotePID,
				SenderPubkeyHex:     senderPubHex,
				ExpireAt:            env.ExpireAtUnix,
				Request:             traceReq,
				Response:            normalizeProtoTracePayload(respBody),
				RequestEnvelopeWire: body,
				RequestPayloadWire:  env.Payload,
				ResponsePayloadWire: respBody,
			})
		}
	})
}

// CallProto 使用 protobuf 二进制编解码发起 p2p RPC。
func CallProto[TReq any, TResp any](ctx context.Context, h host.Host, pid peer.ID, protoID coreprotocol.ID, cfg SecurityConfig, req TReq, out *TResp) error {
	start := time.Now()
	s, err := h.NewStream(ctx, pid, protoID)
	if err != nil {
		return err
	}
	defer s.Close()

	payload, err := encodeProtoValue(req)
	if err != nil {
		return err
	}
	priv := h.Peerstore().PrivKey(h.ID())
	if priv == nil {
		return fmt.Errorf("missing host private key")
	}
	privBytes, _ := priv.Raw()
	msgID, err := randomMsgID()
	if err != nil {
		return err
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 20 * time.Second
	}

	scalarHex, _, err := stretchedPrivToScalarAndPubkey(privBytes)
	if err != nil {
		return fmt.Errorf("extract secp256k1 scalar: %w", err)
	}
	env, err := envelope.NewSignedEnvelopePB(scalarHex, cfg.Domain, cfg.Network, string(protoID), msgID, int64(ttl), payload)
	if err != nil {
		return err
	}
	wire, err := marshalProtoDeterministic(env)
	if err != nil {
		return err
	}
	if _, err := s.Write(wire); err != nil {
		return err
	}
	if cw, ok := s.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}

	body, err := io.ReadAll(s)
	if err != nil {
		if cfg.Trace != nil {
			cfg.Trace.Handle(TraceEvent{
				TS:                  nowRFC3339(),
				Direction:           "send",
				Domain:              cfg.Domain,
				Network:             cfg.Network,
				ProtoID:             string(protoID),
				MsgID:               env.MsgId,
				LocalPeerID:         h.ID().String(),
				RemotePeerID:        pid.String(),
				ExpireAt:            env.ExpireAtUnix,
				Request:             normalizeProtoTracePayload(payload),
				Error:               err.Error(),
				DurationMS:          time.Since(start).Milliseconds(),
				RequestEnvelopeWire: wire,
				RequestPayloadWire:  payload,
			})
		}
		return err
	}
	if len(body) == 0 {
		if cfg.Trace != nil {
			cfg.Trace.Handle(TraceEvent{
				TS:                  nowRFC3339(),
				Direction:           "send",
				Domain:              cfg.Domain,
				Network:             cfg.Network,
				ProtoID:             string(protoID),
				MsgID:               env.MsgId,
				LocalPeerID:         h.ID().String(),
				RemotePeerID:        pid.String(),
				ExpireAt:            env.ExpireAtUnix,
				Request:             normalizeProtoTracePayload(payload),
				Error:               "empty response",
				DurationMS:          time.Since(start).Milliseconds(),
				RequestEnvelopeWire: wire,
				RequestPayloadWire:  payload,
			})
		}
		return fmt.Errorf("empty response")
	}
	var e ErrorResponsePB
	if err := oldproto.Unmarshal(body, &e); err == nil && e.IsError && strings.TrimSpace(e.Error) != "" {
		if cfg.Trace != nil {
			cfg.Trace.Handle(TraceEvent{
				TS:                  nowRFC3339(),
				Direction:           "send",
				Domain:              cfg.Domain,
				Network:             cfg.Network,
				ProtoID:             string(protoID),
				MsgID:               env.MsgId,
				LocalPeerID:         h.ID().String(),
				RemotePeerID:        pid.String(),
				ExpireAt:            env.ExpireAtUnix,
				Request:             normalizeProtoTracePayload(payload),
				Response:            normalizeProtoTracePayload(body),
				Error:               e.Error,
				DurationMS:          time.Since(start).Milliseconds(),
				RequestEnvelopeWire: wire,
				RequestPayloadWire:  payload,
				ResponsePayloadWire: body,
			})
		}
		return errors.New(e.Error)
	}
	if err := decodeProtoInto(body, out); err != nil {
		var legacy legacyErrorResponsePB
		if uerr := oldproto.Unmarshal(body, &legacy); uerr == nil && strings.TrimSpace(legacy.Error) != "" {
			if cfg.Trace != nil {
				cfg.Trace.Handle(TraceEvent{
					TS:                  nowRFC3339(),
					Direction:           "send",
					Domain:              cfg.Domain,
					Network:             cfg.Network,
					ProtoID:             string(protoID),
					MsgID:               env.MsgId,
					LocalPeerID:         h.ID().String(),
					RemotePeerID:        pid.String(),
					ExpireAt:            env.ExpireAtUnix,
					Request:             normalizeProtoTracePayload(payload),
					Response:            normalizeProtoTracePayload(body),
					Error:               legacy.Error,
					DurationMS:          time.Since(start).Milliseconds(),
					RequestEnvelopeWire: wire,
					RequestPayloadWire:  payload,
					ResponsePayloadWire: body,
				})
			}
			return errors.New(legacy.Error)
		}
		if cfg.Trace != nil {
			cfg.Trace.Handle(TraceEvent{
				TS:                  nowRFC3339(),
				Direction:           "send",
				Domain:              cfg.Domain,
				Network:             cfg.Network,
				ProtoID:             string(protoID),
				MsgID:               env.MsgId,
				LocalPeerID:         h.ID().String(),
				RemotePeerID:        pid.String(),
				ExpireAt:            env.ExpireAtUnix,
				Request:             normalizeProtoTracePayload(payload),
				Response:            normalizeProtoTracePayload(body),
				Error:               err.Error(),
				DurationMS:          time.Since(start).Milliseconds(),
				RequestEnvelopeWire: wire,
				RequestPayloadWire:  payload,
				ResponsePayloadWire: body,
			})
		}
		return err
	}
	if cfg.Trace != nil {
		respBody, _ := encodeProtoValue(*out)
		cfg.Trace.Handle(TraceEvent{
			TS:                  nowRFC3339(),
			Direction:           "send",
			Domain:              cfg.Domain,
			Network:             cfg.Network,
			ProtoID:             string(protoID),
			MsgID:               env.MsgId,
			LocalPeerID:         h.ID().String(),
			RemotePeerID:        pid.String(),
			ExpireAt:            env.ExpireAtUnix,
			Request:             normalizeProtoTracePayload(payload),
			Response:            normalizeProtoTracePayload(respBody),
			DurationMS:          time.Since(start).Milliseconds(),
			RequestEnvelopeWire: wire,
			RequestPayloadWire:  payload,
			ResponsePayloadWire: respBody,
		})
	}
	return nil
}

func writeErrProto(w io.Writer, msg string) []byte {
	b, err := marshalProtoDeterministic(&ErrorResponsePB{IsError: true, Error: msg})
	if err != nil {
		// 兜底：编码失败时直接写空错误串的 protobuf 消息。
		b, _ = marshalProtoDeterministic(&ErrorResponsePB{IsError: true, Error: "internal proto encode failed"})
	}
	_, _ = w.Write(b)
	return b
}

func marshalProtoDeterministic(m oldproto.Message) ([]byte, error) {
	buf := oldproto.NewBuffer(nil)
	buf.SetDeterministic(true)
	if err := buf.Marshal(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeProtoValue[T any](v T) ([]byte, error) {
	if m, ok := any(v).(oldproto.Message); ok {
		return marshalProtoDeterministic(m)
	}
	if m, ok := any(&v).(oldproto.Message); ok {
		return marshalProtoDeterministic(m)
	}
	return nil, fmt.Errorf("payload type %T does not implement protobuf message", v)
}

func decodeProtoValue[T any](payload []byte) (T, error) {
	var out T
	if m, ok := any(&out).(oldproto.Message); ok {
		if err := oldproto.Unmarshal(payload, m); err != nil {
			return out, err
		}
		return out, nil
	}
	if m, ok := any(out).(oldproto.Message); ok {
		if err := oldproto.Unmarshal(payload, m); err != nil {
			return out, err
		}
		return out, nil
	}
	return out, fmt.Errorf("payload type %T does not implement protobuf message", out)
}

func decodeProtoInto[T any](payload []byte, out *T) error {
	if out == nil {
		return fmt.Errorf("response out is nil")
	}
	if m, ok := any(out).(oldproto.Message); ok {
		return oldproto.Unmarshal(payload, m)
	}
	return fmt.Errorf("response type %T does not implement protobuf message", *out)
}

func normalizeProtoTracePayload(b []byte) any {
	if len(b) == 0 {
		return map[string]any{"payload_bytes": 0}
	}
	preview := hex.EncodeToString(b)
	if len(preview) > 64 {
		preview = preview[:64]
	}
	return map[string]any{
		"payload_bytes":      len(b),
		"payload_hex_prefix": preview,
	}
}

const secp256k1TypePriv = 0x09
const secp256k1TypePub = 0x09

func stretchedPrivToScalarAndPubkey(privBytes []byte) (scalarHex string, senderPubkey []byte, err error) {
	var scalar []byte
	switch {
	case len(privBytes) == 33 && privBytes[0] == secp256k1TypePriv:
		scalar = privBytes[1:]
	case len(privBytes) == 32:
		scalar = privBytes
	default:
		prefix := byte(0)
		if len(privBytes) > 0 {
			prefix = privBytes[0]
		}
		return "", nil, fmt.Errorf("expected secp256k1 privkey (32 bytes scalar or 33 bytes stretched), got %d bytes with 0x%02x prefix", len(privBytes), prefix)
	}
	scalarHex = hex.EncodeToString(scalar)
	privKey := secp256k1.PrivKeyFromBytes(scalar)
	pubKey := privKey.PubKey()
	pubKeyCompressed := pubKey.SerializeCompressed()
	senderPubkey = pubKeyCompressed
	return scalarHex, senderPubkey, nil
}
