package gatewayclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	oldproto "github.com/golang/protobuf/proto"
	ma "github.com/multiformats/go-multiaddr"
)

type NodeReachabilityAnnouncement struct {
	NodePubkeyHex   string
	Multiaddrs      []string
	HeadHeight      uint64
	Seq             uint64
	PublishedAtUnix int64
	ExpiresAtUnix   int64
	Signature       []byte
}

func (ann NodeReachabilityAnnouncement) Normalize() NodeReachabilityAnnouncement {
	ann.NodePubkeyHex = poolcore.NormalizeClientIDLoose(ann.NodePubkeyHex)
	ann.Multiaddrs = normalizeStringList(ann.Multiaddrs)
	ann.Signature = append([]byte(nil), ann.Signature...)
	return ann
}

func (ann NodeReachabilityAnnouncement) UnsignedArray() []any {
	ann = ann.Normalize()
	return []any{
		"bsv8-node-reachability-announcement-v1",
		ann.NodePubkeyHex,
		ann.Multiaddrs,
		ann.HeadHeight,
		ann.Seq,
		ann.PublishedAtUnix,
		ann.ExpiresAtUnix,
	}
}

// NormalizeNodeReachabilityAddrs 统一节点可达地址。
// 设计说明：
// - 只接受和节点公钥一致的 /p2p/peerID；
// - 去重、排序都在这里做，避免不同入口算出不同签名；
// - 这组地址是声明事实，不替节点猜地址。
func NormalizeNodeReachabilityAddrs(nodePubkeyHex string, addrs []string) ([]string, error) {
	nodePubkeyHex, err := poolcore.NormalizeClientIDStrict(nodePubkeyHex)
	if err != nil {
		return nil, err
	}
	expectPID, err := poolcore.PeerIDFromClientID(nodePubkeyHex)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, raw := range addrs {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(v)
		if err != nil {
			return nil, fmt.Errorf("invalid multiaddr: %w", err)
		}
		pid, err := addr.ValueForProtocol(ma.P_P2P)
		if err != nil {
			return nil, fmt.Errorf("multiaddr missing /p2p peer id")
		}
		if !strings.EqualFold(strings.TrimSpace(pid), expectPID.String()) {
			return nil, fmt.Errorf("multiaddr peer id mismatch")
		}
		canonical := addr.String()
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		out = append(out, canonical)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("multiaddrs required")
	}
	sort.Strings(out)
	return out, nil
}

// BuildNodeReachabilitySignPayload 构建节点可达性声明的签名内容。
func BuildNodeReachabilitySignPayload(nodePubkeyHex string, addrs []string, headHeight uint64, seq uint64, publishedAtUnix int64, expiresAtUnix int64) ([]byte, error) {
	nodePubkeyHex, err := poolcore.NormalizeClientIDStrict(nodePubkeyHex)
	if err != nil {
		return nil, err
	}
	addrs, err = NormalizeNodeReachabilityAddrs(nodePubkeyHex, addrs)
	if err != nil {
		return nil, err
	}
	if seq == 0 {
		return nil, fmt.Errorf("seq must be >= 1")
	}
	if publishedAtUnix <= 0 {
		return nil, fmt.Errorf("published_at_unix required")
	}
	if expiresAtUnix <= publishedAtUnix {
		return nil, fmt.Errorf("expires_at_unix must be greater than published_at_unix")
	}
	raw, err := json.Marshal(NodeReachabilityAnnouncement{
		NodePubkeyHex:   nodePubkeyHex,
		Multiaddrs:      addrs,
		HeadHeight:      headHeight,
		Seq:             seq,
		PublishedAtUnix: publishedAtUnix,
		ExpiresAtUnix:   expiresAtUnix,
	}.UnsignedArray())
	if err != nil {
		return nil, fmt.Errorf("marshal reachability payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

// MarshalSignedNodeReachabilityAnnouncement 把完整声明编码成稳定的 JSON 数组。
func MarshalSignedNodeReachabilityAnnouncement(ann NodeReachabilityAnnouncement) ([]byte, error) {
	ann = ann.Normalize()
	if err := VerifyNodeReachabilityAnnouncement(ann); err != nil {
		return nil, err
	}
	return json.Marshal([]any{ann.UnsignedArray(), hex.EncodeToString(ann.Signature)})
}

// UnmarshalSignedNodeReachabilityAnnouncement 解析 gateway 返回的完整声明。
func UnmarshalSignedNodeReachabilityAnnouncement(raw []byte) (NodeReachabilityAnnouncement, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return NodeReachabilityAnnouncement{}, fmt.Errorf("decode signed announcement: %w", err)
	}
	if len(parts) != 2 {
		return NodeReachabilityAnnouncement{}, fmt.Errorf("signed announcement fields mismatch")
	}
	var fields []json.RawMessage
	if err := json.Unmarshal(parts[0], &fields); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if len(fields) != 7 {
		return NodeReachabilityAnnouncement{}, fmt.Errorf("announcement unsigned fields mismatch")
	}
	var ann NodeReachabilityAnnouncement
	var version string
	if err := json.Unmarshal(fields[0], &version); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if version != "bsv8-node-reachability-announcement-v1" {
		return NodeReachabilityAnnouncement{}, fmt.Errorf("announcement version mismatch")
	}
	if err := json.Unmarshal(fields[1], &ann.NodePubkeyHex); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if err := json.Unmarshal(fields[2], &ann.Multiaddrs); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if err := json.Unmarshal(fields[3], &ann.HeadHeight); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if err := json.Unmarshal(fields[4], &ann.Seq); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if err := json.Unmarshal(fields[5], &ann.PublishedAtUnix); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	if err := json.Unmarshal(fields[6], &ann.ExpiresAtUnix); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	var signatureHex string
	if err := json.Unmarshal(parts[1], &signatureHex); err != nil {
		return NodeReachabilityAnnouncement{}, err
	}
	signature, err := hex.DecodeString(strings.TrimSpace(signatureHex))
	if err != nil || len(signature) == 0 {
		return NodeReachabilityAnnouncement{}, fmt.Errorf("announcement signature hex invalid")
	}
	ann.Signature = signature
	return ann.Normalize(), nil
}

// VerifyNodeReachabilityAnnouncement 验证节点可达性声明签名。
func VerifyNodeReachabilityAnnouncement(ann NodeReachabilityAnnouncement) error {
	nodePubkeyHex, err := poolcore.NormalizeClientIDStrict(ann.NodePubkeyHex)
	if err != nil {
		return err
	}
	if len(ann.Signature) == 0 {
		return fmt.Errorf("signature required")
	}
	payload, err := BuildNodeReachabilitySignPayload(
		nodePubkeyHex,
		ann.Multiaddrs,
		ann.HeadHeight,
		ann.Seq,
		ann.PublishedAtUnix,
		ann.ExpiresAtUnix,
	)
	if err != nil {
		return err
	}
	pubBytes, err := hex.DecodeString(nodePubkeyHex)
	if err != nil {
		return fmt.Errorf("decode client_pubkey_hex: %w", err)
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(pubBytes)
	if err != nil {
		return fmt.Errorf("unmarshal client pubkey: %w", err)
	}
	ok, err := pub.Verify(payload, ann.Signature)
	if err != nil {
		return fmt.Errorf("verify announcement signature: %w", err)
	}
	if !ok {
		return fmt.Errorf("announcement signature invalid")
	}
	return nil
}

// AnnounceNodeReachability 发布本节点可达性。
func (s *service) AnnounceNodeReachability(ctx context.Context, ttlSeconds uint32) (contractmessage.NodeReachabilityAnnouncePaidResp, error) {
	if err := s.requireHost(); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if err := s.requireStore(); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if ttlSeconds == 0 {
		ttlSeconds = 3600
	}

	gateway, err := s.pickGateway("")
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}

	nodePubkeyHex := s.host.NodePubkeyHex()
	multiaddrs := s.host.LocalAdvertiseAddrs()
	if len(multiaddrs) == 0 {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("no advertise addrs")
	}
	normalizedAddrs, err := NormalizeNodeReachabilityAddrs(nodePubkeyHex, multiaddrs)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}

	headHeight, err := s.host.CurrentHeadHeight(ctx)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("query head height failed: %w", err)
	}

	nowUnix := time.Now().Unix()
	expiresAtUnix := nowUnix + int64(ttlSeconds)
	seq := uint64(1)
	if state, found, err := s.store.GetSelfNodeReachabilityState(ctx, nodePubkeyHex); err == nil && found && state.HeadHeight == headHeight {
		seq = state.Seq + 1
	} else if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}

	signPayload, err := BuildNodeReachabilitySignPayload(nodePubkeyHex, normalizedAddrs, headHeight, seq, nowUnix, expiresAtUnix)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	sig, err := s.host.SignLocalNodePayload(ctx, signPayload)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("sign node reachability announcement failed: %w", err)
	}

	ann := NodeReachabilityAnnouncement{
		NodePubkeyHex:   nodePubkeyHex,
		Multiaddrs:      normalizedAddrs,
		HeadHeight:      headHeight,
		Seq:             seq,
		PublishedAtUnix: nowUnix,
		ExpiresAtUnix:   expiresAtUnix,
		Signature:       append([]byte(nil), sig...),
	}
	signedAnnouncement, err := MarshalSignedNodeReachabilityAnnouncement(ann)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}

	body := &contractmessage.NodeReachabilityAnnounceReq{SignedAnnouncement: signedAnnouncement}
	bodyRaw, err := marshalProto(body)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}

	preflightResp, err := s.quotePeer(ctx, gateway.Pubkey, ProtoBroadcastV1NodeReachabilityAnnounce, bodyRaw)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	preflight, err := callRespToResult(preflightResp)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if err := validateNodeReachabilityPreflight(preflight); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}

	paidResp, err := s.payQuotedCall(ctx, gateway.Pubkey, ProtoBroadcastV1NodeReachabilityAnnounce, bodyRaw, ncall.PaymentSchemeChainTxV1, preflightResp.ServiceQuote)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	paidCall, err := callRespToResult(paidResp)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if paidCall == nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("gateway call failed")
	}

	var announceResp contractmessage.NodeReachabilityAnnouncePaidResp
	if err := oldproto.Unmarshal(paidCall.Body, &announceResp); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if !announceResp.Success {
		msg := strings.TrimSpace(announceResp.Error)
		if msg == "" {
			msg = "gateway node reachability announce rejected"
		}
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("gateway node reachability announce rejected: status=%s error=%s", strings.TrimSpace(announceResp.Status), msg)
	}
	if err := s.store.SaveSelfNodeReachabilityState(ctx, SelfNodeReachabilityState{
		NodePubkeyHex:   nodePubkeyHex,
		HeadHeight:      headHeight,
		Seq:             seq,
		LastAnnouncedAt: nowUnix,
		ExpiresAtUnix:   expiresAtUnix,
		UpdatedAtUnix:   nowUnix,
	}); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	return announceResp, nil
}

// QueryNodeReachability 查询目标节点的最新地址声明。
func (s *service) QueryNodeReachability(ctx context.Context, targetNodePubkeyHex string) (contractmessage.NodeReachabilityQueryPaidResp, error) {
	if err := s.requireHost(); err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	if err := s.requireStore(); err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}

	targetNodePubkeyHex, err := poolcore.NormalizeClientIDStrict(targetNodePubkeyHex)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("target_node_pubkey_hex invalid: %w", err)
	}

	gateway, err := s.pickGateway("")
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}

	body := &contractmessage.NodeReachabilityQueryReq{TargetNodePubkeyHex: targetNodePubkeyHex}
	bodyRaw, err := marshalProto(body)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}

	preflightResp, err := s.quotePeer(ctx, gateway.Pubkey, ProtoBroadcastV1NodeReachabilityQuery, bodyRaw)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	preflight, err := callRespToResult(preflightResp)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	if err := validateNodeReachabilityPreflight(preflight); err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}

	paidResp, err := s.payQuotedCall(ctx, gateway.Pubkey, ProtoBroadcastV1NodeReachabilityQuery, bodyRaw, ncall.PaymentSchemeChainTxV1, preflightResp.ServiceQuote)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	paidCall, err := callRespToResult(paidResp)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	if paidCall == nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("gateway call failed")
	}

	var queryResp contractmessage.NodeReachabilityQueryPaidResp
	if err := oldproto.Unmarshal(paidCall.Body, &queryResp); err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	if !queryResp.Success {
		msg := strings.TrimSpace(queryResp.Error)
		if msg == "" {
			msg = "gateway node reachability query rejected"
		}
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("gateway node reachability query rejected: status=%s error=%s", strings.TrimSpace(queryResp.Status), msg)
	}
	if queryResp.Found && len(queryResp.SignedAnnouncement) > 0 {
		ann, err := UnmarshalSignedNodeReachabilityAnnouncement(queryResp.SignedAnnouncement)
		if err == nil {
			ttl := time.Until(time.Unix(ann.ExpiresAtUnix, 0))
			if ttl > 0 {
				_ = s.host.InjectPeerAddrs(ctx, ann.NodePubkeyHex, ann.Multiaddrs, int64(ttl/time.Second))
				_ = s.store.SaveNodeReachabilityCache(ctx, NodeReachabilityCache{
					NodePubkeyHex:          ann.NodePubkeyHex,
					Multiaddrs:             ann.Multiaddrs,
					HeadHeight:             ann.HeadHeight,
					Seq:                    ann.Seq,
					PublishedAtUnix:        ann.PublishedAtUnix,
					ExpiresAtUnix:          ann.ExpiresAtUnix,
					Signature:              append([]byte(nil), ann.Signature...),
					SourceGatewayPubkeyHex: gateway.Pubkey,
					UpdatedAtUnix:          time.Now().Unix(),
				})
			}
		}
	}
	return queryResp, nil
}

func validateNodeReachabilityPreflight(resp *ncall.CallResp) error {
	if resp == nil {
		return fmt.Errorf("preflight response missing")
	}
	if !strings.EqualFold(strings.TrimSpace(resp.Code), "PAYMENT_QUOTED") {
		return fmt.Errorf("payment quote unavailable")
	}
	for _, opt := range resp.PaymentSchemes {
		if opt == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(opt.Scheme), ncall.PaymentSchemeChainTxV1) {
			return nil
		}
	}
	return fmt.Errorf("chain_tx_v1 not offered")
}
