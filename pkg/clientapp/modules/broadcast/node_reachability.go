package broadcast

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/libp2p/go-libp2p/core/crypto"
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

// NormalizeNodeReachabilityAddrs 统一地址声明里的 multiaddrs。
// 设计说明：
// - 节点声明的是“我当前在哪些地址上可达”，不是 gateway 帮它猜的地址；
// - 所有地址都必须带上与 node_pubkey_hex 一致的 /p2p/<peerID>，否则目录缓存会混淆主体；
// - 排序与去重在这里统一做，避免 client / gateway 分别实现后结果不一致。
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

func MarshalSignedNodeReachabilityAnnouncement(ann NodeReachabilityAnnouncement) ([]byte, error) {
	ann = ann.Normalize()
	if err := VerifyNodeReachabilityAnnouncement(ann); err != nil {
		return nil, err
	}
	return json.Marshal([]any{ann.UnsignedArray(), hex.EncodeToString(ann.Signature)})
}

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
