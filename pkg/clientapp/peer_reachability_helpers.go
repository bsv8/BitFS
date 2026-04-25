package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// ensureTargetPeerReachable 确保目标节点可达。
// 设计说明：这是通用 peer-call 基础设施，不是 gateway 专属逻辑。
// 实现会先走本地 peerstore，失败后再通过 gatewayclient 模块查询并回填 peerstore。
func ensureTargetPeerReachable(ctx context.Context, store *clientDB, rt *Runtime, targetPubkeyHex string, pid peer.ID) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if rt.Host.Network().Connectedness(pid) == libnetwork.Connected {
		return nil
	}

	// 先尝试通过 peerstore 连接
	if err := connectViaPeerstore(ctx, rt, pid); err == nil {
		return nil
	}

	if err := queryAndInjectPeerAddrs(ctx, store, rt, targetPubkeyHex); err != nil {
		return fmt.Errorf("query node reachability failed: %w", err)
	}
	return connectViaPeerstore(ctx, rt, pid)
}

func queryAndInjectPeerAddrs(ctx context.Context, store *clientDB, rt *Runtime, targetPubkeyHex string) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	targetPubkeyHex = poolcore.NormalizeClientIDLoose(targetPubkeyHex)
	gwStore, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return err
	}
	host := newModuleHost(rt, store)
	svc := gatewayclient.NewService(host, gwStore)
	resp, err := svc.QueryNodeReachability(ctx, targetPubkeyHex)
	if err != nil {
		return err
	}
	if !resp.Found || len(resp.SignedAnnouncement) == 0 {
		return fmt.Errorf("target peer addrs unavailable")
	}
	ann, err := gatewayclient.UnmarshalSignedNodeReachabilityAnnouncement(resp.SignedAnnouncement)
	if err != nil {
		return fmt.Errorf("decode node reachability announcement failed: %w", err)
	}
	return injectNodeReachabilityAnnouncement(rt, ann)
}

func connectViaPeerstore(ctx context.Context, rt *Runtime, pid peer.ID) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if rt.Host.Network().Connectedness(pid) == libnetwork.Connected {
		return nil
	}
	addrs := rt.Host.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		return fmt.Errorf("target peer addrs unavailable")
	}
	return rt.Host.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: addrs})
}

func injectNodeReachabilityAnnouncement(rt *Runtime, ann gatewayclient.NodeReachabilityAnnouncement) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	pid, err := poolcore.PeerIDFromClientID(ann.NodePubkeyHex)
	if err != nil {
		return err
	}
	ttl := time.Until(time.Unix(ann.ExpiresAtUnix, 0))
	if ttl <= 0 {
		return fmt.Errorf("announcement expired")
	}
	for _, raw := range ann.Multiaddrs {
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return err
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return err
		}
		if info.ID != pid {
			return fmt.Errorf("announcement peer id mismatch")
		}
		rt.Host.Peerstore().AddAddrs(pid, info.Addrs, ttl)
	}
	return nil
}

func announcementSemanticallyEqualGateway(a, b gatewayclient.NodeReachabilityAnnouncement) bool {
	if !strings.EqualFold(strings.TrimSpace(a.NodePubkeyHex), strings.TrimSpace(b.NodePubkeyHex)) {
		return false
	}
	if a.HeadHeight != b.HeadHeight || a.Seq != b.Seq || a.PublishedAtUnix != b.PublishedAtUnix || a.ExpiresAtUnix != b.ExpiresAtUnix {
		return false
	}
	if len(a.Multiaddrs) != len(b.Multiaddrs) || len(a.Signature) != len(b.Signature) {
		return false
	}
	for i := range a.Multiaddrs {
		if a.Multiaddrs[i] != b.Multiaddrs[i] {
			return false
		}
	}
	for i := range a.Signature {
		if a.Signature[i] != b.Signature[i] {
			return false
		}
	}
	return true
}
