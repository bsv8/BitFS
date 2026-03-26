package clientapp

import (
	"context"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p/core/peer"
)

func fetchDomainFeePoolInfo(ctx context.Context, rt *Runtime, pid peer.ID) (dual2of2.InfoResp, peer.AddrInfo, error) {
	var info dual2of2.InfoResp
	gw := peer.AddrInfo{ID: pid, Addrs: rt.Host.Peerstore().Addrs(pid)}
	if err := p2prpc.CallProto(ctx, rt.Host, pid, dual2of2.ProtoFeePoolInfo, domainSec(rt), dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
		return dual2of2.InfoResp{}, peer.AddrInfo{}, err
	}
	return info, gw, nil
}

func domainSec(rt *Runtime) p2prpc.SecurityConfig {
	network := "test"
	if rt != nil && strings.TrimSpace(rt.runIn.BSV.Network) != "" {
		network = strings.ToLower(strings.TrimSpace(rt.runIn.BSV.Network))
	}
	return p2prpc.SecurityConfig{Domain: "bitcast-domain", Network: network, TTL: 30 * time.Second, Trace: rt.rpcTrace}
}
