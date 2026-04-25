package gatewayclient

import (
	"context"
	"fmt"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// service 是 gatewayclient 模块的业务壳。
//
// 设计说明：
// - 只接显式能力，不碰 Runtime、*sql.DB；
// - 所有远端调用都走这里，避免 root 再拼一套半链路；
// - 选择 gateway、quote、pay、落库都收在模块内部。
type service struct {
	host  moduleapi.Host
	store Store
}

// NewService 创建 gatewayclient 服务实例。
func NewService(host moduleapi.Host, store Store) *service {
	return &service{host: host, store: store}
}

func (s *service) obsActions() []moduleapi.OBSAction {
	return []moduleapi.OBSAction{
		{Action: OBSActionGatewayEvents, Handler: s.OBSActionHandler},
		{Action: OBSActionObservedGatewayStates, Handler: s.OBSActionHandler},
		{Action: OBSActionNodeReachabilityCache, Handler: s.OBSActionHandler},
		{Action: OBSActionSelfNodeReachability, Handler: s.OBSActionHandler},
	}
}

func (s *service) settingsActions() []moduleapi.SettingsAction {
	return []moduleapi.SettingsAction{
		{Action: settingsActionGatewayAnnounce, Handler: s.SettingsHandler},
		{Action: settingsActionGatewayQuery, Handler: s.SettingsHandler},
		{Action: OBSActionGatewayEvents, Handler: s.SettingsHandler},
		{Action: OBSActionObservedGatewayStates, Handler: s.SettingsHandler},
		{Action: OBSActionNodeReachabilityCache, Handler: s.SettingsHandler},
		{Action: OBSActionSelfNodeReachability, Handler: s.SettingsHandler},
	}
}

func (s *service) openHooks() []moduleapi.OpenHook {
	return []moduleapi.OpenHook{
		func(ctx context.Context) error {
			return s.StartAutoAnnounce(ctx)
		},
	}
}

func (s *service) requireHost() error {
	if s == nil || s.host == nil {
		return fmt.Errorf("service not initialized")
	}
	return nil
}

func (s *service) requireStore() error {
	if s == nil || s.store == nil {
		return fmt.Errorf("store not initialized")
	}
	return nil
}

func (s *service) callPeerRaw(ctx context.Context, to string, protoID protocol.ID, req moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error) {
	if err := s.requireHost(); err != nil {
		return moduleapi.PeerCallResponse{}, err
	}
	typed, ok := s.host.(bizHost)
	if !ok {
		return moduleapi.PeerCallResponse{}, fmt.Errorf("host does not support peer calls")
	}
	req.To = strings.TrimSpace(to)
	req.ProtocolID = protoID
	req.ContentType = strings.TrimSpace(req.ContentType)
	return typed.PeerCallRaw(ctx, req.To, string(protoID), req)
}

func (s *service) callPeer(ctx context.Context, to string, protoID protocol.ID, body []byte) (moduleapi.PeerCallResponse, error) {
	return s.callPeerRaw(ctx, to, protoID, moduleapi.PeerCallRequest{
		To:          strings.TrimSpace(to),
		ProtocolID:  protoID,
		ContentType: contractmessage.ContentTypeProto,
		Body:        append([]byte(nil), body...),
	})
}

func (s *service) quotePeer(ctx context.Context, to string, protoID protocol.ID, body []byte) (moduleapi.PeerCallResponse, error) {
	return s.callPeerRaw(ctx, to, protoID, moduleapi.PeerCallRequest{
		To:          strings.TrimSpace(to),
		ProtocolID:  protoID,
		ContentType: contractmessage.ContentTypeProto,
		Body:        append([]byte(nil), body...),
		PaymentMode: "quote",
	})
}

func (s *service) payQuotedPeer(ctx context.Context, to string, protoID protocol.ID, body []byte, paymentScheme string, serviceQuote []byte) (moduleapi.PeerCallResponse, error) {
	return s.callPeerRaw(ctx, to, protoID, moduleapi.PeerCallRequest{
		To:            strings.TrimSpace(to),
		ProtocolID:    protoID,
		ContentType:   contractmessage.ContentTypeProto,
		Body:          append([]byte(nil), body...),
		PaymentMode:   "pay",
		PaymentScheme: strings.TrimSpace(paymentScheme),
		ServiceQuote: append([]byte(nil), serviceQuote...),
	})
}

func (s *service) pickGateway(gatewayPeerID string) (moduleapi.PeerNode, error) {
	if err := s.requireHost(); err != nil {
		return moduleapi.PeerNode{}, err
	}
	healthy := normalizeHealthyGateways(s.host.HealthyGatewaySnapshot())
	explicit := strings.ToLower(strings.TrimSpace(gatewayPeerID))
	if explicit != "" {
		for _, item := range healthy {
			if strings.EqualFold(strings.TrimSpace(item.Pubkey), explicit) {
				return item, nil
			}
		}
		return moduleapi.PeerNode{}, fmt.Errorf("gateway %s is not available", explicit)
	}
	preferred := strings.ToLower(strings.TrimSpace(s.host.PreferredGatewayPubkeyHex()))
	if preferred != "" {
		for _, item := range healthy {
			if strings.EqualFold(strings.TrimSpace(item.Pubkey), preferred) {
				return item, nil
			}
		}
	}
	if len(healthy) > 0 {
		return healthy[0], nil
	}
	return moduleapi.PeerNode{}, fmt.Errorf("no gateway available")
}

func normalizeHealthyGateways(in []moduleapi.PeerNode) []moduleapi.PeerNode {
	out := make([]moduleapi.PeerNode, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		if !item.Enabled {
			continue
		}
		pubkey, err := poolcore.NormalizeClientIDStrict(item.Pubkey)
		if err != nil {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		item.Pubkey = pubkey
		out = append(out, item)
	}
	return out
}

func gatewayPeerIDToPubkeyHex(pid peer.ID) string {
	return strings.ToLower(strings.TrimSpace(pid.String()))
}

func gatewayPeerFromHost(host moduleapi.Host, gatewayPeerID string) (peer.ID, *ec.PublicKey, error) {
	if host == nil {
		return "", nil, fmt.Errorf("host is required")
	}
	pid, err := poolcore.PeerIDFromClientID(strings.TrimSpace(gatewayPeerID))
	if err != nil {
		return "", nil, err
	}
	typed, ok := host.(bizHost)
	if !ok {
		return "", nil, fmt.Errorf("host does not support gateway pubkey lookup")
	}
	pub, err := typed.GetGatewayPubkey(pid)
	if err != nil {
		return "", nil, err
	}
	return pid, pub, nil
}
