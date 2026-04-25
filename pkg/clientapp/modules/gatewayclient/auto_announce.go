package gatewayclient

import (
	"context"
	"fmt"
	"time"
)

const (
	autoNodeReachabilityAnnounceCheckInterval = 15 * time.Second
	autoNodeReachabilityAnnounceRenewLead     = 60 * time.Second
	autoNodeReachabilityAnnounceTTL           = 3600
)

type autoAnnounceState struct {
	LastFingerprint      string
	LastGatewayPubkeyHex string
	LastExpiresAtUnix   int64
	LastAttemptError     string
	LastAnnouncedAtUnix int64
}

func (s *service) StartAutoAnnounce(ctx context.Context) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("service not initialized")
	}
	go s.runAutoAnnounceLoop(ctx)
	return nil
}

func (s *service) runAutoAnnounceLoop(ctx context.Context) {
	ticker := time.NewTicker(autoNodeReachabilityAnnounceCheckInterval)
	defer ticker.Stop()

	var state autoAnnounceState
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.doAutoAnnounce(ctx, &state); err != nil {
				state.LastAttemptError = err.Error()
			}
		}
	}
}

func (s *service) doAutoAnnounce(ctx context.Context, state *autoAnnounceState) error {
	nowUnix := time.Now().Unix()
	gateway, err := s.pickGateway("")
	if err != nil {
		return err
	}

	// Get current node info
	nodePubkeyHex := s.host.NodePubkeyHex()
	multiaddrs := s.host.LocalAdvertiseAddrs()
	if len(multiaddrs) == 0 {
		return fmt.Errorf("no advertise addrs")
	}

	headHeight, err := s.host.CurrentHeadHeight(ctx)
	if err != nil {
		return fmt.Errorf("query head height: %w", err)
	}

	// Build fingerprint
	fingerprint := fmt.Sprintf("%s/%d/%v", nodePubkeyHex, headHeight, multiaddrs)
	gatewayPubkeyHex := gateway.Pubkey

	// Check if announcement is needed
	if shouldAutoAnnounce(nowUnix, *state, fingerprint, gatewayPubkeyHex) {
		_, err := s.AnnounceNodeReachability(ctx, autoNodeReachabilityAnnounceTTL)
		if err != nil {
			return fmt.Errorf("announce failed: %w", err)
		}
		state.LastFingerprint = fingerprint
		state.LastGatewayPubkeyHex = gatewayPubkeyHex
		state.LastExpiresAtUnix = nowUnix + int64(autoNodeReachabilityAnnounceTTL)
		state.LastAnnouncedAtUnix = nowUnix
		state.LastAttemptError = ""
	}

	return nil
}

func shouldAutoAnnounce(nowUnix int64, state autoAnnounceState, fingerprint, gateway string) bool {
	// First announcement
	if state.LastFingerprint == "" {
		return true
	}

	// Gateway changed
	if state.LastGatewayPubkeyHex != gateway {
		return true
	}

	// Fingerprint changed
	if state.LastFingerprint != fingerprint {
		return true
	}

	// Renew window reached (renew before expiry)
	if state.LastExpiresAtUnix > 0 && nowUnix >= (state.LastExpiresAtUnix-int64(autoNodeReachabilityAnnounceRenewLead/time.Second)) {
		return true
	}

	return false
}
