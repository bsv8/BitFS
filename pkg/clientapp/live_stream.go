package clientapp

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	liveSubscribeURIScheme = "bitlive:libp2p"
	defaultLiveWindowSize  = 10
	maxLiveWindowSize      = 20
)

type LiveSubscribeURI struct {
	PublisherPubKey string
	StreamID        string
}

type LiveSegmentRef struct {
	SegmentIndex    uint64 `json:"segment_index"`
	SeedHash        string `json:"seed_hash"`
	PublishedAtUnix int64  `json:"published_at_unix,omitempty"`
}

type LiveSubscriberSnapshot struct {
	StreamID        string           `json:"stream_id"`
	PublisherPubKey string           `json:"publisher_pubkey"`
	RecentSegments  []LiveSegmentRef `json:"recent_segments"`
	UpdatedAtUnix   int64            `json:"updated_at_unix"`
}

type liveSubscriberTarget struct {
	PeerID string
	Addrs  []string
	Window int
}

type livePublishedStream struct {
	PublisherPubKey string
	RecentSegments  []LiveSegmentRef
	Subscribers     map[string]liveSubscriberTarget
}

type liveSegmentMeta struct {
	StreamID        string
	SegmentIndex    uint64
	PublishedAtUnix int64
}

type liveRuntime struct {
	mu          sync.RWMutex
	published   map[string]*livePublishedStream
	received    map[string]LiveSubscriberSnapshot
	segmentMeta map[string]liveSegmentMeta
	follows     map[string]*liveFollowState
	autoBuyFn   func(context.Context, *Runtime, LivePurchaseDecision, LiveSubscriberSnapshot) (liveAutoBuyResult, error)
}

func newLiveRuntime() *liveRuntime {
	return &liveRuntime{
		published:   map[string]*livePublishedStream{},
		received:    map[string]LiveSubscriberSnapshot{},
		segmentMeta: map[string]liveSegmentMeta{},
		follows:     map[string]*liveFollowState{},
	}
}

func BuildLiveSubscribeURI(publisherPubKey, streamID string) (string, error) {
	publisherPubKey = strings.ToLower(strings.TrimSpace(publisherPubKey))
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if publisherPubKey == "" || streamID == "" {
		return "", fmt.Errorf("publisher_pubkey and stream_id required")
	}
	if _, err := normalizeCompressedPubKeyHex(publisherPubKey); err != nil {
		return "", fmt.Errorf("invalid publisher_pubkey")
	}
	if !isSeedHashHex(streamID) {
		return "", fmt.Errorf("invalid stream_id")
	}
	return liveSubscribeURIScheme + ":" + publisherPubKey + ":" + streamID, nil
}

func ParseLiveSubscribeURI(raw string) (LiveSubscribeURI, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return LiveSubscribeURI{}, fmt.Errorf("stream uri required")
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 4 || parts[0] != "bitlive" || parts[1] != "libp2p" {
		return LiveSubscribeURI{}, fmt.Errorf("invalid stream uri")
	}
	out := LiveSubscribeURI{
		PublisherPubKey: strings.ToLower(strings.TrimSpace(parts[2])),
		StreamID:        strings.ToLower(strings.TrimSpace(parts[3])),
	}
	if _, err := normalizeCompressedPubKeyHex(out.PublisherPubKey); err != nil {
		return LiveSubscribeURI{}, fmt.Errorf("invalid publisher_pubkey")
	}
	if !isSeedHashHex(out.StreamID) {
		return LiveSubscribeURI{}, fmt.Errorf("invalid stream_id")
	}
	return out, nil
}

func clampLiveWindow(v uint32) int {
	if v == 0 {
		return defaultLiveWindowSize
	}
	if v > maxLiveWindowSize {
		return maxLiveWindowSize
	}
	return int(v)
}

func normalizeLiveSegmentRefs(in []LiveSegmentRef) []LiveSegmentRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]LiveSegmentRef, 0, len(in))
	seen := map[uint64]struct{}{}
	for _, it := range in {
		seedHash := strings.ToLower(strings.TrimSpace(it.SeedHash))
		if !isSeedHashHex(seedHash) {
			continue
		}
		if _, ok := seen[it.SegmentIndex]; ok {
			continue
		}
		seen[it.SegmentIndex] = struct{}{}
		out = append(out, LiveSegmentRef{SegmentIndex: it.SegmentIndex, SeedHash: seedHash, PublishedAtUnix: it.PublishedAtUnix})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SegmentIndex < out[j].SegmentIndex })
	return out
}

func trimLiveSegmentRefs(in []LiveSegmentRef, window int) []LiveSegmentRef {
	if len(in) <= window {
		return append([]LiveSegmentRef(nil), in...)
	}
	return append([]LiveSegmentRef(nil), in[len(in)-window:]...)
}

func liveSegmentRefsToPB(in []LiveSegmentRef) []*liveSegmentRefPB {
	if len(in) == 0 {
		return nil
	}
	out := make([]*liveSegmentRefPB, 0, len(in))
	for _, it := range in {
		out = append(out, &liveSegmentRefPB{SegmentIndex: it.SegmentIndex, SeedHash: it.SeedHash, PublishedAtUnix: it.PublishedAtUnix})
	}
	return out
}

func liveSegmentRefsFromPB(in []*liveSegmentRefPB) []LiveSegmentRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]LiveSegmentRef, 0, len(in))
	for _, it := range in {
		if it == nil {
			continue
		}
		out = append(out, LiveSegmentRef{SegmentIndex: it.SegmentIndex, SeedHash: it.SeedHash, PublishedAtUnix: it.PublishedAtUnix})
	}
	return normalizeLiveSegmentRefs(out)
}

func (lr *liveRuntime) registerPublishedSegments(streamID, publisherPubKey string, recent []LiveSegmentRef) []liveSubscriberTarget {
	if lr == nil {
		return nil
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	publisherPubKey = strings.ToLower(strings.TrimSpace(publisherPubKey))
	recent = normalizeLiveSegmentRefs(recent)
	if streamID == "" || publisherPubKey == "" {
		return nil
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	st, ok := lr.published[streamID]
	if !ok {
		st = &livePublishedStream{Subscribers: map[string]liveSubscriberTarget{}}
		lr.published[streamID] = st
	}
	st.PublisherPubKey = publisherPubKey
	st.RecentSegments = trimLiveSegmentRefs(recent, maxLiveWindowSize)
	for _, seg := range st.RecentSegments {
		lr.segmentMeta[seg.SeedHash] = liveSegmentMeta{
			StreamID:        streamID,
			SegmentIndex:    seg.SegmentIndex,
			PublishedAtUnix: seg.PublishedAtUnix,
		}
	}
	out := make([]liveSubscriberTarget, 0, len(st.Subscribers))
	for _, s := range st.Subscribers {
		out = append(out, s)
	}
	return out
}

func (lr *liveRuntime) subscribe(streamID string, target liveSubscriberTarget) (string, []LiveSegmentRef) {
	if lr == nil {
		return "", nil
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	target.PeerID = strings.TrimSpace(target.PeerID)
	if streamID == "" || target.PeerID == "" {
		return "", nil
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	st, ok := lr.published[streamID]
	if !ok {
		st = &livePublishedStream{Subscribers: map[string]liveSubscriberTarget{}}
		lr.published[streamID] = st
	}
	st.Subscribers[target.PeerID] = target
	return st.PublisherPubKey, trimLiveSegmentRefs(st.RecentSegments, target.Window)
}

func (lr *liveRuntime) storeReceived(streamID, publisherPubKey string, recent []LiveSegmentRef, updatedAt int64) {
	if lr == nil {
		return
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	publisherPubKey = strings.ToLower(strings.TrimSpace(publisherPubKey))
	recent = normalizeLiveSegmentRefs(recent)
	if streamID == "" || publisherPubKey == "" {
		return
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.received[streamID] = LiveSubscriberSnapshot{
		StreamID:        streamID,
		PublisherPubKey: publisherPubKey,
		RecentSegments:  trimLiveSegmentRefs(recent, maxLiveWindowSize),
		UpdatedAtUnix:   updatedAt,
	}
}

func (lr *liveRuntime) receivedSnapshot(streamID string) (LiveSubscriberSnapshot, bool) {
	if lr == nil {
		return LiveSubscriberSnapshot{}, false
	}
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	s, ok := lr.received[strings.ToLower(strings.TrimSpace(streamID))]
	if !ok {
		return LiveSubscriberSnapshot{}, false
	}
	s.RecentSegments = append([]LiveSegmentRef(nil), s.RecentSegments...)
	return s, true
}

func (lr *liveRuntime) segment(seedHash string) (liveSegmentMeta, bool) {
	if lr == nil {
		return liveSegmentMeta{}, false
	}
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	it, ok := lr.segmentMeta[strings.ToLower(strings.TrimSpace(seedHash))]
	return it, ok
}

func (lr *liveRuntime) publishedSnapshot(streamID string) (string, []LiveSegmentRef, bool) {
	if lr == nil {
		return "", nil, false
	}
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	st, ok := lr.published[strings.ToLower(strings.TrimSpace(streamID))]
	if !ok {
		return "", nil, false
	}
	return st.PublisherPubKey, append([]LiveSegmentRef(nil), st.RecentSegments...), true
}

func listLocalLiveQuoteSegments(db *sql.DB, streamID string, window int) ([]LiveQuoteSegment, uint64, error) {
	if db == nil {
		return nil, 0, fmt.Errorf("db not initialized")
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(streamID) {
		return nil, 0, fmt.Errorf("invalid stream_id")
	}
	rows, err := db.Query(`SELECT path,seed_hash,updated_at_unix FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix ASC, path ASC`,
		"%"+string(filepath.Separator)+"live"+string(filepath.Separator)+streamID+string(filepath.Separator)+"%")
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	type item struct {
		seg LiveQuoteSegment
		seq uint64
	}
	items := make([]item, 0, 16)
	var latest uint64
	for rows.Next() {
		var p, seedHash string
		var updatedAt int64
		if err := rows.Scan(&p, &seedHash, &updatedAt); err != nil {
			return nil, 0, err
		}
		segmentBytes, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		data, _, _, err := VerifyLiveSegment(segmentBytes)
		if err != nil {
			continue
		}
		if data.SegmentIndex > 0 && !strings.EqualFold(strings.TrimSpace(data.StreamID), streamID) {
			continue
		}
		seq := data.MediaSequence
		if seq == 0 {
			seq = data.SegmentIndex
		}
		if data.SegmentIndex > latest {
			latest = data.SegmentIndex
		}
		items = append(items, item{
			seg: LiveQuoteSegment{
				SegmentIndex: data.SegmentIndex,
				SeedHash:     strings.ToLower(strings.TrimSpace(seedHash)),
			},
			seq: seq,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].seq == items[j].seq {
			return items[i].seg.SegmentIndex < items[j].seg.SegmentIndex
		}
		return items[i].seq < items[j].seq
	})
	if window > 0 && len(items) > window {
		items = items[len(items)-window:]
	}
	out := make([]LiveQuoteSegment, 0, len(items))
	for _, it := range items {
		out = append(out, it.seg)
	}
	return out, latest, nil
}

func registerLiveHandlers(rt *Runtime) {
	if rt == nil || rt.Host == nil || rt.live == nil {
		return
	}
	h := rt.Host
	trace := rt.rpcTrace
	p2prpc.HandleProto[liveSubscribeReq, liveSubscribeResp](h, ProtoLiveSubscribe, clientSec(trace), func(_ context.Context, req liveSubscribeReq) (liveSubscribeResp, error) {
		streamID := strings.ToLower(strings.TrimSpace(req.StreamID))
		if streamID == "" && strings.TrimSpace(req.StreamURI) != "" {
			parsed, err := ParseLiveSubscribeURI(req.StreamURI)
			if err != nil {
				return liveSubscribeResp{}, err
			}
			streamID = parsed.StreamID
		}
		if !isSeedHashHex(streamID) || strings.TrimSpace(req.SubscriberPeerID) == "" {
			return liveSubscribeResp{}, fmt.Errorf("invalid params")
		}
		window := clampLiveWindow(req.Window)
		publisherPubKey, recent := rt.live.subscribe(streamID, liveSubscriberTarget{
			PeerID: strings.TrimSpace(req.SubscriberPeerID),
			Addrs:  append([]string(nil), req.SubscriberAddrs...),
			Window: window,
		})
		obs.Business("bitcast-client", "live_subscriber_registered", map[string]any{
			"stream_id":       streamID,
			"subscriber_peer": req.SubscriberPeerID,
			"recent_count":    len(recent),
		})
		return liveSubscribeResp{
			Status:          "subscribed",
			StreamID:        streamID,
			PublisherPubKey: publisherPubKey,
			RecentSegments:  liveSegmentRefsToPB(recent),
		}, nil
	})
	p2prpc.HandleProto[liveHeadPushReq, liveHeadPushResp](h, ProtoLiveHeadPush, clientSec(trace), func(_ context.Context, req liveHeadPushReq) (liveHeadPushResp, error) {
		streamID := strings.ToLower(strings.TrimSpace(req.StreamID))
		publisherPubKey := strings.ToLower(strings.TrimSpace(req.PublisherPubKey))
		if !isSeedHashHex(streamID) || publisherPubKey == "" {
			return liveHeadPushResp{}, fmt.Errorf("invalid params")
		}
		rt.live.storeReceived(streamID, publisherPubKey, liveSegmentRefsFromPB(req.RecentSegments), req.SentAtUnix)
		obs.Business("bitcast-client", "live_head_received", map[string]any{
			"stream_id":     streamID,
			"segment_count": len(req.RecentSegments),
		})
		return liveHeadPushResp{Status: "stored"}, nil
	})
	p2prpc.HandleProto[liveQuoteSubmitReq, liveQuoteSubmitResp](h, ProtoLiveQuoteSubmit, clientSec(trace), func(_ context.Context, req liveQuoteSubmitReq) (liveQuoteSubmitResp, error) {
		if rt.DB == nil || strings.TrimSpace(req.DemandID) == "" || strings.TrimSpace(req.SellerPeerID) == "" || !isSeedHashHex(strings.ToLower(strings.TrimSpace(req.StreamID))) || len(req.RecentSegments) == 0 {
			return liveQuoteSubmitResp{}, fmt.Errorf("invalid live quote")
		}
		recent := make([]LiveQuoteSegment, 0, len(req.RecentSegments))
		for _, seg := range req.RecentSegments {
			if seg == nil {
				continue
			}
			seedHash := strings.ToLower(strings.TrimSpace(seg.SeedHash))
			if !isSeedHashHex(seedHash) {
				continue
			}
			recent = append(recent, LiveQuoteSegment{SegmentIndex: seg.SegmentIndex, SeedHash: seedHash})
		}
		if len(recent) == 0 {
			return liveQuoteSubmitResp{}, fmt.Errorf("empty live quote segments")
		}
		recentJSON, err := json.Marshal(recent)
		if err != nil {
			return liveQuoteSubmitResp{}, err
		}
		if _, err := rt.DB.Exec(`INSERT INTO live_quotes(demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at_unix,created_at_unix)
			VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(demand_id,seller_pubkey_hex) DO UPDATE SET
				stream_id=excluded.stream_id,
				latest_segment_index=excluded.latest_segment_index,
				recent_segments_json=excluded.recent_segments_json,
				expires_at_unix=excluded.expires_at_unix,
				created_at_unix=excluded.created_at_unix`,
			strings.TrimSpace(req.DemandID),
			strings.ToLower(strings.TrimSpace(req.SellerPeerID)),
			strings.ToLower(strings.TrimSpace(req.StreamID)),
			req.LatestSegmentIndex,
			string(recentJSON),
			req.ExpiresAtUnix,
			time.Now().Unix(),
		); err != nil {
			return liveQuoteSubmitResp{}, err
		}
		return liveQuoteSubmitResp{Status: "stored"}, nil
	})
}

func BuildLiveSegment(ctx context.Context, rt *Runtime, data liveSegmentDataPB, mediaBytes []byte) ([]byte, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	_ = ctx
	if rt == nil || rt.Host == nil {
		return nil, "", fmt.Errorf("runtime not initialized")
	}
	data.PublisherPubKey = strings.ToLower(strings.TrimSpace(data.PublisherPubKey))
	if data.PublisherPubKey == "" {
		pubHex, err := localPubKeyHex(rt.Host)
		if err != nil {
			return nil, "", err
		}
		data.PublisherPubKey = strings.ToLower(strings.TrimSpace(pubHex))
	}
	data.StreamID = strings.ToLower(strings.TrimSpace(data.StreamID))
	data.PrevSeedHash = strings.ToLower(strings.TrimSpace(data.PrevSeedHash))
	data.InitSeedHash = strings.ToLower(strings.TrimSpace(data.InitSeedHash))
	data.MIMEType = strings.TrimSpace(data.MIMEType)
	data.PlaylistURIHint = strings.TrimSpace(data.PlaylistURIHint)
	if data.MediaSequence == 0 && data.SegmentIndex > 0 {
		data.MediaSequence = data.SegmentIndex
	}
	hash := sha256.Sum256(mediaBytes)
	data.MediaHash = append([]byte(nil), hash[:]...)
	rawData, err := oldproto.Marshal(&data)
	if err != nil {
		return nil, "", err
	}
	priv := rt.Host.Peerstore().PrivKey(rt.Host.ID())
	if priv == nil {
		return nil, "", fmt.Errorf("missing host private key")
	}
	sig, err := priv.Sign(rawData)
	if err != nil {
		return nil, "", err
	}
	seg := liveSegmentPB{
		Data:       rawData,
		MediaBytes: append([]byte(nil), mediaBytes...),
		Signature:  append([]byte(nil), sig...),
	}
	out, err := oldproto.Marshal(&seg)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(out)
	return out, hex.EncodeToString(sum[:]), nil
}

func VerifyLiveSegment(segmentBytes []byte) (liveSegmentDataPB, []byte, string, error) {
	var seg liveSegmentPB
	if err := oldproto.Unmarshal(segmentBytes, &seg); err != nil {
		return liveSegmentDataPB{}, nil, "", err
	}
	var data liveSegmentDataPB
	if err := oldproto.Unmarshal(seg.Data, &data); err != nil {
		return liveSegmentDataPB{}, nil, "", err
	}
	pubHex := strings.ToLower(strings.TrimSpace(data.PublisherPubKey))
	if pubHex == "" {
		return liveSegmentDataPB{}, nil, "", fmt.Errorf("publisher_pubkey required")
	}
	pubHex, err := normalizeCompressedPubKeyHex(pubHex)
	if err != nil {
		return liveSegmentDataPB{}, nil, "", fmt.Errorf("invalid publisher_pubkey")
	}
	rawPub, err := hex.DecodeString(pubHex)
	if err != nil {
		return liveSegmentDataPB{}, nil, "", fmt.Errorf("invalid publisher_pubkey")
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(rawPub)
	if err != nil {
		return liveSegmentDataPB{}, nil, "", err
	}
	ok, err := pub.Verify(seg.Data, seg.Signature)
	if err != nil {
		return liveSegmentDataPB{}, nil, "", err
	}
	if !ok {
		return liveSegmentDataPB{}, nil, "", fmt.Errorf("invalid signature")
	}
	data.StreamID = strings.ToLower(strings.TrimSpace(data.StreamID))
	data.PrevSeedHash = strings.ToLower(strings.TrimSpace(data.PrevSeedHash))
	data.InitSeedHash = strings.ToLower(strings.TrimSpace(data.InitSeedHash))
	data.MIMEType = strings.TrimSpace(data.MIMEType)
	data.PlaylistURIHint = strings.TrimSpace(data.PlaylistURIHint)
	hash := sha256.Sum256(seg.MediaBytes)
	if !strings.EqualFold(hex.EncodeToString(hash[:]), hex.EncodeToString(data.MediaHash)) {
		return liveSegmentDataPB{}, nil, "", fmt.Errorf("media_hash mismatch")
	}
	sum := sha256.Sum256(segmentBytes)
	return data, append([]byte(nil), seg.MediaBytes...), hex.EncodeToString(sum[:]), nil
}

type LivePublishParams struct {
	StreamID       string           `json:"stream_id"`
	RecentSegments []LiveSegmentRef `json:"recent_segments"`
	SubscriberPeer string           `json:"subscriber_peer,omitempty"`
}

type LiveSubscribeResult struct {
	StreamID        string           `json:"stream_id"`
	PublisherPubKey string           `json:"publisher_pubkey"`
	RecentSegments  []LiveSegmentRef `json:"recent_segments"`
}

func TriggerLiveSubscribe(ctx context.Context, rt *Runtime, rawURI string, window uint32) (LiveSubscribeResult, error) {
	if rt == nil || rt.Host == nil || rt.live == nil {
		return LiveSubscribeResult{}, fmt.Errorf("runtime not initialized")
	}
	parsed, err := ParseLiveSubscribeURI(rawURI)
	if err != nil {
		return LiveSubscribeResult{}, err
	}
	publisherID, err := peerIDFromClientID(parsed.PublisherPubKey)
	if err != nil {
		return LiveSubscribeResult{}, err
	}
	var resp liveSubscribeResp
	if err := p2prpc.CallProto(ctx, rt.Host, publisherID, ProtoLiveSubscribe, clientSec(rt.rpcTrace), liveSubscribeReq{
		StreamURI:        rawURI,
		StreamID:         parsed.StreamID,
		Window:           uint32(clampLiveWindow(window)),
		SubscriberPeerID: rt.Host.ID().String(),
		SubscriberAddrs:  localAdvertiseAddrs(rt),
	}, &resp); err != nil {
		return LiveSubscribeResult{}, err
	}
	recent := liveSegmentRefsFromPB(resp.RecentSegments)
	rt.live.storeReceived(parsed.StreamID, parsed.PublisherPubKey, recent, time.Now().Unix())
	return LiveSubscribeResult{
		StreamID:        parsed.StreamID,
		PublisherPubKey: parsed.PublisherPubKey,
		RecentSegments:  recent,
	}, nil
}

func TriggerLivePublishLatest(ctx context.Context, rt *Runtime, streamID string, recent []LiveSegmentRef) error {
	if rt == nil || rt.Host == nil || rt.live == nil {
		return fmt.Errorf("runtime not initialized")
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(streamID) {
		return fmt.Errorf("invalid stream_id")
	}
	pubHex, err := localPubKeyHex(rt.Host)
	if err != nil {
		return err
	}
	normalized := normalizeLiveSegmentRefs(recent)
	nowUnix := time.Now().Unix()
	for i := range normalized {
		if normalized[i].PublishedAtUnix == 0 {
			normalized[i].PublishedAtUnix = nowUnix
		}
	}
	targets := rt.live.registerPublishedSegments(streamID, pubHex, normalized)
	windowSize := defaultLiveWindowSize
	if rt.runIn.Live.Publish.BroadcastWindow > 0 {
		windowSize = clampLiveWindow(rt.runIn.Live.Publish.BroadcastWindow)
	}
	windowed := trimLiveSegmentRefs(normalized, windowSize)
	for _, target := range targets {
		subscriberID, err := peer.Decode(strings.TrimSpace(target.PeerID))
		if err != nil {
			continue
		}
		for _, raw := range target.Addrs {
			ai, err := parseAddr(strings.TrimSpace(raw))
			if err != nil || ai == nil {
				continue
			}
			rt.Host.Peerstore().AddAddrs(ai.ID, ai.Addrs, 10*time.Minute)
		}
		req := liveHeadPushReq{
			StreamID:        streamID,
			PublisherPubKey: strings.ToLower(strings.TrimSpace(pubHex)),
			RecentSegments:  liveSegmentRefsToPB(trimLiveSegmentRefs(windowed, target.Window)),
			SentAtUnix:      nowUnix,
		}
		var resp liveHeadPushResp
		if err := p2prpc.CallProto(ctx, rt.Host, subscriberID, ProtoLiveHeadPush, clientSec(rt.rpcTrace), req, &resp); err != nil {
			obs.Error("bitcast-client", "live_head_push_failed", map[string]any{
				"stream_id":         streamID,
				"transport_peer_id": target.PeerID,
				"error":             err.Error(),
			})
			continue
		}
	}
	return nil
}

func TriggerLiveGetLatest(rt *Runtime, streamID string) (LiveSubscriberSnapshot, error) {
	if rt == nil || rt.live == nil {
		return LiveSubscriberSnapshot{}, fmt.Errorf("runtime not initialized")
	}
	s, ok := rt.live.receivedSnapshot(streamID)
	if ok {
		return s, nil
	}
	publisherPubKey, recent, ok := rt.live.publishedSnapshot(streamID)
	if !ok {
		return LiveSubscriberSnapshot{}, fmt.Errorf("stream not found")
	}
	return LiveSubscriberSnapshot{
		StreamID:        strings.ToLower(strings.TrimSpace(streamID)),
		PublisherPubKey: publisherPubKey,
		RecentSegments:  recent,
		UpdatedAtUnix:   time.Now().Unix(),
	}, nil
}
