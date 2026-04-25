package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/proclivefollows"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
)

// selfNodeReachabilityState 是本节点可达性状态的兼容结构。
// 说明：真正的持久化能力收在 gatewayclient 模块，root 这里只保留薄封装。
type selfNodeReachabilityState struct {
	NodePubkeyHex string
	HeadHeight    uint64
	Seq           uint64
}

// dbLoadCachedNodeReachability 读取节点可达性缓存。
// 设计说明：
// - 缓存事实归 gatewayclient 模块所有，root 这里只保留兼容入口；
// - 这里会做过期判断，过期的缓存直接视为不存在；
// - 上层如果要强制刷新，应走模块查询链，不要依赖这个缓存入口。
func dbLoadCachedNodeReachability(ctx context.Context, store *clientDB, targetNodePubkeyHex string, nowUnix int64) (struct {
	NodePubkeyHex   string
	Multiaddrs      []string
	HeadHeight      uint64
	Seq             uint64
	PublishedAtUnix int64
	ExpiresAtUnix   int64
	Signature       []byte
}, bool, error) {
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return struct {
			NodePubkeyHex   string
			Multiaddrs      []string
			HeadHeight      uint64
			Seq             uint64
			PublishedAtUnix int64
			ExpiresAtUnix   int64
			Signature       []byte
		}{}, false, err
	}
	cache, found, err := gw.GetNodeReachabilityCache(ctx, targetNodePubkeyHex)
	if err != nil || !found {
		return struct {
			NodePubkeyHex   string
			Multiaddrs      []string
			HeadHeight      uint64
			Seq             uint64
			PublishedAtUnix int64
			ExpiresAtUnix   int64
			Signature       []byte
		}{}, found, err
	}
	if nowUnix > 0 && cache.ExpiresAtUnix > 0 && cache.ExpiresAtUnix <= nowUnix {
		return struct {
			NodePubkeyHex   string
			Multiaddrs      []string
			HeadHeight      uint64
			Seq             uint64
			PublishedAtUnix int64
			ExpiresAtUnix   int64
			Signature       []byte
		}{}, false, nil
	}
	return struct {
		NodePubkeyHex   string
		Multiaddrs      []string
		HeadHeight      uint64
		Seq             uint64
		PublishedAtUnix int64
		ExpiresAtUnix   int64
		Signature       []byte
	}{
		NodePubkeyHex:   cache.NodePubkeyHex,
		Multiaddrs:      append([]string(nil), cache.Multiaddrs...),
		HeadHeight:      cache.HeadHeight,
		Seq:             cache.Seq,
		PublishedAtUnix: cache.PublishedAtUnix,
		ExpiresAtUnix:   cache.ExpiresAtUnix,
		Signature:       append([]byte(nil), cache.Signature...),
	}, true, nil
}

// dbSaveNodeReachabilityCache 保存节点可达性缓存。
// 设计说明：缓存写入也必须经过 gatewayclient 模块 store，避免 root 自己再写一份表逻辑。
func dbSaveNodeReachabilityCache(ctx context.Context, store *clientDB, sourceGatewayPubkeyHex string, ann struct {
	NodePubkeyHex   string
	Multiaddrs      []string
	HeadHeight      uint64
	Seq             uint64
	PublishedAtUnix int64
	ExpiresAtUnix   int64
	Signature       []byte
}) error {
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return err
	}
	return gw.SaveNodeReachabilityCache(ctx, gatewayclient.NodeReachabilityCache{
		NodePubkeyHex:          ann.NodePubkeyHex,
		Multiaddrs:             append([]string(nil), ann.Multiaddrs...),
		HeadHeight:             ann.HeadHeight,
		Seq:                    ann.Seq,
		PublishedAtUnix:        ann.PublishedAtUnix,
		ExpiresAtUnix:          ann.ExpiresAtUnix,
		Signature:              append([]byte(nil), ann.Signature...),
		SourceGatewayPubkeyHex: strings.ToLower(strings.TrimSpace(sourceGatewayPubkeyHex)),
		UpdatedAtUnix:          time.Now().Unix(),
	})
}

// dbSaveSelfNodeReachabilityState 保存本节点可达性状态。
// 设计说明：本节点状态属于 gatewayclient 模块事实，不再让 root 直接落表。
func dbSaveSelfNodeReachabilityState(ctx context.Context, store *clientDB, state selfNodeReachabilityState) error {
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return err
	}
	return gw.SaveSelfNodeReachabilityState(ctx, gatewayclient.SelfNodeReachabilityState{
		NodePubkeyHex: strings.TrimSpace(state.NodePubkeyHex),
		HeadHeight:    state.HeadHeight,
		Seq:           state.Seq,
		UpdatedAtUnix: time.Now().Unix(),
	})
}

// dbLoadSelfNodeReachabilityState 读取本节点可达性状态。
// 设计说明：这里只做兼容读取，不再返回静默空壳。
func dbLoadSelfNodeReachabilityState(ctx context.Context, store *clientDB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return selfNodeReachabilityState{}, false, err
	}
	state, found, err := gw.GetSelfNodeReachabilityState(ctx, nodePubkeyHex)
	if err != nil || !found {
		return selfNodeReachabilityState{}, found, err
	}
	return selfNodeReachabilityState{
		NodePubkeyHex: state.NodePubkeyHex,
		HeadHeight:    state.HeadHeight,
		Seq:           state.Seq,
	}, true, nil
}

func dbPersistLiveFollowStatus(ctx context.Context, store *clientDB, st LiveFollowStatus) error {
	if store == nil {
		return nil
	}
	st = normalizeLiveFollowStatus(st)
	decisionJSON := "{}"
	if b, err := json.Marshal(st.LastDecision); err == nil {
		decisionJSON = string(b)
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		st.StreamID = strings.TrimSpace(st.StreamID)
		existing, err := tx.ProcLiveFollows.Query().Where(proclivefollows.StreamIDEQ(st.StreamID)).Only(ctx)
		if err != nil {
			if !gen.IsNotFound(err) {
				return err
			}
			_, err = tx.ProcLiveFollows.Create().
				SetStreamID(st.StreamID).
				SetStreamURI(strings.TrimSpace(st.StreamURI)).
				SetPublisherPubkey(st.PublisherPubKey).
				SetHaveSegmentIndex(int64(st.HaveSegmentIndex)).
				SetLastBoughtSegmentIndex(int64(st.LastBoughtSegmentIndex)).
				SetLastBoughtSeedHash(st.LastBoughtSeedHash).
				SetLastOutputFilePath(strings.TrimSpace(st.LastOutputFilePath)).
				SetLastQuoteSellerPubkeyHex(st.LastQuoteSellerPubHex).
				SetLastDecisionJSON(decisionJSON).
				SetStatus(strings.TrimSpace(st.Status)).
				SetLastError(strings.TrimSpace(st.LastError)).
				SetUpdatedAtUnix(st.UpdatedAtUnix).
				Save(ctx)
			return err
		}
		_, err = existing.Update().
			SetStreamURI(strings.TrimSpace(st.StreamURI)).
			SetPublisherPubkey(st.PublisherPubKey).
			SetHaveSegmentIndex(int64(st.HaveSegmentIndex)).
			SetLastBoughtSegmentIndex(int64(st.LastBoughtSegmentIndex)).
			SetLastBoughtSeedHash(st.LastBoughtSeedHash).
			SetLastOutputFilePath(strings.TrimSpace(st.LastOutputFilePath)).
			SetLastQuoteSellerPubkeyHex(st.LastQuoteSellerPubHex).
			SetLastDecisionJSON(decisionJSON).
			SetStatus(strings.TrimSpace(st.Status)).
			SetLastError(strings.TrimSpace(st.LastError)).
			SetUpdatedAtUnix(st.UpdatedAtUnix).
			Save(ctx)
		return err
	})
}

func dbLoadLiveFollowStatus(ctx context.Context, store *clientDB, streamID string) (LiveFollowStatus, bool, error) {
	if store == nil {
		return LiveFollowStatus{}, false, nil
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (LiveFollowStatus, error) {
		node, err := root.ProcLiveFollows.Query().Where(proclivefollows.StreamIDEQ(strings.ToLower(strings.TrimSpace(streamID)))).Only(ctx)
		if err != nil {
			return LiveFollowStatus{}, err
		}
		st := LiveFollowStatus{
			StreamID:               node.StreamID,
			StreamURI:              node.StreamURI,
			PublisherPubKey:        node.PublisherPubkey,
			HaveSegmentIndex:       node.HaveSegmentIndex,
			LastBoughtSegmentIndex: uint64(node.LastBoughtSegmentIndex),
			LastBoughtSeedHash:     node.LastBoughtSeedHash,
			LastOutputFilePath:     node.LastOutputFilePath,
			LastQuoteSellerPubHex:  node.LastQuoteSellerPubkeyHex,
			Status:                 node.Status,
			LastError:              node.LastError,
			UpdatedAtUnix:          node.UpdatedAtUnix,
		}
		if strings.TrimSpace(node.LastDecisionJSON) != "" {
			_ = json.Unmarshal([]byte(node.LastDecisionJSON), &st.LastDecision)
		}
		return normalizeLiveFollowStatus(st), nil
	})
	if err != nil {
		if gen.IsNotFound(err) || err == sql.ErrNoRows {
			return LiveFollowStatus{}, false, nil
		}
		return LiveFollowStatus{}, false, err
	}
	return out, true, nil
}

func dbListRunningLiveFollowStatuses(ctx context.Context, store *clientDB) ([]LiveFollowStatus, error) {
	if store == nil {
		return nil, nil
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]LiveFollowStatus, error) {
		nodes, err := root.ProcLiveFollows.Query().
			Where(proclivefollows.StatusEQ("running")).
			Order(proclivefollows.ByUpdatedAtUnix()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]LiveFollowStatus, 0, len(nodes))
		for _, node := range nodes {
			st := LiveFollowStatus{
				StreamID:               node.StreamID,
				StreamURI:              node.StreamURI,
				PublisherPubKey:        node.PublisherPubkey,
				HaveSegmentIndex:       node.HaveSegmentIndex,
				LastBoughtSegmentIndex: uint64(node.LastBoughtSegmentIndex),
				LastBoughtSeedHash:     node.LastBoughtSeedHash,
				LastOutputFilePath:     node.LastOutputFilePath,
				LastQuoteSellerPubHex:  node.LastQuoteSellerPubkeyHex,
				Status:                 node.Status,
				LastError:              node.LastError,
				UpdatedAtUnix:          node.UpdatedAtUnix,
			}
			if strings.TrimSpace(node.LastDecisionJSON) != "" {
				_ = json.Unmarshal([]byte(node.LastDecisionJSON), &st.LastDecision)
			}
			out = append(out, normalizeLiveFollowStatus(st))
		}
		return out, nil
	})
}
