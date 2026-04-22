package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/proclivefollows"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procnodereachabilitycache"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procselfnodereachabilitystate"
)

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

func dbLoadCachedNodeReachability(ctx context.Context, store *clientDB, targetNodePubkeyHex string, nowUnix int64) (broadcastmodule.NodeReachabilityAnnouncement, bool, error) {
	if store == nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, nil
	}
	targetNodePubkeyHex, err := normalizeCompressedPubKeyHex(targetNodePubkeyHex)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, err
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (broadcastmodule.NodeReachabilityAnnouncement, error) {
		node, err := root.ProcNodeReachabilityCache.Query().
			Where(
				procnodereachabilitycache.TargetNodePubkeyHexEQ(targetNodePubkeyHex),
				procnodereachabilitycache.ExpiresAtUnixGT(nowUnix),
			).
			Only(ctx)
		if err != nil {
			return broadcastmodule.NodeReachabilityAnnouncement{}, err
		}
		addrs, err := unmarshalReachabilityStringList(node.MultiaddrsJSON)
		if err != nil {
			return broadcastmodule.NodeReachabilityAnnouncement{}, err
		}
		return broadcastmodule.NodeReachabilityAnnouncement{
			NodePubkeyHex:   targetNodePubkeyHex,
			Multiaddrs:      addrs,
			HeadHeight:      uint64(node.HeadHeight),
			Seq:             uint64(node.Seq),
			PublishedAtUnix: node.PublishedAtUnix,
			ExpiresAtUnix:   node.ExpiresAtUnix,
			Signature:       append([]byte(nil), node.Signature...),
		}, nil
	})
	if err != nil {
		if gen.IsNotFound(err) || err == sql.ErrNoRows {
			return broadcastmodule.NodeReachabilityAnnouncement{}, false, nil
		}
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, err
	}
	return out, true, nil
}

func dbSaveNodeReachabilityCache(ctx context.Context, store *clientDB, sourceGatewayPubkeyHex string, ann broadcastmodule.NodeReachabilityAnnouncement) error {
	if store == nil {
		return nil
	}
	ann.NodePubkeyHex = strings.ToLower(strings.TrimSpace(ann.NodePubkeyHex))
	multiaddrsJSON, err := marshalReachabilityStringList(ann.Multiaddrs)
	if err != nil {
		return err
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		now := time.Now().Unix()
		existing, err := tx.ProcNodeReachabilityCache.Query().
			Where(procnodereachabilitycache.TargetNodePubkeyHexEQ(ann.NodePubkeyHex)).
			Only(ctx)
		if err != nil {
			if !gen.IsNotFound(err) {
				return err
			}
			_, err = tx.ProcNodeReachabilityCache.Create().
				SetTargetNodePubkeyHex(ann.NodePubkeyHex).
				SetSourceGatewayPubkeyHex(strings.ToLower(strings.TrimSpace(sourceGatewayPubkeyHex))).
				SetHeadHeight(int64(ann.HeadHeight)).
				SetSeq(int64(ann.Seq)).
				SetMultiaddrsJSON(multiaddrsJSON).
				SetPublishedAtUnix(ann.PublishedAtUnix).
				SetExpiresAtUnix(ann.ExpiresAtUnix).
				SetSignature(append([]byte(nil), ann.Signature...)).
				SetUpdatedAtUnix(now).
				Save(ctx)
			return err
		}
		_, err = existing.Update().
			SetSourceGatewayPubkeyHex(strings.ToLower(strings.TrimSpace(sourceGatewayPubkeyHex))).
			SetHeadHeight(int64(ann.HeadHeight)).
			SetSeq(int64(ann.Seq)).
			SetMultiaddrsJSON(multiaddrsJSON).
			SetPublishedAtUnix(ann.PublishedAtUnix).
			SetExpiresAtUnix(ann.ExpiresAtUnix).
			SetSignature(append([]byte(nil), ann.Signature...)).
			SetUpdatedAtUnix(now).
			Save(ctx)
		return err
	})
}

func dbSaveSelfNodeReachabilityState(ctx context.Context, store *clientDB, state selfNodeReachabilityState) error {
	if store == nil {
		return nil
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		now := time.Now().Unix()
		nodePubkeyHex := strings.ToLower(strings.TrimSpace(state.NodePubkeyHex))
		existing, err := tx.ProcSelfNodeReachabilityState.Query().
			Where(procselfnodereachabilitystate.NodePubkeyHexEQ(nodePubkeyHex)).
			Only(ctx)
		if err != nil {
			if !gen.IsNotFound(err) {
				return err
			}
			_, err = tx.ProcSelfNodeReachabilityState.Create().
				SetNodePubkeyHex(nodePubkeyHex).
				SetHeadHeight(int64(state.HeadHeight)).
				SetSeq(int64(state.Seq)).
				SetUpdatedAtUnix(now).
				Save(ctx)
			return err
		}
		_, err = existing.Update().
			SetHeadHeight(int64(state.HeadHeight)).
			SetSeq(int64(state.Seq)).
			SetUpdatedAtUnix(now).
			Save(ctx)
		return err
	})
}

func dbLoadSelfNodeReachabilityState(ctx context.Context, store *clientDB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	if store == nil {
		return selfNodeReachabilityState{}, false, nil
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (selfNodeReachabilityState, error) {
		node, err := root.ProcSelfNodeReachabilityState.Query().
			Where(procselfnodereachabilitystate.NodePubkeyHexEQ(strings.ToLower(strings.TrimSpace(nodePubkeyHex)))).
			Only(ctx)
		if err != nil {
			return selfNodeReachabilityState{}, err
		}
		return selfNodeReachabilityState{
			NodePubkeyHex: node.NodePubkeyHex,
			HeadHeight:    uint64(node.HeadHeight),
			Seq:           uint64(node.Seq),
		}, nil
	})
	if err != nil {
		if gen.IsNotFound(err) || err == sql.ErrNoRows {
			return selfNodeReachabilityState{}, false, nil
		}
		return selfNodeReachabilityState{}, false, err
	}
	return out, true, nil
}
