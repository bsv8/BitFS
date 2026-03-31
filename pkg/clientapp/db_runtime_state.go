package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	oldproto "github.com/golang/protobuf/proto"
)

func dbStoreInboxMessage(ctx context.Context, store *clientDB, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte) (ncall.CallResp, error) {
	if store == nil {
		return ncall.CallResp{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (ncall.CallResp, error) {
		now := time.Now().Unix()
		result, err := db.Exec(
			`INSERT INTO inbox_messages(message_id,sender_pubkey_hex,target_input,route,content_type,body_bytes,body_size_bytes,received_at_unix)
			 VALUES(?,?,?,?,?,?,?,?)`,
			strings.TrimSpace(messageID),
			strings.TrimSpace(senderPubKeyHex),
			strings.TrimSpace(targetInput),
			strings.TrimSpace(route),
			strings.TrimSpace(contentType),
			append([]byte(nil), body...),
			len(body),
			now,
		)
		var inboxID int64
		switch {
		case err == nil:
			inboxID, _ = result.LastInsertId()
		case strings.Contains(strings.ToLower(err.Error()), "unique constraint failed"):
			row := db.QueryRow(
				`SELECT id,received_at_unix FROM inbox_messages WHERE sender_pubkey_hex=? AND message_id=?`,
				strings.TrimSpace(senderPubKeyHex),
				strings.TrimSpace(messageID),
			)
			if scanErr := row.Scan(&inboxID, &now); scanErr != nil {
				return ncall.CallResp{}, scanErr
			}
		default:
			return ncall.CallResp{}, err
		}
		ack, err := oldproto.Marshal(&inboxReceipt{InboxMessageID: inboxID, ReceivedAtUnix: now})
		if err != nil {
			return ncall.CallResp{}, err
		}
		return ncall.CallResp{
			Ok:          true,
			Code:        "OK",
			ContentType: ncall.ContentTypeProto,
			Body:        ack,
		}, nil
	})
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
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`INSERT INTO live_follows(
			stream_id,stream_uri,publisher_pubkey,have_segment_index,last_bought_segment_index,last_bought_seed_hash,last_output_file_path,last_quote_seller_pubkey_hex,last_decision_json,status,last_error,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(stream_id) DO UPDATE SET
			stream_uri=excluded.stream_uri,
			publisher_pubkey=excluded.publisher_pubkey,
			have_segment_index=excluded.have_segment_index,
			last_bought_segment_index=excluded.last_bought_segment_index,
			last_bought_seed_hash=excluded.last_bought_seed_hash,
			last_output_file_path=excluded.last_output_file_path,
			last_quote_seller_pubkey_hex=excluded.last_quote_seller_pubkey_hex,
			last_decision_json=excluded.last_decision_json,
			status=excluded.status,
			last_error=excluded.last_error,
			updated_at_unix=excluded.updated_at_unix`,
			st.StreamID,
			strings.TrimSpace(st.StreamURI),
			st.PublisherPubKey,
			st.HaveSegmentIndex,
			st.LastBoughtSegmentIndex,
			st.LastBoughtSeedHash,
			strings.TrimSpace(st.LastOutputFilePath),
			st.LastQuoteSellerPeerID,
			decisionJSON,
			strings.TrimSpace(st.Status),
			strings.TrimSpace(st.LastError),
			st.UpdatedAtUnix,
		)
		return err
	})
}

func dbLoadLiveFollowStatus(ctx context.Context, store *clientDB, streamID string) (LiveFollowStatus, bool, error) {
	if store == nil {
		return LiveFollowStatus{}, false, nil
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (LiveFollowStatus, error) {
		var st LiveFollowStatus
		var decisionJSON string
		err := db.QueryRow(`SELECT stream_uri,publisher_pubkey,have_segment_index,last_bought_segment_index,last_bought_seed_hash,last_output_file_path,last_quote_seller_pubkey_hex,last_decision_json,status,last_error,updated_at_unix
			FROM live_follows WHERE stream_id=?`, strings.ToLower(strings.TrimSpace(streamID))).Scan(
			&st.StreamURI,
			&st.PublisherPubKey,
			&st.HaveSegmentIndex,
			&st.LastBoughtSegmentIndex,
			&st.LastBoughtSeedHash,
			&st.LastOutputFilePath,
			&st.LastQuoteSellerPeerID,
			&decisionJSON,
			&st.Status,
			&st.LastError,
			&st.UpdatedAtUnix,
		)
		if err != nil {
			return LiveFollowStatus{}, err
		}
		st.StreamID = strings.ToLower(strings.TrimSpace(streamID))
		if strings.TrimSpace(decisionJSON) != "" {
			_ = json.Unmarshal([]byte(decisionJSON), &st.LastDecision)
		}
		return normalizeLiveFollowStatus(st), nil
	})
	if err != nil {
		if err == sql.ErrNoRows {
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
	return clientDBValue(ctx, store, func(db *sql.DB) ([]LiveFollowStatus, error) {
		rows, err := db.Query(`SELECT stream_id,stream_uri,publisher_pubkey,have_segment_index,last_bought_segment_index,last_bought_seed_hash,last_output_file_path,last_quote_seller_pubkey_hex,last_decision_json,status,last_error,updated_at_unix
			FROM live_follows WHERE status='running' ORDER BY updated_at_unix ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]LiveFollowStatus, 0)
		for rows.Next() {
			var st LiveFollowStatus
			var decisionJSON string
			if err := rows.Scan(
				&st.StreamID,
				&st.StreamURI,
				&st.PublisherPubKey,
				&st.HaveSegmentIndex,
				&st.LastBoughtSegmentIndex,
				&st.LastBoughtSeedHash,
				&st.LastOutputFilePath,
				&st.LastQuoteSellerPeerID,
				&decisionJSON,
				&st.Status,
				&st.LastError,
				&st.UpdatedAtUnix,
			); err != nil {
				return nil, err
			}
			_ = json.Unmarshal([]byte(decisionJSON), &st.LastDecision)
			out = append(out, normalizeLiveFollowStatus(st))
		}
		if err := rows.Err(); err != nil {
			return nil, err
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
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (broadcastmodule.NodeReachabilityAnnouncement, error) {
		var (
			sourceGatewayPubkeyHex string
			headHeight             uint64
			seq                    uint64
			multiaddrsJSON         string
			publishedAtUnix        int64
			expiresAtUnix          int64
			signature              []byte
		)
		err := db.QueryRow(
			`SELECT source_gateway_pubkey_hex,head_height,seq,multiaddrs_json,published_at_unix,expires_at_unix,signature
			   FROM node_reachability_cache
			  WHERE target_node_pubkey_hex=? AND expires_at_unix>?`,
			targetNodePubkeyHex,
			nowUnix,
		).Scan(&sourceGatewayPubkeyHex, &headHeight, &seq, &multiaddrsJSON, &publishedAtUnix, &expiresAtUnix, &signature)
		if err != nil {
			return broadcastmodule.NodeReachabilityAnnouncement{}, err
		}
		addrs, err := unmarshalReachabilityStringList(multiaddrsJSON)
		if err != nil {
			return broadcastmodule.NodeReachabilityAnnouncement{}, err
		}
		return broadcastmodule.NodeReachabilityAnnouncement{
			NodePubkeyHex:   targetNodePubkeyHex,
			Multiaddrs:      addrs,
			HeadHeight:      headHeight,
			Seq:             seq,
			PublishedAtUnix: publishedAtUnix,
			ExpiresAtUnix:   expiresAtUnix,
			Signature:       append([]byte(nil), signature...),
		}, nil
	})
	if err != nil {
		if err == sql.ErrNoRows {
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
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		_, err := db.Exec(
			`INSERT INTO node_reachability_cache(
				target_node_pubkey_hex,source_gateway_pubkey_hex,head_height,seq,multiaddrs_json,published_at_unix,expires_at_unix,signature,updated_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(target_node_pubkey_hex) DO UPDATE SET
				source_gateway_pubkey_hex=excluded.source_gateway_pubkey_hex,
				head_height=excluded.head_height,
				seq=excluded.seq,
				multiaddrs_json=excluded.multiaddrs_json,
				published_at_unix=excluded.published_at_unix,
				expires_at_unix=excluded.expires_at_unix,
				signature=excluded.signature,
				updated_at_unix=excluded.updated_at_unix`,
			ann.NodePubkeyHex,
			strings.ToLower(strings.TrimSpace(sourceGatewayPubkeyHex)),
			ann.HeadHeight,
			ann.Seq,
			multiaddrsJSON,
			ann.PublishedAtUnix,
			ann.ExpiresAtUnix,
			append([]byte(nil), ann.Signature...),
			now,
		)
		return err
	})
}

func dbSaveSelfNodeReachabilityState(ctx context.Context, store *clientDB, state selfNodeReachabilityState) error {
	if store == nil {
		return nil
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO self_node_reachability_state(node_pubkey_hex,head_height,seq,updated_at_unix) VALUES(?,?,?,?)
			 ON CONFLICT(node_pubkey_hex) DO UPDATE SET
				head_height=excluded.head_height,
				seq=excluded.seq,
				updated_at_unix=excluded.updated_at_unix`,
			strings.ToLower(strings.TrimSpace(state.NodePubkeyHex)),
			state.HeadHeight,
			state.Seq,
			time.Now().Unix(),
		)
		return err
	})
}

func dbLoadSelfNodeReachabilityState(ctx context.Context, store *clientDB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	if store == nil {
		return selfNodeReachabilityState{}, false, nil
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (selfNodeReachabilityState, error) {
		var out selfNodeReachabilityState
		err := db.QueryRow(`SELECT node_pubkey_hex,head_height,seq FROM self_node_reachability_state WHERE node_pubkey_hex=?`, strings.ToLower(strings.TrimSpace(nodePubkeyHex))).Scan(
			&out.NodePubkeyHex,
			&out.HeadHeight,
			&out.Seq,
		)
		if err != nil {
			return selfNodeReachabilityState{}, err
		}
		return out, nil
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return selfNodeReachabilityState{}, false, nil
		}
		return selfNodeReachabilityState{}, false, err
	}
	return out, true, nil
}
