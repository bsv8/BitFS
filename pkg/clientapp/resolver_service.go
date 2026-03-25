package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const ProtoResolverResolve protocol.ID = "/bsv-transfer/client/resolver/resolve/1.0.0"

type resolverResolveReq struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name"`
}

type resolverResolveResp struct {
	Ok              bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok"`
	Code            string `protobuf:"bytes,2,opt,name=code,proto3" json:"code,omitempty"`
	Message         string `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	Name            string `protobuf:"bytes,4,opt,name=name,proto3" json:"name,omitempty"`
	TargetPubkeyHex string `protobuf:"bytes,5,opt,name=target_pubkey_hex,json=targetPubkeyHex,proto3" json:"target_pubkey_hex,omitempty"`
	UpdatedAtUnix   int64  `protobuf:"varint,6,opt,name=updated_at_unix,json=updatedAtUnix,proto3" json:"updated_at_unix,omitempty"`
}

type TriggerResolverResolveParams struct {
	ResolverPubkeyHex string
	Name              string
}

type resolverRecord struct {
	Name            string `json:"name"`
	TargetPubkeyHex string `json:"target_pubkey_hex"`
	UpdatedAtUnix   int64  `json:"updated_at_unix"`
}

// registerResolverHandlers 注册“名字 -> 目标节点公钥”的最小解析协议。
// 设计说明：
// - 这层只负责身份解析，不负责 route、不负责 get/post，也不直接返回 seed；
// - 解析服务本身仍是普通 node，寻址继续复用 node->gateway 目录能力；
// - 注册/续费/删除协议后续再做，本轮先用本地表 + admin 口子承载 resolver 数据。
func registerResolverHandlers(rt *Runtime) {
	if rt == nil || rt.Host == nil || rt.DB == nil {
		return
	}
	p2prpc.HandleProto[resolverResolveReq, resolverResolveResp](rt.Host, ProtoResolverResolve, clientSec(rt.rpcTrace), func(_ context.Context, req resolverResolveReq) (resolverResolveResp, error) {
		name, err := normalizeResolverNameCanonical(req.Name)
		if err != nil {
			return resolverResolveResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error()}, nil
		}
		record, found, err := loadResolverRecord(rt.DB, name)
		if err != nil {
			return resolverResolveResp{}, err
		}
		if !found {
			return resolverResolveResp{Ok: false, Code: "NOT_FOUND", Message: "resolver name not found"}, nil
		}
		return resolverResolveResp{
			Ok:              true,
			Code:            "OK",
			Name:            record.Name,
			TargetPubkeyHex: record.TargetPubkeyHex,
			UpdatedAtUnix:   record.UpdatedAtUnix,
		}, nil
	})
}

func TriggerResolverResolve(ctx context.Context, rt *Runtime, p TriggerResolverResolveParams) (resolverResolveResp, error) {
	var out resolverResolveResp
	if rt == nil || rt.Host == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	resolverPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.ResolverPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("resolver_pubkey_hex invalid: %w", err)
	}
	name, err := normalizeResolverNameCanonical(p.Name)
	if err != nil {
		return out, err
	}
	_, resolverPeerID, err := resolveClientTarget(resolverPubkeyHex)
	if err != nil {
		return out, err
	}
	if err := ensureTargetPeerReachable(ctx, rt, resolverPubkeyHex, resolverPeerID); err != nil {
		return out, err
	}
	err = p2prpc.CallProto(ctx, rt.Host, resolverPeerID, ProtoResolverResolve, clientSec(rt.rpcTrace), resolverResolveReq{
		Name: name,
	}, &out)
	return out, err
}

func normalizeResolverNameCanonical(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("resolver name is required")
	}
	if strings.Contains(value, "/") {
		return "", fmt.Errorf("resolver name must not contain /")
	}
	// 设计说明：
	// - 这里先落最小公共子集：trim + lowercase；
	// - ENS/ETH 完整规范化以后单独补，避免本轮把协议接口和规范细节绑死。
	value = strings.ToLower(value)
	return value, nil
}

func loadResolverRecord(db *sql.DB, name string) (resolverRecord, bool, error) {
	if db == nil {
		return resolverRecord{}, false, fmt.Errorf("db is nil")
	}
	var out resolverRecord
	err := db.QueryRow(
		`SELECT name,target_pubkey_hex,updated_at_unix FROM resolver_name_records WHERE name=?`,
		strings.TrimSpace(name),
	).Scan(&out.Name, &out.TargetPubkeyHex, &out.UpdatedAtUnix)
	if err == sql.ErrNoRows {
		return resolverRecord{}, false, nil
	}
	if err != nil {
		return resolverRecord{}, false, err
	}
	return out, true, nil
}

func listResolverRecords(db *sql.DB) ([]resolverRecord, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(`SELECT name,target_pubkey_hex,updated_at_unix FROM resolver_name_records ORDER BY updated_at_unix DESC,name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]resolverRecord, 0)
	for rows.Next() {
		var item resolverRecord
		if err := rows.Scan(&item.Name, &item.TargetPubkeyHex, &item.UpdatedAtUnix); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func upsertResolverRecord(db *sql.DB, rawName string, rawTargetPubkeyHex string) (resolverRecord, error) {
	if db == nil {
		return resolverRecord{}, fmt.Errorf("db is nil")
	}
	name, err := normalizeResolverNameCanonical(rawName)
	if err != nil {
		return resolverRecord{}, err
	}
	targetPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(rawTargetPubkeyHex))
	if err != nil {
		return resolverRecord{}, fmt.Errorf("target_pubkey_hex invalid: %w", err)
	}
	nowUnix := time.Now().Unix()
	_, err = db.Exec(
		`INSERT INTO resolver_name_records(name,target_pubkey_hex,updated_at_unix) VALUES(?,?,?)
		 ON CONFLICT(name) DO UPDATE SET
		 	target_pubkey_hex=excluded.target_pubkey_hex,
		 	updated_at_unix=excluded.updated_at_unix`,
		name,
		targetPubkeyHex,
		nowUnix,
	)
	if err != nil {
		return resolverRecord{}, err
	}
	return resolverRecord{
		Name:            name,
		TargetPubkeyHex: targetPubkeyHex,
		UpdatedAtUnix:   nowUnix,
	}, nil
}

func deleteResolverRecord(db *sql.DB, rawName string) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	name, err := normalizeResolverNameCanonical(rawName)
	if err != nil {
		return "", err
	}
	if _, err := db.Exec(`DELETE FROM resolver_name_records WHERE name=?`, name); err != nil {
		return "", err
	}
	return name, nil
}
