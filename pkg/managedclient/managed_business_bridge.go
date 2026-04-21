package managedclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func (d *managedDaemon) executeManagedBusinessControlCommand(req controlCommandRequest) (controlCommandResult, error) {
	rt := d.currentRuntime()
	if rt == nil {
		return controlCommandResult{
			CommandID: req.CommandID,
			Action:    req.Action,
			Result:    "failed",
			Error:     "runtime not initialized",
		}, nil
	}
	ctx := d.rootCtx
	if ctx == nil {
		return controlCommandResult{
			CommandID: req.CommandID,
			Action:    req.Action,
			Result:    "failed",
			Error:     "runtime context is not ready",
		}, nil
	}
	store := rt.DB()
	if store == nil {
		return businessActionFailure(req, d, "client db is nil", nil), nil
	}

	switch req.Action {
	case controlActionWalletPayBSV:
		toAddress := strings.TrimSpace(controlCommandPayloadString(req.Payload, "to_address"))
		if toAddress == "" {
			return businessActionFailure(req, d, "to_address is required", nil), nil
		}
		amountSatoshi, ok := pricingPayloadUint64(req.Payload, "amount_satoshi")
		if !ok || amountSatoshi == 0 {
			return businessActionFailure(req, d, "amount_satoshi is required", nil), nil
		}
		result, err := clientapp.TriggerWalletBSVTransfer(ctx, store, rt, clientapp.WalletBSVTransferRequest{
			ToAddress:     toAddress,
			AmountSatoshi: amountSatoshi,
		})
		payload := map[string]any{"wallet_transfer_result": result}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		if !result.Ok {
			msg := strings.TrimSpace(result.Message)
			if msg == "" {
				msg = "wallet bsv transfer failed"
			}
			return businessActionFailure(req, d, msg, payload), nil
		}
		return businessActionSuccess(req, "submitted", payload, d), nil

	case controlActionWalletTokenSendPreview:
		result, err := clientapp.TriggerWalletTokenSendPreview(ctx, store, rt, clientapp.WalletTokenSendPreviewRequest{
			TokenStandard: strings.TrimSpace(controlCommandPayloadString(req.Payload, "token_standard")),
			AssetKey:      strings.TrimSpace(controlCommandPayloadString(req.Payload, "asset_key")),
			AmountText:    strings.TrimSpace(controlCommandPayloadString(req.Payload, "amount_text")),
			ToAddress:     strings.TrimSpace(controlCommandPayloadString(req.Payload, "to_address")),
		})
		payload := map[string]any{
			"wallet_token_send_preview_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		if !result.Ok {
			msg := strings.TrimSpace(result.Message)
			if msg == "" {
				msg = "wallet token send preview failed"
			}
			return businessActionFailure(req, d, msg, payload), nil
		}
		return businessActionSuccess(req, "previewed", payload, d), nil

	case controlActionWalletTokenSendSign:
		result, err := clientapp.TriggerWalletTokenSendSign(ctx, store, rt, clientapp.WalletTokenSendSignRequest{
			TokenStandard:       strings.TrimSpace(controlCommandPayloadString(req.Payload, "token_standard")),
			AssetKey:            strings.TrimSpace(controlCommandPayloadString(req.Payload, "asset_key")),
			AmountText:          strings.TrimSpace(controlCommandPayloadString(req.Payload, "amount_text")),
			ToAddress:           strings.TrimSpace(controlCommandPayloadString(req.Payload, "to_address")),
			ExpectedPreviewHash: strings.TrimSpace(controlCommandPayloadString(req.Payload, "expected_preview_hash")),
		})
		payload := map[string]any{
			"wallet_token_send_sign_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		if !result.Ok {
			msg := strings.TrimSpace(result.Message)
			if msg == "" {
				msg = "wallet token send sign failed"
			}
			return businessActionFailure(req, d, msg, payload), nil
		}
		return businessActionSuccess(req, "signed", payload, d), nil

	case controlActionWalletTokenSendSubmit:
		result, err := clientapp.TriggerWalletTokenSendSubmit(ctx, store, rt, clientapp.WalletAssetActionSubmitRequest{
			SignedTxHex: strings.TrimSpace(controlCommandPayloadString(req.Payload, "signed_tx_hex")),
		})
		payload := map[string]any{
			"wallet_token_send_submit_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		if !result.Ok {
			msg := strings.TrimSpace(result.Message)
			if msg == "" {
				msg = "wallet token send submit failed"
			}
			return businessActionFailure(req, d, msg, payload), nil
		}
		return businessActionSuccess(req, "submitted", payload, d), nil

	case controlActionGatewayPublishDemand:
		seedHash := strings.TrimSpace(controlCommandPayloadString(req.Payload, "seed_hash"))
		chunkCount, ok := pricingPayloadUint32(req.Payload, "chunk_count")
		if seedHash == "" || !ok || chunkCount == 0 {
			return businessActionFailure(req, d, "seed_hash and chunk_count are required", nil), nil
		}
		result, err := clientapp.TriggerGatewayPublishDemand(ctx, store, rt, clientapp.PublishDemandParams{
			SeedHash:      seedHash,
			ChunkCount:    chunkCount,
			GatewayPeerID: strings.TrimSpace(controlCommandPayloadString(req.Payload, "gateway_pubkey_hex")),
		})
		payload := map[string]any{
			"gateway_publish_demand_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "published", payload, d), nil

	case controlActionGatewayPublishDemandBatch:
		items, err := businessPayloadDemandBatchItems(req.Payload)
		if err != nil {
			return businessActionFailure(req, d, err.Error(), nil), nil
		}
		result, err := clientapp.TriggerGatewayPublishDemandBatch(ctx, store, rt, clientapp.PublishDemandBatchParams{
			Items:         items,
			GatewayPeerID: strings.TrimSpace(controlCommandPayloadString(req.Payload, "gateway_pubkey_hex")),
		})
		payload := map[string]any{
			"gateway_publish_demand_batch_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "published", payload, d), nil

	case controlActionGatewayPublishLiveDemand:
		streamID := strings.TrimSpace(controlCommandPayloadString(req.Payload, "stream_id"))
		if streamID == "" {
			return businessActionFailure(req, d, "stream_id is required", nil), nil
		}
		haveSegmentIndex, _ := pricingPayloadInt64(req.Payload, "have_segment_index")
		window, _ := pricingPayloadUint32(req.Payload, "window")
		result, err := clientapp.TriggerGatewayPublishLiveDemand(ctx, store, rt, clientapp.PublishLiveDemandParams{
			StreamID:         streamID,
			HaveSegmentIndex: haveSegmentIndex,
			Window:           window,
			GatewayPeerID:    strings.TrimSpace(controlCommandPayloadString(req.Payload, "gateway_pubkey_hex")),
		})
		payload := map[string]any{
			"gateway_publish_live_demand_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "published", payload, d), nil

	case controlActionGatewayPublishDemandChainTxQuotePay:
		seedHash := strings.TrimSpace(controlCommandPayloadString(req.Payload, "seed_hash"))
		chunkCount, ok := pricingPayloadUint32(req.Payload, "chunk_count")
		if seedHash == "" || !ok || chunkCount == 0 {
			return businessActionFailure(req, d, "seed_hash and chunk_count are required", nil), nil
		}
		result, err := clientapp.TriggerGatewayDemandPublishChainTxQuotePay(ctx, store, rt, clientapp.PublishDemandParams{
			SeedHash:      seedHash,
			ChunkCount:    chunkCount,
			GatewayPeerID: strings.TrimSpace(controlCommandPayloadString(req.Payload, "gateway_pubkey_hex")),
		})
		payload := map[string]any{
			"gateway_publish_demand_chain_tx_quote_pay_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "published", payload, d), nil

	case controlActionGatewayReachabilityAnnounce:
		ttlSeconds, ok := pricingPayloadUint32(req.Payload, "ttl_seconds")
		if !ok || ttlSeconds == 0 {
			return businessActionFailure(req, d, "ttl_seconds is required", nil), nil
		}
		result, err := clientapp.TriggerGatewayAnnounceNodeReachability(ctx, store, rt, clientapp.AnnounceNodeReachabilityParams{
			TTLSeconds:    ttlSeconds,
			GatewayPeerID: strings.TrimSpace(controlCommandPayloadString(req.Payload, "gateway_pubkey_hex")),
		})
		payload := map[string]any{
			"gateway_reachability_announce_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "announced", payload, d), nil

	case controlActionGatewayReachabilityQuery:
		targetNodePubkeyHex := strings.TrimSpace(controlCommandPayloadString(req.Payload, "target_node_pubkey_hex"))
		if targetNodePubkeyHex == "" {
			return businessActionFailure(req, d, "target_node_pubkey_hex is required", nil), nil
		}
		result, err := clientapp.TriggerGatewayQueryNodeReachability(ctx, store, rt, clientapp.QueryNodeReachabilityParams{
			TargetNodePubkeyHex: targetNodePubkeyHex,
			GatewayPeerID:       strings.TrimSpace(controlCommandPayloadString(req.Payload, "gateway_pubkey_hex")),
		})
		payload := map[string]any{
			"gateway_reachability_query_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "queried", payload, d), nil

	case controlActionDomainResolve:
		domain := strings.TrimSpace(controlCommandPayloadString(req.Payload, "domain"))
		if domain == "" {
			return businessActionFailure(req, d, "domain is required", nil), nil
		}
		pubkeyHex, err := clientapp.ResolveDomainToPubkey(ctx, rt, domain)
		payload := map[string]any{
			"domain_resolve_result": map[string]any{
				"domain":     domain,
				"pubkey_hex": pubkeyHex,
			},
		}
		if err != nil {
			code := clientapp.ModuleHookCodeOf(err)
			msg := clientapp.ModuleHookMessageOf(err)
			if msg == "" {
				msg = err.Error()
			}
			return businessActionFailure(req, d, fmt.Sprintf("%s: %s", code, msg), payload), nil
		}
		return businessActionSuccess(req, "resolved", payload, d), nil

	case controlActionDomainRegister:
		resolverPubkeyHex := strings.TrimSpace(controlCommandPayloadString(req.Payload, "resolver_pubkey_hex"))
		name := strings.TrimSpace(controlCommandPayloadString(req.Payload, "name"))
		targetPubkeyHex := strings.TrimSpace(controlCommandPayloadString(req.Payload, "target_pubkey_hex"))
		if resolverPubkeyHex == "" || name == "" || targetPubkeyHex == "" {
			return businessActionFailure(req, d, "resolver_pubkey_hex, name and target_pubkey_hex are required", nil), nil
		}
		result, err := clientapp.TriggerDomainRegisterName(ctx, store, rt, clientapp.TriggerDomainRegisterNameParams{
			ResolverPubkeyHex: resolverPubkeyHex,
			ResolverAddr:      strings.TrimSpace(controlCommandPayloadString(req.Payload, "resolver_addr")),
			Name:              name,
			TargetPubkeyHex:   targetPubkeyHex,
		})
		payload := map[string]any{
			"domain_register_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		if !result.Ok {
			msg := strings.TrimSpace(result.Message)
			if msg == "" {
				msg = "domain register failed"
			}
			return businessActionFailure(req, d, msg, payload), nil
		}
		return businessActionSuccess(req, "registered", payload, d), nil

	case controlActionDomainSetTarget:
		resolverPubkeyHex := strings.TrimSpace(controlCommandPayloadString(req.Payload, "resolver_pubkey_hex"))
		name := strings.TrimSpace(controlCommandPayloadString(req.Payload, "name"))
		targetPubkeyHex := strings.TrimSpace(controlCommandPayloadString(req.Payload, "target_pubkey_hex"))
		if resolverPubkeyHex == "" || name == "" || targetPubkeyHex == "" {
			return businessActionFailure(req, d, "resolver_pubkey_hex, name and target_pubkey_hex are required", nil), nil
		}
		result, err := clientapp.TriggerDomainSetTarget(ctx, store, rt, clientapp.TriggerDomainSetTargetParams{
			ResolverPubkeyHex: resolverPubkeyHex,
			ResolverAddr:      strings.TrimSpace(controlCommandPayloadString(req.Payload, "resolver_addr")),
			Name:              name,
			TargetPubkeyHex:   targetPubkeyHex,
		})
		payload := map[string]any{
			"domain_set_target_result": result,
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		if !result.Ok {
			msg := strings.TrimSpace(result.Message)
			if msg == "" {
				msg = "domain set target failed"
			}
			return businessActionFailure(req, d, msg, payload), nil
		}
		return businessActionSuccess(req, "updated", payload, d), nil

	case controlActionPeerCall:
		to := strings.TrimSpace(controlCommandPayloadString(req.Payload, "to"))
		protocolID := strings.TrimSpace(controlCommandPayloadString(req.Payload, "protocol_id"))
		contentType := strings.TrimSpace(controlCommandPayloadString(req.Payload, "content_type"))
		if to == "" || protocolID == "" {
			return businessActionFailure(req, d, "to and protocol_id are required", nil), nil
		}
		if contentType == "" {
			contentType = "application/json"
		}
		body, err := businessPayloadPeerCallBody(req.Payload, contentType)
		if err != nil {
			return businessActionFailure(req, d, err.Error(), nil), nil
		}
		serviceQuote, err := businessPayloadOptionalBase64(req.Payload, "service_quote_base64")
		if err != nil {
			return businessActionFailure(req, d, err.Error(), nil), nil
		}
		requireActiveFeePool, _ := businessPayloadBool(req.Payload, "require_active_fee_pool")
		result, err := clientapp.TriggerPeerCall(ctx, rt, clientapp.TriggerPeerCallParams{
			To:                   to,
			ProtocolID:           protocol.ID(protocolID),
			ContentType:          contentType,
			Body:                 body,
			Store:                store,
			PaymentMode:          strings.TrimSpace(controlCommandPayloadString(req.Payload, "payment_mode")),
			PaymentScheme:        strings.TrimSpace(controlCommandPayloadString(req.Payload, "payment_scheme")),
			ServiceQuote:         serviceQuote,
			RequireActiveFeePool: requireActiveFeePool,
		})
		payload := map[string]any{
			"peer_call_response": businessPeerCallResponseMap(result),
		}
		if err != nil {
			return businessActionFailure(req, d, err.Error(), payload), nil
		}
		return businessActionSuccess(req, "returned", payload, d), nil

	case controlActionPeerSelf:
		return d.executeManagedPeerSelfControlCommand(req)

	case controlActionPeerConnect:
		return d.executeManagedPeerConnectControlCommand(req)

	default:
		return controlCommandResult{}, fmt.Errorf("unsupported control action: %s", req.Action)
	}
}

func businessActionSuccess(req controlCommandRequest, result string, payload map[string]any, d *managedDaemon) controlCommandResult {
	return controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		OK:           true,
		Result:       result,
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
		Payload:      payload,
	}
}

func businessActionFailure(req controlCommandRequest, d *managedDaemon, errText string, payload map[string]any) controlCommandResult {
	return controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		Result:       "failed",
		Error:        strings.TrimSpace(errText),
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
		Payload:      payload,
	}
}

func (d *managedDaemon) executeManagedPeerSelfControlCommand(req controlCommandRequest) (controlCommandResult, error) {
	rt := d.currentRuntime()
	if rt == nil {
		return businessActionFailure(req, d, "runtime not initialized", nil), nil
	}
	// 这里显式暴露 runtime 的节点身份，给 outproc e2e 做跨进程编排，
	// 避免测试侧再去读进程内对象或推测地址格式。
	info, err := businessBuildPeerSelfPayload(rt)
	payload := map[string]any{"peer_self": info}
	if err != nil {
		return businessActionFailure(req, d, err.Error(), payload), nil
	}
	return businessActionSuccess(req, "returned", payload, d), nil
}

func (d *managedDaemon) executeManagedPeerConnectControlCommand(req controlCommandRequest) (controlCommandResult, error) {
	rt := d.currentRuntime()
	if rt == nil {
		return businessActionFailure(req, d, "runtime not initialized", nil), nil
	}
	ctx := d.rootCtx
	if ctx == nil {
		return businessActionFailure(req, d, "runtime context is not ready", nil), nil
	}
	// 只接受完整 /ip4/.../p2p/... 地址，确保 e2e 用的就是线上同款拨号输入。
	addrText := strings.TrimSpace(controlCommandPayloadString(req.Payload, "addr"))
	if addrText == "" {
		return businessActionFailure(req, d, "addr is required", nil), nil
	}
	target, err := peer.AddrInfoFromString(addrText)
	payload := map[string]any{
		"peer_connect": map[string]any{
			"addr":    addrText,
			"peer_id": "",
		},
	}
	if err != nil {
		return businessActionFailure(req, d, "invalid addr", payload), nil
	}
	if target == nil || target.ID == "" || len(target.Addrs) == 0 {
		return businessActionFailure(req, d, "addr must include peer id and transport address", payload), nil
	}
	payload["peer_connect"] = map[string]any{
		"addr":    addrText,
		"peer_id": target.ID.String(),
	}
	if rt.Host == nil {
		return businessActionFailure(req, d, "runtime host not initialized", payload), nil
	}
	// 先写 peerstore，再主动 connect；失败直接回包，避免吞掉连通性问题。
	rt.Host.Peerstore().AddAddrs(target.ID, target.Addrs, 2*time.Minute)
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rt.Host.Connect(connectCtx, *target); err != nil {
		return businessActionFailure(req, d, err.Error(), payload), nil
	}
	return businessActionSuccess(req, "connected", payload, d), nil
}

func businessPayloadDemandBatchItems(payload map[string]any) ([]clientapp.PublishDemandBatchItem, error) {
	raw, ok := payload["items"]
	if !ok || raw == nil {
		return nil, fmt.Errorf("items is required")
	}
	list, ok := raw.([]any)
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("items is required")
	}
	out := make([]clientapp.PublishDemandBatchItem, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("items[%d] is invalid", i)
		}
		seedHash := strings.TrimSpace(controlCommandPayloadString(m, "seed_hash"))
		chunkCount, ok := pricingPayloadUint32(m, "chunk_count")
		if seedHash == "" || !ok || chunkCount == 0 {
			return nil, fmt.Errorf("items[%d] seed_hash and chunk_count are required", i)
		}
		out = append(out, clientapp.PublishDemandBatchItem{
			SeedHash:   seedHash,
			ChunkCount: chunkCount,
		})
	}
	return out, nil
}

func businessPayloadOptionalBase64(payload map[string]any, key string) ([]byte, error) {
	value := strings.TrimSpace(controlCommandPayloadString(payload, key))
	if value == "" {
		return nil, nil
	}
	out, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", key)
	}
	return out, nil
}

func businessPayloadPeerCallBody(payload map[string]any, contentType string) ([]byte, error) {
	bodyB64 := strings.TrimSpace(controlCommandPayloadString(payload, "body_base64"))
	rawBody, hasBody := payload["body"]
	if bodyB64 != "" && hasBody && rawBody != nil {
		return nil, fmt.Errorf("body and body_base64 are mutually exclusive")
	}
	if bodyB64 != "" {
		out, err := base64.StdEncoding.DecodeString(bodyB64)
		if err != nil {
			return nil, fmt.Errorf("invalid body_base64")
		}
		return out, nil
	}
	if !hasBody || rawBody == nil {
		return nil, nil
	}
	if bodyText, ok := rawBody.(string); ok {
		bodyText = strings.TrimSpace(bodyText)
		if bodyText == "" {
			return nil, nil
		}
		if strings.Contains(strings.ToLower(contentType), "json") {
			if json.Valid([]byte(bodyText)) {
				return []byte(bodyText), nil
			}
			encoded, err := json.Marshal(bodyText)
			if err != nil {
				return nil, err
			}
			return encoded, nil
		}
		return []byte(bodyText), nil
	}
	encoded, err := json.Marshal(rawBody)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func businessPayloadBool(payload map[string]any, key string) (bool, bool) {
	if len(payload) == 0 {
		return false, false
	}
	raw, ok := payload[key]
	if !ok || raw == nil {
		return false, false
	}
	switch v := raw.(type) {
	case bool:
		return v, true
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true":
			return true, true
		case "false":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

func businessPeerCallResponseMap(resp ncall.CallResp) map[string]any {
	out := map[string]any{
		"ok":           resp.Ok,
		"code":         strings.TrimSpace(resp.Code),
		"message":      strings.TrimSpace(resp.Message),
		"content_type": strings.TrimSpace(resp.ContentType),
		"body_base64":  base64.StdEncoding.EncodeToString(resp.Body),
	}
	if len(resp.PaymentSchemes) > 0 {
		items := make([]map[string]any, 0, len(resp.PaymentSchemes))
		for _, item := range resp.PaymentSchemes {
			if item == nil {
				continue
			}
			items = append(items, map[string]any{
				"scheme":                      strings.TrimSpace(item.Scheme),
				"payment_domain":              strings.TrimSpace(item.PaymentDomain),
				"amount_satoshi":              item.AmountSatoshi,
				"description":                 strings.TrimSpace(item.Description),
				"minimum_pool_amount_satoshi": item.MinimumPoolAmountSatoshi,
				"fee_rate_sat_per_byte_milli": item.FeeRateSatPerByteMilli,
				"lock_blocks":                 item.LockBlocks,
				"pricing_mode":                strings.TrimSpace(item.PricingMode),
				"service_quantity":            item.ServiceQuantity,
				"service_quantity_unit":       strings.TrimSpace(item.ServiceQuantityUnit),
				"quote_status":                strings.TrimSpace(item.QuoteStatus),
			})
		}
		out["payment_schemes"] = items
	}
	if strings.TrimSpace(resp.PaymentReceiptScheme) != "" {
		out["payment_receipt_scheme"] = strings.TrimSpace(resp.PaymentReceiptScheme)
	}
	if len(resp.PaymentReceipt) > 0 {
		out["payment_receipt_base64"] = base64.StdEncoding.EncodeToString(resp.PaymentReceipt)
	}
	if len(resp.ServiceQuote) > 0 {
		out["service_quote_base64"] = base64.StdEncoding.EncodeToString(resp.ServiceQuote)
	}
	if len(resp.ServiceReceipt) > 0 {
		out["service_receipt_base64"] = base64.StdEncoding.EncodeToString(resp.ServiceReceipt)
	}
	return out
}

func businessBuildPeerSelfPayload(rt *clientapp.Runtime) (map[string]any, error) {
	if rt == nil || rt.Host == nil {
		return map[string]any{
			"pubkey_hex": "",
			"peer_id":    "",
			"addrs":      []string{},
		}, fmt.Errorf("runtime host not initialized")
	}
	// 对外业务 ID 统一使用 client_pubkey_hex（33-byte 压缩公钥），
	// 不能把 libp2p host 公钥编码（protobuf 封装）直接当业务 ID 回给上层。
	cfgPubHex := strings.ToLower(strings.TrimSpace(rt.ConfigSnapshot().ClientID))
	pubHex := cfgPubHex
	if pubHex == "" {
		rawPubHex, err := runtimePubKeyHex(rt)
		if err != nil {
			return map[string]any{
				"pubkey_hex": "",
				"peer_id":    strings.TrimSpace(rt.Host.ID().String()),
				"addrs":      []string{},
			}, err
		}
		pubHex = strings.ToLower(strings.TrimSpace(rawPubHex))
	}
	peerID := strings.TrimSpace(rt.Host.ID().String())
	p2pTail := multiaddr.StringCast("/p2p/" + peerID)
	addrs := make([]string, 0, len(rt.Host.Addrs()))
	for _, addr := range rt.Host.Addrs() {
		if addr == nil {
			continue
		}
		// 控制面统一返回带 /p2p 的完整地址，测试可直接拿来拨号。
		addrs = append(addrs, addr.Encapsulate(p2pTail).String())
	}
	sort.Strings(addrs)
	return map[string]any{
		"pubkey_hex": pubHex,
		"peer_id":    peerID,
		"addrs":      addrs,
	}, nil
}
