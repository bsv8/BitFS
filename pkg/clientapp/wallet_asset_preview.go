package clientapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv8/BFTP/pkg/infra/fundalloc"
)

const walletAssetPreviewFeeRateSatPerKB = 0.5

type walletAssetActionPreviewChange struct {
	OwnerScope    string `json:"owner_scope"`
	AssetGroup    string `json:"asset_group"`
	AssetStandard string `json:"asset_standard"`
	AssetKey      string `json:"asset_key"`
	AssetSymbol   string `json:"asset_symbol"`
	QuantityText  string `json:"quantity_text"`
	Direction     string `json:"direction"`
	Note          string `json:"note,omitempty"`
}

type walletAssetActionPreview struct {
	Action                    string                           `json:"action"`
	Feasible                  bool                             `json:"feasible"`
	CanSign                   bool                             `json:"can_sign"`
	Summary                   string                           `json:"summary"`
	DetailLines               []string                         `json:"detail_lines,omitempty"`
	WarningLevel              string                           `json:"warning_level,omitempty"`
	EstimatedNetworkFeeBSVSat uint64                           `json:"estimated_network_fee_bsv_sat,omitempty"`
	FeeFundingTargetBSVSat    uint64                           `json:"fee_funding_target_bsv_sat,omitempty"`
	SelectedAssetUTXOIDs      []string                         `json:"selected_asset_utxo_ids,omitempty"`
	SelectedFeeUTXOIDs        []string                         `json:"selected_fee_utxo_ids,omitempty"`
	TxID                      string                           `json:"txid,omitempty"`
	PreviewHash               string                           `json:"preview_hash,omitempty"`
	Changes                   []walletAssetActionPreviewChange `json:"changes,omitempty"`
}

type walletAssetActionPreviewResp struct {
	Ok      bool                     `json:"ok"`
	Code    string                   `json:"code"`
	Message string                   `json:"message,omitempty"`
	Preview walletAssetActionPreview `json:"preview"`
}

type walletTokenSendPreviewRequest struct {
	TokenStandard string `json:"token_standard"`
	AssetKey      string `json:"asset_key"`
	AmountText    string `json:"amount_text"`
	ToAddress     string `json:"to_address"`
}

type walletOrdinalTransferPreviewRequest struct {
	UTXOID    string `json:"utxo_id"`
	AssetKey  string `json:"asset_key"`
	ToAddress string `json:"to_address"`
}

type walletTokenPreviewCandidate struct {
	Item          walletTokenOutputItem
	CreatedAtUnix int64
	Quantity      decimalTextValue
}

// 设计说明：
// - preview 统一承担“资产变化预演”合同，先把花什么、收什么、链费多少讲清楚；
// - bsv20 / bsv21 tokens.send 和 ordinals.transfer 共用一套 preview_hash 语义；
// - 如果运行时具备真实构造依赖，preview 会直接升级成“可签名预览”，避免页面、CLI、HTTP 各自再做第二套判断。
func (s *httpAPIServer) handleWalletTokenSendPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletTokenSendPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletTokenSendPreview(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletOrdinalTransferPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletOrdinalTransferPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletOrdinalTransferPreview(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func buildWalletTokenSendPreview(r *http.Request, s *httpAPIServer, req walletTokenSendPreviewRequest) (walletAssetActionPreviewResp, error) {
	standard := normalizeWalletTokenStandard(req.TokenStandard)
	if standard == "" {
		return walletAssetActionPreviewResp{}, fmt.Errorf("invalid token standard")
	}
	assetKey := strings.TrimSpace(req.AssetKey)
	if assetKey == "" {
		return walletAssetActionPreviewResp{}, fmt.Errorf("asset_key is required")
	}
	amountText := normalizePreviewQuantityText(req.AmountText)
	requestedAmount, err := parseDecimalText(amountText)
	if err != nil || requestedAmount.intValue == nil || requestedAmount.intValue.Sign() <= 0 {
		return walletAssetActionPreviewResp{}, fmt.Errorf("amount_text invalid")
	}
	toAddress := strings.TrimSpace(req.ToAddress)
	if err := validatePreviewAddress(toAddress); err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	preview, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletAssetActionPreview, error) {
		return previewWalletTokenSend(db, address, standard, assetKey, amountText, toAddress)
	})
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	if s != nil && s.rt != nil && preview.Feasible {
		switch standard {
		case "bsv20", "bsv21":
			prepared, prepareErr := httpDBValue(r.Context(), s, func(db *sql.DB) (preparedWalletTokenSend, error) {
				return prepareWalletTokenSend(r.Context(), db, s.rt, address, standard, assetKey, amountText, toAddress)
			})
			if prepareErr == nil {
				preview = prepared.Preview
			} else {
				preview.CanSign = false
				preview.WarningLevel = "high"
				preview.DetailLines = append(preview.DetailLines, "状态: 当前预演可行，但真实交易构造失败，暂不能签名。")
				preview.DetailLines = append(preview.DetailLines, "原因: "+prepareErr.Error())
			}
		}
	}
	return walletAssetActionPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: preview,
	}, nil
}

func buildWalletOrdinalTransferPreview(r *http.Request, s *httpAPIServer, req walletOrdinalTransferPreviewRequest) (walletAssetActionPreviewResp, error) {
	toAddress := strings.TrimSpace(req.ToAddress)
	if err := validatePreviewAddress(toAddress); err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	preview, err := httpDBValue(r.Context(), s, func(db *sql.DB) (walletAssetActionPreview, error) {
		return previewWalletOrdinalTransfer(db, address, strings.TrimSpace(req.UTXOID), strings.TrimSpace(req.AssetKey), toAddress)
	})
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	if s != nil && s.rt != nil && preview.Feasible {
		prepared, prepareErr := httpDBValue(r.Context(), s, func(db *sql.DB) (preparedWalletOrdinalTransfer, error) {
			return prepareWalletOrdinalTransfer(db, s.rt, address, strings.TrimSpace(req.UTXOID), strings.TrimSpace(req.AssetKey), toAddress)
		})
		if prepareErr == nil {
			preview = prepared.Preview
		} else {
			preview.CanSign = false
			preview.WarningLevel = "high"
			preview.DetailLines = append(preview.DetailLines, "状态: 当前预演可行，但真实交易构造失败，暂不能签名。")
		}
	}
	return walletAssetActionPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: preview,
	}, nil
}

func previewWalletTokenSend(db *sql.DB, address string, standard string, assetKey string, amountText string, toAddress string) (walletAssetActionPreview, error) {
	requested, _ := parseDecimalText(amountText)
	candidates, err := loadWalletTokenPreviewCandidates(db, address, standard, assetKey)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	selected, selectedTotal, availableTotal, assetSymbol, err := selectWalletTokenPreviewCandidates(candidates, requested)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	changeText, err := subtractDecimalText(selectedTotal, amountText)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	assetInputSats := uint64(len(selected))
	assetOutputCount := 1
	if isPositiveDecimalText(changeText) {
		assetOutputCount++
	}
	feeSelection, fee, fundingNeed, err := previewPlainBSVFeeFunding(db, address, assetInputSats, len(selected), assetOutputCount)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	feasible := compareDecimalText(availableTotal, amountText) >= 0 && feeSelection.Feasible
	detailLines := []string{
		fmt.Sprintf("发送数量: %s %s", amountText, firstNonEmptyString(assetSymbol, assetKey)),
		fmt.Sprintf("接收地址: %s", toAddress),
		fmt.Sprintf("已选承载输出: %d 个", len(selected)),
		fmt.Sprintf("预估矿工费: %d sat BSV", fee),
	}
	if availableTotal != "" {
		detailLines = append(detailLines, fmt.Sprintf("当前可见总量: %s %s", availableTotal, firstNonEmptyString(assetSymbol, assetKey)))
	}
	if fundingNeed > 0 {
		detailLines = append(detailLines, fmt.Sprintf("需额外提供 BSV: %d sat", fundingNeed))
	}
	if isPositiveDecimalText(changeText) {
		detailLines = append(detailLines, fmt.Sprintf("资产找零: %s %s", changeText, firstNonEmptyString(assetSymbol, assetKey)))
	}
	if !feasible {
		if compareDecimalText(availableTotal, amountText) < 0 {
			detailLines = append(detailLines, "状态: 当前 token 可见持仓不足，预览不可执行。")
		} else {
			detailLines = append(detailLines, "状态: 当前 plain BSV 不足以承担预估矿工费，预览不可执行。")
		}
	} else {
		detailLines = append(detailLines, "状态: 当前资产和链费条件足够，但真正协议构造尚未接入，本阶段只提供预演。")
	}
	changeLines := []walletAssetActionPreviewChange{
		{
			OwnerScope:    "wallet_self",
			AssetGroup:    walletAssetGroupToken,
			AssetStandard: standard,
			AssetKey:      assetKey,
			AssetSymbol:   assetSymbol,
			QuantityText:  amountText,
			Direction:     "debit",
			Note:          "token send request",
		},
		{
			OwnerScope:    "receiver",
			AssetGroup:    walletAssetGroupToken,
			AssetStandard: standard,
			AssetKey:      assetKey,
			AssetSymbol:   assetSymbol,
			QuantityText:  amountText,
			Direction:     "credit",
			Note:          toAddress,
		},
	}
	if isPositiveDecimalText(changeText) {
		changeLines = append(changeLines, walletAssetActionPreviewChange{
			OwnerScope:    "wallet_self",
			AssetGroup:    walletAssetGroupToken,
			AssetStandard: standard,
			AssetKey:      assetKey,
			AssetSymbol:   assetSymbol,
			QuantityText:  changeText,
			Direction:     "credit",
			Note:          "token change",
		})
	}
	changeLines = append(changeLines, walletAssetActionPreviewChange{
		OwnerScope:    "network_fee",
		AssetGroup:    "bsv",
		AssetStandard: "native",
		AssetKey:      "bsv",
		AssetSymbol:   "BSV",
		QuantityText:  fmt.Sprintf("%d", fee),
		Direction:     "debit",
		Note:          "estimated miner fee",
	})
	selectedAssetUTXOIDs := make([]string, 0, len(selected))
	for _, item := range selected {
		selectedAssetUTXOIDs = append(selectedAssetUTXOIDs, item.Item.UTXOID)
	}
	return walletAssetActionPreview{
		Action:                    "tokens.send",
		Feasible:                  feasible,
		CanSign:                   false,
		Summary:                   buildWalletTokenSendPreviewSummary(feasible, amountText, assetSymbol, len(selected), fee),
		DetailLines:               detailLines,
		WarningLevel:              previewWarningLevel(feasible),
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    fundingNeed,
		SelectedAssetUTXOIDs:      selectedAssetUTXOIDs,
		SelectedFeeUTXOIDs:        feeSelection.SelectedUTXOIDs,
		Changes:                   changeLines,
	}, nil
}

func previewWalletOrdinalTransfer(db *sql.DB, address string, utxoID string, assetKey string, toAddress string) (walletAssetActionPreview, error) {
	item, err := loadWalletOrdinalDetail(db, address, utxoID, assetKey)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	feeSelection, fee, fundingNeed, err := previewPlainBSVFeeFunding(db, address, item.ValueSatoshi, 1, 1)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	feasible := feeSelection.Feasible
	detailLines := []string{
		fmt.Sprintf("Ordinal 标识: %s", item.AssetKey),
		fmt.Sprintf("承载输出: %s", item.UTXOID),
		fmt.Sprintf("接收地址: %s", toAddress),
		fmt.Sprintf("预估矿工费: %d sat BSV", fee),
	}
	if fundingNeed > 0 {
		detailLines = append(detailLines, fmt.Sprintf("需额外提供 BSV: %d sat", fundingNeed))
	}
	if !feasible {
		detailLines = append(detailLines, "状态: 当前 plain BSV 不足以承担预估矿工费，预览不可执行。")
	} else {
		detailLines = append(detailLines, "状态: 当前资产和链费条件足够，但真正协议构造尚未接入，本阶段只提供预演。")
	}
	return walletAssetActionPreview{
		Action:                    "ordinals.transfer",
		Feasible:                  feasible,
		CanSign:                   false,
		Summary:                   buildWalletOrdinalTransferPreviewSummary(feasible, item.AssetKey, fee),
		DetailLines:               detailLines,
		WarningLevel:              previewWarningLevel(feasible),
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    fundingNeed,
		SelectedAssetUTXOIDs:      []string{item.UTXOID},
		SelectedFeeUTXOIDs:        feeSelection.SelectedUTXOIDs,
		Changes: []walletAssetActionPreviewChange{
			{
				OwnerScope:    "wallet_self",
				AssetGroup:    walletAssetGroupOrdinal,
				AssetStandard: item.AssetStandard,
				AssetKey:      item.AssetKey,
				AssetSymbol:   item.AssetSymbol,
				QuantityText:  "1",
				Direction:     "debit",
				Note:          "ordinal transfer request",
			},
			{
				OwnerScope:    "receiver",
				AssetGroup:    walletAssetGroupOrdinal,
				AssetStandard: item.AssetStandard,
				AssetKey:      item.AssetKey,
				AssetSymbol:   item.AssetSymbol,
				QuantityText:  "1",
				Direction:     "credit",
				Note:          toAddress,
			},
			{
				OwnerScope:    "network_fee",
				AssetGroup:    "bsv",
				AssetStandard: "native",
				AssetKey:      "bsv",
				AssetSymbol:   "BSV",
				QuantityText:  fmt.Sprintf("%d", fee),
				Direction:     "debit",
				Note:          "estimated miner fee",
			},
		},
	}, nil
}

func loadWalletTokenPreviewCandidates(db *sql.DB, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return []walletTokenPreviewCandidate{}, nil
	}
	walletID := walletIDByAddress(address)
	rows, err := db.Query(
		`SELECT u.utxo_id,u.txid,u.vout,u.value_satoshi,u.allocation_class,u.allocation_reason,u.created_at_unix,
		        a.asset_standard,a.asset_key,a.asset_symbol,a.quantity_text,a.source_name,a.payload_json,a.updated_at_unix
		 FROM wallet_utxo_assets a
		 JOIN wallet_utxo u ON u.utxo_id=a.utxo_id
		 WHERE a.wallet_id=? AND a.address=? AND a.asset_group=? AND a.asset_standard=? AND a.asset_key=? AND u.state='unspent'
		 ORDER BY u.created_at_unix ASC,u.value_satoshi ASC,u.utxo_id ASC`,
		walletID, address, walletAssetGroupToken, standard, assetKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]walletTokenPreviewCandidate, 0, 8)
	for rows.Next() {
		var item walletTokenPreviewCandidate
		var payload string
		if err := rows.Scan(
			&item.Item.UTXOID,
			&item.Item.TxID,
			&item.Item.Vout,
			&item.Item.ValueSatoshi,
			&item.Item.AllocationClass,
			&item.Item.AllocationReason,
			&item.CreatedAtUnix,
			&item.Item.TokenStandard,
			&item.Item.AssetKey,
			&item.Item.AssetSymbol,
			&item.Item.QuantityText,
			&item.Item.SourceName,
			&payload,
			&item.Item.UpdatedAtUnix,
		); err != nil {
			return nil, err
		}
		qty, err := parseDecimalText(item.Item.QuantityText)
		if err != nil {
			return nil, fmt.Errorf("token quantity_text invalid: %w", err)
		}
		item.Quantity = qty
		item.Item.WalletAddress = address
		item.Item.Payload = json.RawMessage(payload)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func selectWalletTokenPreviewCandidates(candidates []walletTokenPreviewCandidate, requested decimalTextValue) ([]walletTokenPreviewCandidate, string, string, string, error) {
	selected := make([]walletTokenPreviewCandidate, 0, len(candidates))
	var selectedAcc decimalTextAccumulator
	var totalAcc decimalTextAccumulator
	assetSymbol := ""
	for _, item := range candidates {
		if err := totalAcc.Add(item.Item.QuantityText); err != nil {
			return nil, "", "", "", err
		}
		if assetSymbol == "" {
			assetSymbol = strings.TrimSpace(item.Item.AssetSymbol)
		}
		if compareDecimalValues(selectedAcc.value(), requested) >= 0 {
			continue
		}
		selected = append(selected, item)
		if err := selectedAcc.Add(item.Item.QuantityText); err != nil {
			return nil, "", "", "", err
		}
	}
	return selected, selectedAcc.String(), totalAcc.String(), assetSymbol, nil
}

type plainBSVPreviewSelection struct {
	Feasible        bool
	SelectedUTXOIDs []string
	TotalSatoshi    uint64
}

func previewPlainBSVFeeFunding(db *sql.DB, address string, assetInputSatoshi uint64, assetInputCount int, assetOutputCount int) (plainBSVPreviewSelection, uint64, uint64, error) {
	return previewPlainBSVFunding(db, address, assetInputSatoshi, assetInputCount, assetOutputCount, uint64(assetOutputCount))
}

// previewPlainBSVFunding 按“固定输出总额 + 矿工费”的口径估算 plain BSV 选币。
// 设计说明：
// - 对 bsv20 来说，固定输出额就是 token 承载输出的 1 sat 总和；
// - 对 bsv21 来说，还要把 overlay fee 输出额一并算进去；
// - 这样 preview/sign 能共用一套 BSV 资金判断，不会在 bsv21 上低估所需资金。
func previewPlainBSVFunding(db *sql.DB, address string, assetInputSatoshi uint64, assetInputCount int, assetOutputCount int, fixedOutputSatoshi uint64) (plainBSVPreviewSelection, uint64, uint64, error) {
	candidates, err := listPlainBSVFundingCandidatesFromDB(db, address)
	if err != nil {
		return plainBSVPreviewSelection{}, 0, 0, err
	}
	selected := []fundalloc.Candidate{}
	var fundingNeed uint64
	var fee uint64
	var totalSelected uint64
	for range 4 {
		bsvChangeOutputs := 0
		if totalSelected > fundingNeed && len(selected) > 0 {
			bsvChangeOutputs = 1
		}
		outputCount := assetOutputCount + bsvChangeOutputs
		size := estimateProvisionalP2PKHSize(assetInputCount+len(selected), outputCount)
		fee = estimateMinerFeeSatPerKB(size, walletAssetPreviewFeeRateSatPerKB)
		if assetInputSatoshi >= fixedOutputSatoshi+fee {
			fundingNeed = 0
		} else {
			fundingNeed = fixedOutputSatoshi + fee - assetInputSatoshi
		}
		if fundingNeed == 0 {
			selected = nil
			totalSelected = 0
			break
		}
		selection, selErr := fundalloc.SelectPlainBSVForTarget(candidates, fundingNeed)
		if selErr != nil {
			return plainBSVPreviewSelection{Feasible: false}, fee, fundingNeed, nil
		}
		selected = selection.Selected
		totalSelected = selection.TotalSatoshi
		if totalSelected >= fundingNeed {
			bsvChangeOutputs = 0
			if totalSelected > fundingNeed {
				bsvChangeOutputs = 1
			}
			finalSize := estimateProvisionalP2PKHSize(assetInputCount+len(selected), assetOutputCount+bsvChangeOutputs)
			finalFee := estimateMinerFeeSatPerKB(finalSize, walletAssetPreviewFeeRateSatPerKB)
			finalNeed := fixedOutputSatoshi + finalFee
			if assetInputSatoshi < finalNeed {
				finalNeed -= assetInputSatoshi
			} else {
				finalNeed = 0
			}
			if finalNeed == fundingNeed {
				ids := make([]string, 0, len(selected))
				for _, item := range selected {
					ids = append(ids, item.ID)
				}
				return plainBSVPreviewSelection{
					Feasible:        true,
					SelectedUTXOIDs: ids,
					TotalSatoshi:    totalSelected,
				}, finalFee, finalNeed, nil
			}
			fundingNeed = finalNeed
		}
	}
	ids := make([]string, 0, len(selected))
	for _, item := range selected {
		ids = append(ids, item.ID)
	}
	return plainBSVPreviewSelection{
		Feasible:        fundingNeed == 0 || totalSelected >= fundingNeed,
		SelectedUTXOIDs: ids,
		TotalSatoshi:    totalSelected,
	}, fee, fundingNeed, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func listPlainBSVFundingCandidatesFromDB(db *sql.DB, address string) ([]fundalloc.Candidate, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return []fundalloc.Candidate{}, nil
	}
	walletID := walletIDByAddress(address)
	rows, err := db.Query(
		`SELECT utxo_id,txid,vout,value_satoshi,created_at_unix,allocation_class,allocation_reason
		 FROM wallet_utxo
		 WHERE wallet_id=? AND address=? AND state='unspent'
		 ORDER BY created_at_unix ASC,value_satoshi ASC,txid ASC,vout ASC`,
		walletID,
		address,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]fundalloc.Candidate, 0, 8)
	for rows.Next() {
		var item fundalloc.Candidate
		var allocationClass string
		var allocationReason string
		if err := rows.Scan(&item.ID, &item.TxID, &item.Vout, &item.ValueSatoshi, &item.CreatedAtUnix, &allocationClass, &allocationReason); err != nil {
			return nil, err
		}
		item.ProtectionClass = fundalloc.ProtectionClass(allocationClass)
		item.ProtectionReason = strings.TrimSpace(allocationReason)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func estimateProvisionalP2PKHSize(inputCount int, outputCount int) int {
	if inputCount < 0 {
		inputCount = 0
	}
	if outputCount < 0 {
		outputCount = 0
	}
	// 设计说明：
	// - preview 目前还没到最终协议构造阶段，这里用标准 p2pkh 交易大小做保守估算；
	// - 先把“是否有足够 BSV 承担链费”判断出来，后面接入真正模板时再替换成精确大小；
	// - 10 + 148 * in + 34 * out 是经典 p2pkh 近似值，适合当前预演阶段。
	return 10 + inputCount*148 + outputCount*34
}

func validatePreviewAddress(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("to_address is required")
	}
	if _, err := script.NewAddressFromString(strings.TrimSpace(raw)); err != nil {
		return fmt.Errorf("to_address invalid")
	}
	return nil
}

func normalizePreviewQuantityText(raw string) string {
	return strings.TrimSpace(raw)
}

func previewWarningLevel(feasible bool) string {
	if feasible {
		return "medium"
	}
	return "high"
}

func buildWalletTokenSendPreviewSummary(feasible bool, amountText string, assetSymbol string, selectedCount int, fee uint64) string {
	label := firstNonEmptyString(assetSymbol, "token")
	if feasible {
		return fmt.Sprintf("将发送 %s %s，预计选择 %d 个承载输出并消耗约 %d sat BSV 链费。", amountText, label, selectedCount, fee)
	}
	return fmt.Sprintf("无法完成 %s %s 的发送预演，当前资产或链费条件不足。", amountText, label)
}

func buildWalletOrdinalTransferPreviewSummary(feasible bool, assetKey string, fee uint64) string {
	if feasible {
		return fmt.Sprintf("将转移 ordinal %s，预计消耗约 %d sat BSV 链费。", assetKey, fee)
	}
	return fmt.Sprintf("无法完成 ordinal %s 的转移预演，当前 plain BSV 不足以承担链费。", assetKey)
}

func firstNonEmptyString(values ...string) string {
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value != "" {
			return value
		}
	}
	return ""
}

func compareDecimalText(left string, right string) int {
	lv, err := parseDecimalText(left)
	if err != nil {
		return -1
	}
	rv, err := parseDecimalText(right)
	if err != nil {
		return 1
	}
	return compareDecimalValues(decimalTextValue{intValue: lv.intValue, scale: lv.scale}, decimalTextValue{intValue: rv.intValue, scale: rv.scale})
}

func compareDecimalValues(left decimalTextValue, right decimalTextValue) int {
	lv := normalizeDecimalValue(left)
	rv := normalizeDecimalValue(right)
	scale := lv.scale
	if rv.scale > scale {
		scale = rv.scale
	}
	leftInt := newScaledDecimalInt(lv, scale)
	rightInt := newScaledDecimalInt(rv, scale)
	return leftInt.Cmp(rightInt)
}

func subtractDecimalText(left string, right string) (string, error) {
	lv, err := parseDecimalText(left)
	if err != nil {
		return "", err
	}
	rv, err := parseDecimalText(right)
	if err != nil {
		return "", err
	}
	scale := lv.scale
	if rv.scale > scale {
		scale = rv.scale
	}
	leftInt := newScaledDecimalInt(lv, scale)
	rightInt := newScaledDecimalInt(rv, scale)
	return formatDecimalText(leftInt.Sub(leftInt, rightInt), scale), nil
}

func isPositiveDecimalText(raw string) bool {
	value, err := parseDecimalText(raw)
	if err != nil || value.intValue == nil {
		return false
	}
	return value.intValue.Sign() > 0
}

func normalizeDecimalValue(v decimalTextValue) decimalTextValue {
	if v.intValue == nil {
		return decimalTextValue{intValue: decimalZero(), scale: 0}
	}
	return v
}

func newScaledDecimalInt(v decimalTextValue, scale int) *big.Int {
	value := normalizeDecimalValue(v)
	out := new(big.Int).Set(value.intValue)
	if value.scale < scale {
		out.Mul(out, decimalPow10(scale-value.scale))
	}
	return out
}

func decimalZero() *big.Int {
	return big.NewInt(0)
}

func (a decimalTextAccumulator) value() decimalTextValue {
	if a.sum == nil {
		return decimalTextValue{intValue: decimalZero(), scale: 0}
	}
	return decimalTextValue{intValue: new(big.Int).Set(a.sum), scale: a.scale}
}
