package clientapp

import (
	"context"
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

type decimalTextValue struct {
	intValue *big.Int
	scale    int
}

type decimalTextAccumulator struct {
	sum   *big.Int
	scale int
}

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

type walletTokenOutputItem struct {
	UTXOID           string          `json:"utxo_id"`
	WalletAddress    string          `json:"wallet_address"`
	TxID             string          `json:"txid"`
	Vout             uint32          `json:"vout"`
	ValueSatoshi     uint64          `json:"value_satoshi"`
	AllocationClass  string          `json:"allocation_class"`
	AllocationReason string          `json:"allocation_reason"`
	TokenStandard    string          `json:"token_standard"`
	AssetKey         string          `json:"asset_key"`
	AssetSymbol      string          `json:"asset_symbol"`
	QuantityText     string          `json:"quantity_text"`
	SourceName       string          `json:"source_name"`
	UpdatedAtUnix    int64           `json:"updated_at_unix"`
	Payload          json.RawMessage `json:"payload"`
}

type walletTokenPreviewCandidate struct {
	Item          walletTokenOutputItem
	CreatedAtUnix int64
	Quantity      decimalTextValue
}

// 设计说明：
// - preview 统一承担“资产变化预演”合同，先把花什么、收什么、链费多少讲清楚；
// - 当前只保留 bsv21 tokens.send，wallet 不再暴露旧资产查询/ordinal 转移入口；
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
		return previewWalletTokenSend(r.Context(), db, s.rt, address, standard, assetKey, amountText, toAddress)
	})
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	if s != nil && s.rt != nil && preview.Feasible {
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
	return walletAssetActionPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: preview,
	}, nil
}

func previewWalletTokenSend(ctx context.Context, db *sql.DB, rt *Runtime, address string, standard string, assetKey string, amountText string, toAddress string) (walletAssetActionPreview, error) {
	requested, _ := parseDecimalText(amountText)
	candidates, err := loadWalletTokenPreviewCandidates(ctx, db, rt, address, standard, assetKey)
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

func loadWalletTokenPreviewCandidates(ctx context.Context, db *sql.DB, rt *Runtime, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	return loadWalletBSV21WOCCandidates(ctx, db, rt, address, assetKey)
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
// - 当前 wallet 资产面只保留 bsv21 send；
// - fixedOutputSatoshi 既包含 token 承载输出的 1 sat，也包含 bsv21 protocol fee；
// - 这样 preview/sign 能共用一套 BSV 资金判断，不会在真实构造时低估所需资金。
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

func normalizeWalletTokenStandard(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "bsv21":
		return "bsv21"
	default:
		return ""
	}
}

// resolveWalletAddressForHTTP 统一给钱包写接口拿当前钱包地址。
// 设计说明：
// - 正式运行时优先使用当前 runtime 钱包地址，避免多钱包测试夹具把口径搞乱；
// - 仅在最小测试环境里，才退回到本地 wallet_utxo 投影里的最新地址，保证 handler 可以被单测直接驱动。
func resolveWalletAddressForHTTP(ctx context.Context, s *httpAPIServer) (string, error) {
	if s == nil {
		return "", fmt.Errorf("http api server is nil")
	}
	if s.rt != nil {
		addr, err := clientWalletAddress(s.rt)
		if err == nil && strings.TrimSpace(addr) != "" {
			return strings.TrimSpace(addr), nil
		}
	}
	return httpDBValue(ctx, s, func(db *sql.DB) (string, error) {
		var addr string
		err := db.QueryRow(`SELECT address FROM wallet_utxo ORDER BY updated_at_unix DESC,created_at_unix DESC,utxo_id ASC LIMIT 1`).Scan(&addr)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return strings.TrimSpace(addr), nil
	})
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

func (a *decimalTextAccumulator) Add(raw string) error {
	value, err := parseDecimalText(raw)
	if err != nil {
		return err
	}
	if a.sum == nil {
		a.sum = new(big.Int).Set(value.intValue)
		a.scale = value.scale
		return nil
	}
	scale := a.scale
	if value.scale > scale {
		scale = value.scale
	}
	current := newScaledDecimalInt(decimalTextValue{intValue: a.sum, scale: a.scale}, scale)
	incoming := newScaledDecimalInt(value, scale)
	current.Add(current, incoming)
	a.sum = current
	a.scale = scale
	return nil
}

func (a decimalTextAccumulator) String() string {
	return formatDecimalText(a.value().intValue, a.value().scale)
}

func parseDecimalText(raw string) (decimalTextValue, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return decimalTextValue{}, fmt.Errorf("decimal text is empty")
	}
	sign := 1
	switch value[0] {
	case '+':
		value = value[1:]
	case '-':
		sign = -1
		value = value[1:]
	}
	if value == "" {
		return decimalTextValue{}, fmt.Errorf("decimal text is empty")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 {
		return decimalTextValue{}, fmt.Errorf("decimal text invalid")
	}
	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	if intPart == "" {
		intPart = "0"
	}
	if !allDecimalDigits(intPart) || !allDecimalDigits(fracPart) {
		return decimalTextValue{}, fmt.Errorf("decimal text invalid")
	}
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return decimalTextValue{intValue: decimalZero(), scale: len(fracPart)}, nil
	}
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return decimalTextValue{}, fmt.Errorf("decimal text invalid")
	}
	if sign < 0 {
		n.Neg(n)
	}
	return decimalTextValue{intValue: n, scale: len(fracPart)}, nil
}

func formatDecimalText(value *big.Int, scale int) string {
	if value == nil {
		return "0"
	}
	if scale < 0 {
		scale = 0
	}
	sign := ""
	raw := new(big.Int).Set(value)
	if raw.Sign() < 0 {
		sign = "-"
		raw.Abs(raw)
	}
	digits := raw.String()
	if scale == 0 {
		return sign + digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	point := len(digits) - scale
	intPart := digits[:point]
	fracPart := strings.TrimRight(digits[point:], "0")
	if fracPart == "" {
		return sign + intPart
	}
	return sign + intPart + "." + fracPart
}

func decimalPow10(n int) *big.Int {
	if n <= 0 {
		return big.NewInt(1)
	}
	out := big.NewInt(10)
	return out.Exp(out, big.NewInt(int64(n)), nil)
}

func allDecimalDigits(raw string) bool {
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
