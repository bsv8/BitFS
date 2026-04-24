package fundalloc

import (
	"fmt"
	"sort"
	"strings"
)

// ProtectionClass 描述一个输出能否被普通 BSV 资金分配器安全使用。
// 设计说明：
// - 分配器先看保护分类，再看金额策略，避免把“带二层资产的 1sat 输出”误当普通零钱花掉；
// - 这里只表达“能不能给普通 BSV 业务拿去花”，不承担完整资产协议建模；
// - 未来 tokens / ordinals 真正接进来后，只需要把识别结果映射成这里的分类即可。
type ProtectionClass string

const (
	ProtectionPlainBSV       ProtectionClass = "plain_bsv"
	ProtectionProtectedAsset ProtectionClass = "protected_asset"
	ProtectionUnknown        ProtectionClass = "unknown"
)

// Candidate 是分配器看到的一条候选输出。
type Candidate struct {
	ID               string
	TxID             string
	Vout             uint32
	ValueSatoshi     uint64
	CreatedAtUnix    int64
	ProtectionClass  ProtectionClass
	ProtectionReason string
}

// Selection 是一次资金分配结果。
type Selection struct {
	Selected       []Candidate
	TotalSatoshi   uint64
	EligibleCount  int
	ProtectedCount int
	UnknownCount   int
}

// SortOldSmallFirst 对候选输出做稳定排序。
// 排序口径：
// - 老的优先；
// - 同年龄下小金额优先；
// - 最后用 txid/vout 保持稳定顺序。
func SortOldSmallFirst(in []Candidate) []Candidate {
	out := append([]Candidate(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		left := normalizeCreatedAtUnix(out[i].CreatedAtUnix)
		right := normalizeCreatedAtUnix(out[j].CreatedAtUnix)
		if left != right {
			return left < right
		}
		if out[i].ValueSatoshi != out[j].ValueSatoshi {
			return out[i].ValueSatoshi < out[j].ValueSatoshi
		}
		leftTxID := strings.ToLower(strings.TrimSpace(out[i].TxID))
		rightTxID := strings.ToLower(strings.TrimSpace(out[j].TxID))
		if leftTxID != rightTxID {
			return leftTxID < rightTxID
		}
		return out[i].Vout < out[j].Vout
	})
	return out
}

// FilterEligiblePlainBSV 只保留可以给普通 BSV 业务花费的候选输出。
func FilterEligiblePlainBSV(in []Candidate) Selection {
	sorted := SortOldSmallFirst(in)
	out := Selection{
		Selected: make([]Candidate, 0, len(sorted)),
	}
	for _, item := range sorted {
		switch normalizeProtectionClass(item.ProtectionClass) {
		case ProtectionPlainBSV:
			out.Selected = append(out.Selected, item)
			out.EligibleCount++
		case ProtectionProtectedAsset:
			out.ProtectedCount++
		default:
			out.UnknownCount++
		}
	}
	return out
}

// SelectPlainBSVForTarget 为普通 BSV 支出选择一组可花输出。
// 设计说明：
// - 分配器只从 plain_bsv 候选里选；
// - 选择策略沿用 old-first + small-first，优先消化老、小零钱；
// - 这里不做“协议识别”，识别结果必须由上层先写进 Candidate。
func SelectPlainBSVForTarget(in []Candidate, targetSatoshi uint64) (Selection, error) {
	if targetSatoshi == 0 {
		return Selection{}, fmt.Errorf("target amount must be > 0")
	}
	filtered := FilterEligiblePlainBSV(in)
	if len(filtered.Selected) == 0 {
		return Selection{}, fmt.Errorf("no eligible plain bsv utxos available")
	}
	result := Selection{
		Selected:       make([]Candidate, 0, len(filtered.Selected)),
		EligibleCount:  filtered.EligibleCount,
		ProtectedCount: filtered.ProtectedCount,
		UnknownCount:   filtered.UnknownCount,
	}
	for _, item := range filtered.Selected {
		result.Selected = append(result.Selected, item)
		result.TotalSatoshi += item.ValueSatoshi
		if result.TotalSatoshi >= targetSatoshi {
			return result, nil
		}
	}
	return Selection{}, fmt.Errorf("insufficient eligible plain bsv utxos: have=%d need=%d", result.TotalSatoshi, targetSatoshi)
}

func normalizeProtectionClass(v ProtectionClass) ProtectionClass {
	switch ProtectionClass(strings.ToLower(strings.TrimSpace(string(v)))) {
	case ProtectionPlainBSV:
		return ProtectionPlainBSV
	case ProtectionProtectedAsset:
		return ProtectionProtectedAsset
	default:
		return ProtectionUnknown
	}
}

func normalizeCreatedAtUnix(v int64) int64 {
	if v > 0 {
		return v
	}
	// 未知年龄的输出最后再分，避免新接入识别器前把“没看明白的旧数据”误当老零钱优先花掉。
	return 1<<62 - 1
}
