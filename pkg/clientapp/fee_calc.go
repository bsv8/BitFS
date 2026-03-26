package clientapp

import "math"

// estimateMinerFeeSatPerKB 统一按 sat/KB 口径估算手工构造交易的矿工费。
// 设计说明：
// - BitFS 里少数交易是本地临时拼出来的，容易被读代码的人误解成 sat/B；
// - 这里显式写成“size / 1000 * sat_per_kb”，把费率单位钉死在代码里；
// - 结果向上取整，避免出现非零费率却因为小数被截断而低估。
func estimateMinerFeeSatPerKB(sizeBytes int, feeRateSatPerKB float64) uint64 {
	if sizeBytes <= 0 {
		return 1
	}
	if feeRateSatPerKB <= 0 {
		return 1
	}
	fee := uint64(math.Ceil((float64(sizeBytes) / 1000.0) * feeRateSatPerKB))
	if fee == 0 {
		return 1
	}
	return fee
}
