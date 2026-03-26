package clientapp

import "testing"

func TestEstimateMinerFeeSatPerKB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		sizeBytes       int
		feeRateSatPerKB float64
		want            uint64
	}{
		{name: "round_up_fractional_fee", sizeBytes: 2500, feeRateSatPerKB: 0.5, want: 2},
		{name: "minimum_one_sat", sizeBytes: 300, feeRateSatPerKB: 0.5, want: 1},
		{name: "non_positive_rate_falls_back_to_one", sizeBytes: 300, feeRateSatPerKB: 0, want: 1},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := estimateMinerFeeSatPerKB(tt.sizeBytes, tt.feeRateSatPerKB); got != tt.want {
				t.Fatalf("estimateMinerFeeSatPerKB() = %d, want %d", got, tt.want)
			}
		})
	}
}
