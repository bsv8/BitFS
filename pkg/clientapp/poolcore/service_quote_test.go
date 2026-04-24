package poolcore

import "testing"

func TestListenOfferBudgetToDurationSeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		proposedPayment  uint64
		minimumPayment   uint64
		minimumDuration  uint32
		wantDuration     uint64
		wantErrSubstring string
	}{
		{
			name:            "minimum budget gets minimum duration",
			proposedPayment: 50,
			minimumPayment:  50,
			minimumDuration: 60,
			wantDuration:    60,
		},
		{
			name:            "higher budget buys proportional duration",
			proposedPayment: 125,
			minimumPayment:  50,
			minimumDuration: 60,
			wantDuration:    150,
		},
		{
			name:             "zero budget rejected",
			proposedPayment:  0,
			minimumPayment:   50,
			minimumDuration:  60,
			wantErrSubstring: "proposed payment required",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ListenOfferBudgetToDurationSeconds(tc.proposedPayment, tc.minimumPayment, tc.minimumDuration)
			if tc.wantErrSubstring != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", tc.wantErrSubstring)
				}
				if err.Error() != tc.wantErrSubstring {
					t.Fatalf("error = %q, want %q", err.Error(), tc.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListenOfferBudgetToDurationSeconds() error = %v", err)
			}
			if got != tc.wantDuration {
				t.Fatalf("duration = %d, want %d", got, tc.wantDuration)
			}
		})
	}
}
