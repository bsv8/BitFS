package clientapp

import "testing"

func TestOrchestratorIsRetryableFailure(t *testing.T) {
	t.Parallel()
	o := newOrchestrator(&Runtime{})
	if o == nil {
		t.Fatal("newOrchestrator returned nil")
	}

	cases := []struct {
		name string
		in   clientKernelResult
		want bool
	}{
		{name: "session_missing", in: clientKernelResult{ErrorCode: "session_missing"}, want: false},
		{name: "wallet_insufficient", in: clientKernelResult{ErrorCode: "wallet_insufficient"}, want: false},
		{name: "wallet_insufficient_paused", in: clientKernelResult{ErrorCode: "wallet_insufficient_paused"}, want: false},
		{name: "rpc_failed", in: clientKernelResult{ErrorCode: "fee_pool_info_failed"}, want: true},
		{name: "empty_code", in: clientKernelResult{}, want: true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := o.isRetryableFailure(tc.in)
			if got != tc.want {
				t.Fatalf("isRetryableFailure()=%v, want=%v", got, tc.want)
			}
		})
	}
}
