package clientapp

import "testing"

func TestShouldRunListenBillingLoop(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   feePoolKernelResult
		want bool
	}{
		{
			name: "applied accepted",
			in: feePoolKernelResult{
				Accepted: true,
				Status:   "applied",
			},
			want: true,
		},
		{
			name: "failed accepted",
			in: feePoolKernelResult{
				Accepted: true,
				Status:   "failed",
			},
			want: false,
		},
		{
			name: "paused accepted",
			in: feePoolKernelResult{
				Accepted: true,
				Status:   "paused",
			},
			want: false,
		},
		{
			name: "applied rejected",
			in: feePoolKernelResult{
				Accepted: false,
				Status:   "applied",
			},
			want: false,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := shouldRunListenBillingLoop(tc.in)
			if got != tc.want {
				t.Fatalf("shouldRunListenBillingLoop()=%v, want=%v", got, tc.want)
			}
		})
	}
}

func TestEnsureFeePoolCommandID(t *testing.T) {
	t.Parallel()

	if got := ensureFeePoolCommandID("  cmd-1  "); got != "cmd-1" {
		t.Fatalf("ensureFeePoolCommandID()=%q, want=%q", got, "cmd-1")
	}
	if got := ensureFeePoolCommandID(""); got == "" {
		t.Fatalf("ensureFeePoolCommandID() should never return empty")
	}
}
