package clientapp

import "testing"

func TestShouldRunListenBillingLoop(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   clientKernelResult
		want bool
	}{
		{
			name: "applied accepted",
			in: clientKernelResult{
				Accepted: true,
				Status:   "applied",
			},
			want: true,
		},
		{
			name: "failed accepted",
			in: clientKernelResult{
				Accepted: true,
				Status:   "failed",
			},
			want: false,
		},
		{
			name: "paused accepted",
			in: clientKernelResult{
				Accepted: true,
				Status:   "paused",
			},
			want: false,
		},
		{
			name: "applied rejected",
			in: clientKernelResult{
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
