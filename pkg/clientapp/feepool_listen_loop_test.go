package clientapp

import (
	"errors"
	"testing"
)

func TestIsWalletInsufficientForListen(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "no_utxos", err: errors.New("no utxos for client address"), want: true},
		{name: "insufficient", err: errors.New("insufficient selected utxos: have=10 target=20"), want: true},
		{name: "not_enough", err: errors.New("not enough funds"), want: true},
		{name: "network_timeout", err: errors.New("rpc timeout"), want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isWalletInsufficientForListen(tc.err)
			if got != tc.want {
				t.Fatalf("isWalletInsufficientForListen()=%v, want %v", got, tc.want)
			}
		})
	}
}
