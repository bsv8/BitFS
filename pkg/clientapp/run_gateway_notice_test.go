package clientapp

import "testing"

func TestNoEnabledGatewayMessage(t *testing.T) {
	t.Helper()

	cases := []struct {
		name           string
		httpAPIEnabled bool
		fsHTTPEnabled  bool
		want           string
	}{
		{
			name:           "http_api_enabled",
			httpAPIEnabled: true,
			fsHTTPEnabled:  false,
			want:           "no enabled gateway, waiting for HTTP API configuration",
		},
		{
			name:           "http_api_disabled_fs_http_enabled",
			httpAPIEnabled: false,
			fsHTTPEnabled:  true,
			want:           "no enabled gateway, HTTP API disabled; waiting external gateway config or runner injection",
		},
		{
			name:           "http_api_disabled_fs_http_disabled",
			httpAPIEnabled: false,
			fsHTTPEnabled:  false,
			want:           "no enabled gateway, HTTP API and FS HTTP disabled; waiting external gateway config or runner injection",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			got := noEnabledGatewayMessage(tc.httpAPIEnabled, tc.fsHTTPEnabled)
			if got != tc.want {
				t.Fatalf("message mismatch: got=%q want=%q", got, tc.want)
			}
		})
	}
}
