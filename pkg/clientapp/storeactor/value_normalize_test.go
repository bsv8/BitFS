package sqliteactor

import (
	"bytes"
	"database/sql/driver"
	"math"
	"testing"
	"time"
)

func TestNormalizeDriverValue(t *testing.T) {
	now := time.Unix(123, 456)
	tests := []struct {
		name    string
		in      any
		want    any
		wantErr string
	}{
		{
			name: "uint32",
			in:   uint32(17),
			want: int64(17),
		},
		{
			name: "uint64 within range",
			in:   uint64(math.MaxInt64),
			want: int64(math.MaxInt64),
		},
		{
			name:    "uint64 overflow",
			in:      uint64(math.MaxInt64) + 1,
			wantErr: "sqlite parameter uint64 overflows int64",
		},
		{
			name: "int",
			in:   int(23),
			want: int64(23),
		},
		{
			name: "bytes",
			in:   []byte{0x01, 0x02, 0x03},
			want: []byte{0x01, 0x02, 0x03},
		},
		{
			name: "string",
			in:   "hello",
			want: "hello",
		},
		{
			name: "nil",
			in:   nil,
			want: nil,
		},
		{
			name: "time",
			in:   now,
			want: now,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeDriverValue(tt.in)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if err.Error() != "" && !bytes.Contains([]byte(err.Error()), []byte(tt.wantErr)) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch want := tt.want.(type) {
			case []byte:
				gotBytes, ok := got.([]byte)
				if !ok {
					t.Fatalf("want []byte, got %T", got)
				}
				if !bytes.Equal(gotBytes, want) {
					t.Fatalf("unexpected bytes: got %x want %x", gotBytes, want)
				}
			default:
				if got != tt.want {
					t.Fatalf("unexpected value: got %#v want %#v", got, tt.want)
				}
			}
		})
	}
}

func TestNormalizeNamedValue(t *testing.T) {
	nv := &driver.NamedValue{Ordinal: 1, Value: uint32(9)}
	if err := normalizeNamedValue(nv); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, ok := nv.Value.(int64); !ok || got != 9 {
		t.Fatalf("unexpected named value: %#v", nv.Value)
	}
}
