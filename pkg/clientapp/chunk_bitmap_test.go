package clientapp

import (
	"reflect"
	"testing"
)

func TestChunkBitmapHexFromIndexesBTBitOrder(t *testing.T) {
	t.Parallel()

	got := chunkBitmapHexFromIndexes([]uint32{0, 2, 3, 7, 8}, 10)
	// 第 0 块在 bit7，第 8 块在下一个字节 bit7。
	const want = "b180"
	if got != want {
		t.Fatalf("bitmap mismatch: got=%s want=%s", got, want)
	}
}

func TestChunkIndexesFromBitmapHexBTBitOrder(t *testing.T) {
	t.Parallel()

	got, err := chunkIndexesFromBitmapHex("b180", 10)
	if err != nil {
		t.Fatalf("decode bitmap: %v", err)
	}
	want := []uint32{0, 2, 3, 7, 8}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("indexes mismatch: got=%v want=%v", got, want)
	}
}

func TestChunkBitmapRoundTrip(t *testing.T) {
	t.Parallel()

	in := []uint32{0, 1, 5, 8, 9, 15}
	bitmap := chunkBitmapHexFromIndexes(in, 16)
	got, err := chunkIndexesFromBitmapHex(bitmap, 16)
	if err != nil {
		t.Fatalf("round trip decode: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip mismatch: got=%v want=%v", got, in)
	}
}
