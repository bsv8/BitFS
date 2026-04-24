package fundalloc

import "testing"

func TestSelectPlainBSVForTarget_ExcludesProtectedAndUnknown(t *testing.T) {
	t.Parallel()

	selected, err := SelectPlainBSVForTarget([]Candidate{
		{ID: "p", TxID: "p", Vout: 0, ValueSatoshi: 1, CreatedAtUnix: 1, ProtectionClass: ProtectionProtectedAsset},
		{ID: "u", TxID: "u", Vout: 0, ValueSatoshi: 2, CreatedAtUnix: 2, ProtectionClass: ProtectionUnknown},
		{ID: "a", TxID: "a", Vout: 0, ValueSatoshi: 3, CreatedAtUnix: 3, ProtectionClass: ProtectionPlainBSV},
		{ID: "b", TxID: "b", Vout: 0, ValueSatoshi: 5, CreatedAtUnix: 4, ProtectionClass: ProtectionPlainBSV},
	}, 6)
	if err != nil {
		t.Fatalf("SelectPlainBSVForTarget: %v", err)
	}
	if got, want := len(selected.Selected), 2; got != want {
		t.Fatalf("selected count mismatch: got=%d want=%d", got, want)
	}
	if got, want := selected.TotalSatoshi, uint64(8); got != want {
		t.Fatalf("selected total mismatch: got=%d want=%d", got, want)
	}
	if got, want := selected.ProtectedCount, 1; got != want {
		t.Fatalf("protected count mismatch: got=%d want=%d", got, want)
	}
	if got, want := selected.UnknownCount, 1; got != want {
		t.Fatalf("unknown count mismatch: got=%d want=%d", got, want)
	}
}

func TestSortOldSmallFirst_PrefersAgeBeforeAmount(t *testing.T) {
	t.Parallel()

	sorted := SortOldSmallFirst([]Candidate{
		{ID: "new-small", TxID: "b", Vout: 0, ValueSatoshi: 1, CreatedAtUnix: 20, ProtectionClass: ProtectionPlainBSV},
		{ID: "old-large", TxID: "a", Vout: 0, ValueSatoshi: 9, CreatedAtUnix: 10, ProtectionClass: ProtectionPlainBSV},
		{ID: "old-small", TxID: "c", Vout: 0, ValueSatoshi: 2, CreatedAtUnix: 10, ProtectionClass: ProtectionPlainBSV},
	})
	if got, want := sorted[0].ID, "old-small"; got != want {
		t.Fatalf("sorted[0] mismatch: got=%s want=%s", got, want)
	}
	if got, want := sorted[1].ID, "old-large"; got != want {
		t.Fatalf("sorted[1] mismatch: got=%s want=%s", got, want)
	}
	if got, want := sorted[2].ID, "new-small"; got != want {
		t.Fatalf("sorted[2] mismatch: got=%s want=%s", got, want)
	}
}
