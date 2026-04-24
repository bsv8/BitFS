package obs

import "testing"

func TestFormatConsoleLine(t *testing.T) {
	ev := Event{
		Level:    LevelError,
		Category: CategorySystem,
		Service:  ServiceBitFSClient,
		Name:     "task_scheduler_shutdown_failed",
		Fields: map[string]any{
			"error":   "context canceled",
			"attempt": 2,
		},
	}
	got := formatConsoleLine(ev)
	want := `[error][system][` + ServiceBitFSClient + `] task_scheduler_shutdown_failed attempt=2 error="context canceled"`
	if got != want {
		t.Fatalf("console line mismatch\nwant: %s\ngot:  %s", want, got)
	}
}
