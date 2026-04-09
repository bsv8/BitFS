package clientapp

import (
	"context"
	"testing"
	"time"
)

func TestTaskSchedulerShutdownWaitsForRunningTask(t *testing.T) {
	t.Parallel()

	scheduler := newTaskScheduler(nil, "bitcast-client")
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runEntered := make(chan struct{})
	runExited := make(chan struct{})

	if err := scheduler.RegisterPeriodicTask(parentCtx, periodicTaskSpec{
		Name:      "test_wait_shutdown",
		Owner:     "unit_test",
		Mode:      "static",
		Interval:  time.Hour,
		Immediate: true,
		Run: func(ctx context.Context, trigger string) (map[string]any, error) {
			select {
			case <-runEntered:
			default:
				close(runEntered)
			}
			<-ctx.Done()
			time.Sleep(80 * time.Millisecond)
			select {
			case <-runExited:
			default:
				close(runExited)
			}
			return map[string]any{"trigger": trigger}, ctx.Err()
		},
	}); err != nil {
		t.Fatalf("RegisterPeriodicTask() error = %v", err)
	}

	select {
	case <-runEntered:
	case <-time.After(2 * time.Second):
		t.Fatalf("task did not start")
	}

	shutdownDone := make(chan struct{})
	go func() {
		_ = scheduler.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatalf("Shutdown() returned before running task exited")
	case <-time.After(20 * time.Millisecond):
	}

	select {
	case <-runExited:
	case <-time.After(2 * time.Second):
		t.Fatalf("task did not exit after shutdown")
	}

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Shutdown() did not finish after running task exited")
	}
}

func TestTaskSchedulerWritesProfileAndRunRows(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := newClientDB(db, nil)
	scheduler := newTaskScheduler(store, "bitcast-client")

	runDone := make(chan struct{})
	if err := scheduler.RegisterPeriodicTask(context.Background(), periodicTaskSpec{
		Name:      "test_scheduler_write",
		Owner:     "unit_test",
		Mode:      "static",
		Interval:  time.Hour,
		Immediate: true,
		Run: func(ctx context.Context, trigger string) (map[string]any, error) {
			close(runDone)
			return map[string]any{"trigger": trigger}, nil
		},
	}); err != nil {
		t.Fatalf("RegisterPeriodicTask() error = %v", err)
	}

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("task did not run")
	}

	if err := scheduler.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	var status string
	var intervalSeconds, closedAtUnix, lastStartedAtUnix, lastEndedAtUnix, lastDurationMS, runCount, successCount, failureCount int64
	if err := db.QueryRow(
		`SELECT status,interval_seconds,closed_at_unix,last_started_at_unix,last_ended_at_unix,last_duration_ms,run_count,success_count,failure_count
		 FROM proc_scheduler_tasks WHERE task_name=?`,
		"test_scheduler_write",
	).Scan(&status, &intervalSeconds, &closedAtUnix, &lastStartedAtUnix, &lastEndedAtUnix, &lastDurationMS, &runCount, &successCount, &failureCount); err != nil {
		t.Fatalf("load scheduler task row failed: %v", err)
	}
	if status != "stopped" {
		t.Fatalf("status mismatch: got=%s want=stopped", status)
	}
	if intervalSeconds != int64(time.Hour/time.Second) {
		t.Fatalf("interval_seconds mismatch: got=%d", intervalSeconds)
	}
	if closedAtUnix <= 0 || lastStartedAtUnix <= 0 || lastEndedAtUnix <= 0 {
		t.Fatalf("unexpected task timestamps: closed=%d started=%d ended=%d", closedAtUnix, lastStartedAtUnix, lastEndedAtUnix)
	}
	if lastDurationMS < 0 {
		t.Fatalf("last_duration_ms must not be negative: %d", lastDurationMS)
	}
	if runCount != 1 || successCount != 1 || failureCount != 0 {
		t.Fatalf("run counters mismatch: run=%d success=%d failure=%d", runCount, successCount, failureCount)
	}

	var runRows int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM proc_scheduler_task_runs WHERE task_name=?`, "test_scheduler_write").Scan(&runRows); err != nil {
		t.Fatalf("count task runs failed: %v", err)
	}
	if runRows != 1 {
		t.Fatalf("task run rows mismatch: got=%d want=1", runRows)
	}
}
