package clientapp

import (
	"context"
	"testing"
	"time"
)

func TestTaskSchedulerShutdownWaitsForRunningTask(t *testing.T) {
	t.Parallel()

	scheduler := newTaskScheduler(nil, nil, "bitcast-client")
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
