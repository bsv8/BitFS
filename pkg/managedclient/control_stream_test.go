package managedclient

import (
	"testing"

	"github.com/bsv8/BFTP/pkg/obs"
)

func TestMapManagedObsEvents_WorkspaceChangedOnlyForMutationEvents(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		want bool
	}{
		{name: "workspace_scanned", want: true},
		{name: "evt_trigger_workspace_list_begin", want: false},
		{name: "evt_trigger_workspace_list_end", want: false},
		{name: "evt_trigger_workspace_list_failed", want: false},
		{name: "evt_trigger_workspace_add_begin", want: false},
		{name: "evt_trigger_workspace_add_failed", want: false},
		{name: "evt_trigger_workspace_add_end", want: true},
		{name: "evt_trigger_workspace_update_begin", want: false},
		{name: "evt_trigger_workspace_update_failed", want: false},
		{name: "evt_trigger_workspace_update_end", want: true},
		{name: "evt_trigger_workspace_delete_begin", want: false},
		{name: "evt_trigger_workspace_delete_failed", want: false},
		{name: "evt_trigger_workspace_delete_end", want: true},
		{name: "evt_trigger_workspace_sync_once_begin", want: false},
		{name: "evt_trigger_workspace_sync_once_failed", want: false},
		{name: "evt_trigger_workspace_sync_once_end", want: true},
	}

	for _, tc := range cases {
		if got := isWorkspaceChangedObsEvent(tc.name); got != tc.want {
			t.Fatalf("event %q mapped=%v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestMapManagedObsEvents_WorkspaceListDoesNotBridgeChanged(t *testing.T) {
	t.Parallel()

	mapped := mapManagedObsEvents(obs.Event{
		Level:   obs.LevelBusiness,
		Service: obs.ServiceBitFSClient,
		Name:    "evt_trigger_workspace_list_begin",
	})
	if len(mapped) != 0 {
		t.Fatalf("workspace list should not bridge to resource.workspace.changed: %+v", mapped)
	}
}

func TestMapManagedObsEvents_WorkspaceMutationBridgesChanged(t *testing.T) {
	t.Parallel()

	mapped := mapManagedObsEvents(obs.Event{
		Level:   obs.LevelBusiness,
		Service: obs.ServiceBitFSClient,
		Name:    "evt_trigger_workspace_add_end",
	})
	if len(mapped) != 1 {
		t.Fatalf("workspace mutation should bridge once, got=%d", len(mapped))
	}
	if got, want := mapped[0].Topic, "resource.workspace.changed"; got != want {
		t.Fatalf("topic=%q, want %q", got, want)
	}
}

func TestMapManagedObsEvents_WorkspaceBeginAndFailedDoNotBridgeChanged(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"evt_trigger_workspace_add_begin",
		"evt_trigger_workspace_add_failed",
		"evt_trigger_workspace_update_begin",
		"evt_trigger_workspace_update_failed",
		"evt_trigger_workspace_delete_begin",
		"evt_trigger_workspace_delete_failed",
		"evt_trigger_workspace_sync_once_begin",
		"evt_trigger_workspace_sync_once_failed",
	} {
		mapped := mapManagedObsEvents(obs.Event{
			Level:   obs.LevelBusiness,
			Service: obs.ServiceBitFSClient,
			Name:    name,
		})
		if len(mapped) != 0 {
			t.Fatalf("%s should not bridge to resource.workspace.changed: %+v", name, mapped)
		}
	}
}
