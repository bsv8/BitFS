package clientapp

import (
	"encoding/json"
	"time"
)

type fileChunkOrderSnapshot struct {
	Name               string `json:"name"`
	ActiveDemandChunks uint32 `json:"active_demand_chunks"`
	FocusChunk         uint32 `json:"focus_chunk"`
	PrefetchCursor     uint32 `json:"prefetch_cursor"`
	PrefetchDistance   uint32 `json:"prefetch_distance"`
	LastSelectedChunk  uint32 `json:"last_selected_chunk"`
	LastSelectPhase    string `json:"last_select_phase"`
	LastSelectReason   string `json:"last_select_reason"`
	UpdatedAtUnix      int64  `json:"updated_at_unix"`
}

type fileDownloadRuntimeState struct {
	Strategy      speedPriceSchedulerState `json:"strategy"`
	ChunkPolicy   fileChunkOrderSnapshot   `json:"chunk_policy"`
	UpdatedAtUnix int64                    `json:"updated_at_unix"`
}

func defaultFileDownloadRuntimeState() fileDownloadRuntimeState {
	now := time.Now().Unix()
	return fileDownloadRuntimeState{
		Strategy: speedPriceSchedulerState{
			StrategyName:  TransferStrategySmart,
			StartedAtUnix: now,
			UpdatedAtUnix: now,
		},
		ChunkPolicy: fileChunkOrderSnapshot{
			Name:          "demand_prefetch_backfill",
			UpdatedAtUnix: now,
		},
		UpdatedAtUnix: now,
	}
}

func decodeFileDownloadRuntimeState(raw string) fileDownloadRuntimeState {
	state := defaultFileDownloadRuntimeState()
	if raw == "" {
		return state
	}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return defaultFileDownloadRuntimeState()
	}
	if state.UpdatedAtUnix == 0 {
		state.UpdatedAtUnix = time.Now().Unix()
	}
	if state.Strategy.StrategyName == "" {
		state.Strategy.StrategyName = TransferStrategySmart
	}
	if state.ChunkPolicy.Name == "" {
		state.ChunkPolicy.Name = "demand_prefetch_backfill"
	}
	return state
}

func encodeFileDownloadRuntimeState(state fileDownloadRuntimeState) string {
	b, err := json.Marshal(state)
	if err != nil {
		return "{}"
	}
	return string(b)
}
