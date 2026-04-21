package clientapp

import (
	"context"
	"fmt"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileOrderAdapter struct {
	store       *clientDB
	ownerPubHex string
}

func newDownloadFileOrderAdapter(store *clientDB, ownerPubHex ...string) *downloadFileOrderAdapter {
	if store == nil {
		return nil
	}
	owner := ""
	if len(ownerPubHex) > 0 {
		owner = strings.ToLower(strings.TrimSpace(ownerPubHex[0]))
	}
	return &downloadFileOrderAdapter{
		store:       store,
		ownerPubHex: owner,
	}
}

func (a *downloadFileOrderAdapter) EnsureFrontOrder(ctx context.Context, req filedownload.FrontOrderRequest) (string, error) {
	if a == nil || a.store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	jobID := strings.TrimSpace(req.JobID)
	if jobID == "" {
		return "", fmt.Errorf("job_id is required")
	}
	seedHash := normalizeSeedHashHex(req.SeedHash)
	if seedHash == "" {
		return "", fmt.Errorf("seed_hash is required")
	}
	frontOrderID := strings.TrimSpace(req.FrontOrderID)
	if frontOrderID == "" {
		frontOrderID = buildDownloadFrontOrderID(jobID, seedHash)
	}
	if err := dbUpsertFrontOrder(ctx, a.store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "getfilebyhash",
		OwnerPubkeyHex:   firstNonEmpty(strings.ToLower(strings.TrimSpace(req.OwnerPubkeyHex)), a.ownerPubHex),
		TargetObjectType: "seed",
		TargetObjectID:   strings.TrimSpace(req.TargetObjectID),
		Status:           "pending",
		Note:             strings.TrimSpace(req.Note),
		Payload: map[string]any{
			"job_id":           jobID,
			"seed_hash":        seedHash,
			"target_object_id": strings.TrimSpace(req.TargetObjectID),
			"front_order_id":   frontOrderID,
		},
	}); err != nil {
		return "", err
	}
	return frontOrderID, nil
}

func (a *downloadFileOrderAdapter) BindJobFrontOrder(ctx context.Context, jobID string, frontOrderID string) error {
	if a == nil || a.store == nil {
		return fmt.Errorf("client db is nil")
	}
	jobID = strings.TrimSpace(jobID)
	frontOrderID = strings.TrimSpace(frontOrderID)
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}
	if frontOrderID == "" {
		return fmt.Errorf("front_order_id is required")
	}
	job, found := newDownloadFileJobStoreAdapter(a.store).GetJob(ctx, jobID)
	if !found {
		return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
	}
	if current := strings.TrimSpace(job.FrontOrderID); current != "" && !strings.EqualFold(current, frontOrderID) {
		return fmt.Errorf("front_order_id already bound: %s", current)
	}
	if err := dbUpsertFrontOrder(ctx, a.store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "getfilebyhash",
		OwnerPubkeyHex:   a.ownerPubHex,
		TargetObjectType: "seed",
		TargetObjectID:   strings.TrimSpace(job.SeedHash),
		Status:           "pending",
		Note:             "getfilebyhash",
		Payload: map[string]any{
			"job_id":         jobID,
			"seed_hash":      strings.TrimSpace(job.SeedHash),
			"front_order_id": frontOrderID,
		},
	}); err != nil {
		return err
	}
	return a.setJobFrontOrder(ctx, jobID, frontOrderID)
}

func (a *downloadFileOrderAdapter) setJobFrontOrder(ctx context.Context, jobID string, frontOrderID string) error {
	return newDownloadFileJobStoreAdapter(a.store).SetFrontOrderID(ctx, jobID, frontOrderID)
}

func buildDownloadFrontOrderID(jobID string, seedHash string) string {
	return "fo_download_" + strings.TrimSpace(jobID)
}

var _ filedownload.FrontOrderBinder = (*downloadFileOrderAdapter)(nil)
