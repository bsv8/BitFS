package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

func newDownloadFileHTTPTestHandler(store *clientDB) *downloadFileHTTPHandler {
	caps := &DownloadFileCaps{
		store:    store,
		jobStore: newDownloadFileJobStoreAdapter(store),
		fileStore: &httpTestDownloadFileStore{
			completeFound: true,
			complete: filedownload.LocalFile{
				FilePath: "/tmp/getfilebyhash-local.bin",
				FileSize: 123,
				SeedHash: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			},
		},
	}
	return newDownloadFileHTTPHandler(caps)
}

type httpTestDownloadFileStore struct {
	completeFound bool
	complete      filedownload.LocalFile
	completeErr   error
	completeCalls int
	lastComplete  filedownload.CompleteFileInput
}

func (s *httpTestDownloadFileStore) FindCompleteFile(ctx context.Context, seedHash string) (filedownload.LocalFile, bool, error) {
	return s.complete, s.completeFound, nil
}

func (s *httpTestDownloadFileStore) PreparePartFile(ctx context.Context, input filedownload.PreparePartFileInput) (filedownload.PartFile, error) {
	return filedownload.PartFile{PartFilePath: "/tmp/part-" + input.SeedHash[:8] + ".bin", SeedHash: input.SeedHash}, nil
}

func (s *httpTestDownloadFileStore) MarkChunkStored(ctx context.Context, input filedownload.StoredChunkInput) error {
	return nil
}

func (s *httpTestDownloadFileStore) CompleteFile(ctx context.Context, input filedownload.CompleteFileInput) (filedownload.LocalFile, error) {
	s.completeCalls++
	s.lastComplete = input
	if s.completeErr != nil {
		return filedownload.LocalFile{}, s.completeErr
	}
	return s.complete, nil
}

type httpTestDownloadSeedStore struct {
	meta  filedownload.SeedMeta
	found bool
}

func (s httpTestDownloadSeedStore) SaveSeed(ctx context.Context, input filedownload.SaveSeedInput) error {
	return filedownload.NewError(filedownload.CodeModuleDisabled, "seed store is not available")
}

func (s httpTestDownloadSeedStore) LoadSeedMeta(ctx context.Context, seedHash string) (filedownload.SeedMeta, bool, error) {
	return s.meta, s.found, nil
}

type httpTestDownloadDemandPublisher struct {
	result filedownload.PublishDemandResult
}

func (p httpTestDownloadDemandPublisher) PublishDemand(ctx context.Context, req filedownload.PublishDemandRequest) (filedownload.PublishDemandResult, error) {
	return p.result, nil
}

type httpTestDownloadQuoteReader struct {
	quotes []filedownload.QuoteReport
}

func (r httpTestDownloadQuoteReader) ListQuotes(ctx context.Context, demandID string) ([]filedownload.QuoteReport, error) {
	return append([]filedownload.QuoteReport(nil), r.quotes...), nil
}

type httpTestDownloadPolicy struct {
	selected filedownload.QuoteReport
}

func (p httpTestDownloadPolicy) SelectQuote(ctx context.Context, req filedownload.StartRequest, quotes []filedownload.QuoteReport) (filedownload.QuoteReport, bool, string, error) {
	if p.selected.SellerPubkey == "" && len(quotes) > 0 {
		return quotes[0], true, "", nil
	}
	return p.selected, true, "", nil
}

type httpTestDownloadTransferRunner struct {
	results map[uint32]filedownload.ChunkTransferResult
	calls   []filedownload.ChunkTransferRequest
}

func (r *httpTestDownloadTransferRunner) RunChunkTransfer(ctx context.Context, req filedownload.ChunkTransferRequest) (filedownload.ChunkTransferResult, error) {
	r.calls = append(r.calls, req)
	if res, ok := r.results[req.ChunkIndex]; ok {
		return res, nil
	}
	return filedownload.ChunkTransferResult{
		Data:     []byte("chunk"),
		PaidSat:  req.ChunkPriceSat,
		SpeedBps: 1000,
	}, nil
}

func TestHTTPHandlerHandleStartPOST(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	reqBody := `{"seed_hash":"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","chunk_count":10}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(reqBody))
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStart(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			JobID   string `json:"job_id"`
			Message string `json:"message,omitempty"`
			Status  struct {
				JobID          string `json:"job_id"`
				SeedHash       string `json:"seed_hash"`
				State          string `json:"state"`
				OutputFilePath string `json:"output_file_path,omitempty"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v, body: %s", err, rec.Body.String())
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
	if resp.Data.Status.SeedHash != "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2" {
		t.Fatalf("seed_hash mismatch: got %q (len=%d)", resp.Data.Status.SeedHash, len(resp.Data.Status.SeedHash))
	}
	if resp.Data.Status.State != filedownload.StateLocal {
		t.Fatalf("expected state=%s, got %s", filedownload.StateLocal, resp.Data.Status.State)
	}
	if resp.Data.Status.OutputFilePath == "" {
		t.Fatalf("expected output_file_path to be set")
	}
}

func TestHTTPRouteGetFileByHashKeepsNewEntryAndRemovesOldOne(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	srv := &httpAPIServer{
		downloadFileHandler: newDownloadFileHTTPTestHandler(store),
	}
	mux, err := srv.buildMux()
	if err != nil {
		t.Fatalf("build mux: %v", err)
	}

	newReq := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(`{}`))
	newRec := httptest.NewRecorder()
	mux.ServeHTTP(newRec, newReq)
	if newRec.Code == http.StatusNotFound {
		t.Fatalf("new getfilebyhash route should stay registered")
	}

	oldReq := httptest.NewRequest(http.MethodPost, "/api/v1/files/get-file", strings.NewReader(`{}`))
	oldRec := httptest.NewRecorder()
	mux.ServeHTTP(oldRec, oldReq)
	if oldRec.Code != http.StatusNotFound {
		t.Fatalf("old get-file route should be removed: got=%d body=%s", oldRec.Code, oldRec.Body.String())
	}
}

func TestHTTPHandlerHandleStartDoneAfterTransfer(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	seedHash := "9999999999999999999999999999999999999999999999999999999999999999"
	files := &httpTestDownloadFileStore{
		completeFound: false,
		complete: filedownload.LocalFile{
			FilePath: "/tmp/http-final.bin",
			FileSize: 8,
			SeedHash: seedHash,
		},
	}
	caps := &DownloadFileCaps{
		jobStore:        newDownloadFileJobStoreAdapter(store),
		seedStore:       httpTestDownloadSeedStore{meta: filedownload.SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		fileStore:       files,
		demandPublisher: httpTestDownloadDemandPublisher{result: filedownload.PublishDemandResult{DemandID: "demand_http_done", Status: "submitted"}},
		quoteReader:     httpTestDownloadQuoteReader{quotes: []filedownload.QuoteReport{{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}}},
		policy:          httpTestDownloadPolicy{selected: filedownload.QuoteReport{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}},
		transferRunner: &httpTestDownloadTransferRunner{results: map[uint32]filedownload.ChunkTransferResult{
			0: {Data: []byte("aaaa"), PaidSat: 10, SpeedBps: 1000},
			1: {Data: []byte("bbbb"), PaidSat: 10, SpeedBps: 1200},
		}},
	}
	handler := newDownloadFileHTTPHandler(caps)

	reqBody := `{"seed_hash":"` + seedHash + `","chunk_count":2}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(reqBody))
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStart(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Status struct {
				State          string `json:"state"`
				OutputFilePath string `json:"output_file_path,omitempty"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v, body: %s", err, rec.Body.String())
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
	if resp.Data.Status.State != filedownload.StateDone {
		t.Fatalf("expected state=%s, got %s", filedownload.StateDone, resp.Data.Status.State)
	}
	if resp.Data.Status.OutputFilePath != "/tmp/http-final.bin" {
		t.Fatalf("expected output_file_path=/tmp/http-final.bin, got %s", resp.Data.Status.OutputFilePath)
	}
	if files.completeCalls != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalls)
	}
}

func TestHTTPHandlerHandleStartResumeRunningJob(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	jobStore := newDownloadFileJobStoreAdapter(store)
	seedHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	jobID := "http_job_resume"

	ctx := context.Background()
	if _, created, err := jobStore.CreateJob(ctx, &filedownload.Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      filedownload.StateRunning,
		ChunkCount: 2,
		DemandID:   "demand_http_resume",
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	} else if !created {
		t.Fatalf("expected job to be created")
	}
	if err := jobStore.SetPartFilePath(ctx, jobID, "/tmp/http-resume.part"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}
	if err := jobStore.AppendQuote(ctx, jobID, filedownload.QuoteReport{
		SellerPubkey:        "seller_http",
		SeedPriceSat:        100,
		ChunkPriceSat:       10,
		ChunkCount:          2,
		FileSizeBytes:       8,
		RecommendedFileName: "http.bin",
		MimeType:            "application/octet-stream",
		Selected:            true,
	}); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}
	if err := jobStore.AppendChunkReport(ctx, jobID, filedownload.ChunkReport{
		ChunkIndex:    0,
		State:         filedownload.ChunkStateStored,
		SellerPubkey:  "seller_http",
		ChunkPriceSat: 10,
		SpeedBps:      1000,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append stored chunk failed: %v", err)
	}

	files := &httpTestDownloadFileStore{
		completeFound: false,
		complete: filedownload.LocalFile{
			FilePath: "/tmp/http-resume-final.bin",
			FileSize: 8,
			SeedHash: seedHash,
		},
	}
	transfer := &httpTestDownloadTransferRunner{
		results: map[uint32]filedownload.ChunkTransferResult{
			1: {Data: []byte("bbbb"), PaidSat: 10, SpeedBps: 1200},
		},
	}
	caps := &DownloadFileCaps{
		store:          store,
		jobStore:       jobStore,
		seedStore:      httpTestDownloadSeedStore{meta: filedownload.SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		fileStore:      files,
		quoteReader:    httpTestDownloadQuoteReader{quotes: []filedownload.QuoteReport{{SellerPubkey: "seller_http", SeedPriceSat: 100, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "http.bin", MimeType: "application/octet-stream", Selected: true}}},
		transferRunner: transfer,
	}
	handler := newDownloadFileHTTPHandler(caps)

	reqBody := `{"seed_hash":"` + seedHash + `","chunk_count":2}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(reqBody))
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStart(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Status struct {
				State          string `json:"state"`
				OutputFilePath string `json:"output_file_path,omitempty"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v, body: %s", err, rec.Body.String())
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
	if resp.Data.Status.State != filedownload.StateDone {
		t.Fatalf("expected state=%s, got %s", filedownload.StateDone, resp.Data.Status.State)
	}
	if resp.Data.Status.OutputFilePath != "/tmp/http-resume-final.bin" {
		t.Fatalf("expected output_file_path=/tmp/http-resume-final.bin, got %s", resp.Data.Status.OutputFilePath)
	}
	if files.completeCalls != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalls)
	}
	if len(transfer.calls) != 1 {
		t.Fatalf("expected one transfer call, got %d", len(transfer.calls))
	}
	if transfer.calls[0].ChunkIndex != 1 {
		t.Fatalf("expected transferred chunk 1, got %d", transfer.calls[0].ChunkIndex)
	}
}

func TestHTTPHandlerHandleStartMethodNotAllowed(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStart(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d: %s", http.StatusMethodNotAllowed, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "error" {
		t.Fatalf("expected status=error, got %s", resp.Status)
	}
	if resp.Error.Code != "METHOD_NOT_ALLOWED" {
		t.Fatalf("expected code=METHOD_NOT_ALLOWED, got %s", resp.Error.Code)
	}
}

func TestHTTPHandlerHandleStartInvalidJSON(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(`{invalid json}`))
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStart(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "error" {
		t.Fatalf("expected status=error, got %s", resp.Status)
	}
	if resp.Error.Code != "BAD_REQUEST" {
		t.Fatalf("expected code=BAD_REQUEST, got %s", resp.Error.Code)
	}
}

func TestHTTPHandlerHandleStartModuleDisabled(t *testing.T) {
	t.Parallel()

	handler := newDownloadFileHTTPHandler(&DownloadFileCaps{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(`{"seed_hash":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","chunk_count":1}`))
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStart(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d: %s", http.StatusInternalServerError, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "error" {
		t.Fatalf("expected status=error, got %s", resp.Status)
	}
	if resp.Error.Code != filedownload.CodeModuleDisabled {
		t.Fatalf("expected code=%s, got %s", filedownload.CodeModuleDisabled, resp.Error.Code)
	}
}

func TestHTTPHandlerHandleStatusGET(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	startReq := `{"seed_hash":"b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","chunk_count":5}`
	startRec := httptest.NewRecorder()
	startHTTPReq := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(startReq))
	startHTTPReq = startHTTPReq.WithContext(context.Background())
	handler.handleStart(startRec, startHTTPReq)

	var startResp struct {
		Status string `json:"status"`
		Data   struct {
			JobID  string `json:"job_id"`
			Status struct {
				JobID string `json:"job_id"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(startRec.Body.Bytes(), &startResp); err != nil {
		t.Fatalf("decode start response: %v, body: %s", err, startRec.Body.String())
	}
	jobID := startResp.Data.JobID
	if jobID == "" {
		jobID = startResp.Data.Status.JobID
	}
	if jobID == "" {
		t.Fatalf("job_id is empty, body: %s", startRec.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash/status?job_id="+jobID, nil)
	statusReq = statusReq.WithContext(context.Background())
	statusRec := httptest.NewRecorder()

	handler.handleStatus(statusRec, statusReq)

	if statusRec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, statusRec.Code, statusRec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			JobID    string `json:"job_id"`
			SeedHash string `json:"seed_hash"`
			State    string `json:"state"`
		} `json:"data"`
	}
	if err := json.Unmarshal(statusRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
	if resp.Data.JobID != jobID {
		t.Fatalf("expected job_id=%s, got %s", jobID, resp.Data.JobID)
	}
}

func TestHTTPHandlerHandleStatusMethodNotAllowed(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash/status?job_id=test_job", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStatus(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHTTPHandlerHandleStatusMissingJobID(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash/status", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStatus(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "error" {
		t.Fatalf("expected status=error, got %s", resp.Status)
	}
	if resp.Error.Code != "BAD_REQUEST" {
		t.Fatalf("expected code=BAD_REQUEST, got %s", resp.Error.Code)
	}
}

func TestHTTPHandlerHandleStatusJobNotFound(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash/status?job_id=nonexistent", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleStatus(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNotFound, rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "error" {
		t.Fatalf("expected status=error, got %s", resp.Status)
	}
	if resp.Error.Code != filedownload.CodeJobNotFound {
		t.Fatalf("expected code=%s, got %s", filedownload.CodeJobNotFound, resp.Error.Code)
	}
}

func TestHTTPHandlerHandleChunksGET(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	startReq := `{"seed_hash":"c1c2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","chunk_count":3}`
	startRec := httptest.NewRecorder()
	startHTTPReq := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(startReq))
	startHTTPReq = startHTTPReq.WithContext(context.Background())
	handler.handleStart(startRec, startHTTPReq)

	var startResp struct {
		Status string `json:"status"`
		Data   struct {
			JobID  string `json:"job_id"`
			Status struct {
				JobID string `json:"job_id"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(startRec.Body.Bytes(), &startResp); err != nil {
		t.Fatalf("decode start response: %v, body: %s", err, startRec.Body.String())
	}
	jobID := startResp.Data.JobID
	if jobID == "" {
		jobID = startResp.Data.Status.JobID
	}
	if jobID == "" {
		t.Fatalf("job_id is empty, body: %s", startRec.Body.String())
	}

	chunksReq := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash/chunks?job_id="+jobID, nil)
	chunksReq = chunksReq.WithContext(context.Background())
	chunksRec := httptest.NewRecorder()

	handler.handleChunks(chunksRec, chunksReq)

	if chunksRec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, chunksRec.Code, chunksRec.Body.String())
	}

	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(chunksRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
}

func TestHTTPHandlerHandleNodesGET(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	startReq := `{"seed_hash":"d1d2d3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","chunk_count":2}`
	startRec := httptest.NewRecorder()
	startHTTPReq := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(startReq))
	startHTTPReq = startHTTPReq.WithContext(context.Background())
	handler.handleStart(startRec, startHTTPReq)

	var startResp struct {
		Status string `json:"status"`
		Data   struct {
			JobID  string `json:"job_id"`
			Status struct {
				JobID string `json:"job_id"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(startRec.Body.Bytes(), &startResp); err != nil {
		t.Fatalf("decode start response: %v, body: %s", err, startRec.Body.String())
	}
	jobID := startResp.Data.JobID
	if jobID == "" {
		jobID = startResp.Data.Status.JobID
	}
	if jobID == "" {
		t.Fatalf("job_id is empty, body: %s", startRec.Body.String())
	}

	nodesReq := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash/nodes?job_id="+jobID, nil)
	nodesReq = nodesReq.WithContext(context.Background())
	nodesRec := httptest.NewRecorder()

	handler.handleNodes(nodesRec, nodesReq)

	if nodesRec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, nodesRec.Code, nodesRec.Body.String())
	}

	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(nodesRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
}

func TestHTTPHandlerHandleQuotesGET(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	startReq := `{"seed_hash":"e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","chunk_count":5}`
	startRec := httptest.NewRecorder()
	startHTTPReq := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(startReq))
	startHTTPReq = startHTTPReq.WithContext(context.Background())
	handler.handleStart(startRec, startHTTPReq)

	var startResp struct {
		Status string `json:"status"`
		Data   struct {
			JobID  string `json:"job_id"`
			Status struct {
				JobID string `json:"job_id"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(startRec.Body.Bytes(), &startResp); err != nil {
		t.Fatalf("decode start response: %v, body: %s", err, startRec.Body.String())
	}
	jobID := startResp.Data.JobID
	if jobID == "" {
		jobID = startResp.Data.Status.JobID
	}
	if jobID == "" {
		t.Fatalf("job_id is empty, body: %s", startRec.Body.String())
	}

	quotesReq := httptest.NewRequest(http.MethodGet, "/api/v1/files/getfilebyhash/quotes?job_id="+jobID, nil)
	quotesReq = quotesReq.WithContext(context.Background())
	quotesRec := httptest.NewRecorder()

	handler.handleQuotes(quotesRec, quotesReq)

	if quotesRec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, quotesRec.Code, quotesRec.Body.String())
	}

	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(quotesRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", resp.Status)
	}
}

func TestHTTPHandlerDuplicateStartReturnsSameJob(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	seedHash := "f1f2f3f4f5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	reqBody := `{"seed_hash":"` + seedHash + `","chunk_count":7}`

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(reqBody))
	req1 = req1.WithContext(context.Background())
	rec1 := httptest.NewRecorder()
	handler.handleStart(rec1, req1)

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(reqBody))
	req2 = req2.WithContext(context.Background())
	rec2 := httptest.NewRecorder()
	handler.handleStart(rec2, req2)

	var resp1, resp2 struct {
		Status string `json:"status"`
		Data   struct {
			JobID string `json:"job_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("decode second response: %v", err)
	}

	if resp1.Data.JobID != resp2.Data.JobID {
		t.Fatalf("expected same job_id on duplicate start, got %s and %s", resp1.Data.JobID, resp2.Data.JobID)
	}
}

func TestHTTPHandlerConcurrentDuplicateStart(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	seedHash := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	reqBody := `{"seed_hash":"` + seedHash + `","chunk_count":10}`

	var wg sync.WaitGroup
	results := make(chan string, 10)
	errCh := make(chan error, 10)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash", strings.NewReader(reqBody))
			req = req.WithContext(context.Background())
			rec := httptest.NewRecorder()
			handler.handleStart(rec, req)
			if rec.Code != http.StatusOK {
				errCh <- fmt.Errorf("handler returned %d: %s", rec.Code, rec.Body.String())
				return
			}
			var resp struct {
				Status string `json:"status"`
				Data   struct {
					JobID string `json:"job_id"`
				} `json:"data"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				errCh <- fmt.Errorf("decode response: %v", err)
				return
			}
			results <- resp.Data.JobID
		}(i)
	}

	wg.Wait()
	close(results)
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent request failed: %v", err)
	}

	var firstJobID string
	for jobID := range results {
		if firstJobID == "" {
			firstJobID = jobID
		} else if jobID != firstJobID {
			t.Fatalf("concurrent requests returned different job_ids: %s vs %s", firstJobID, jobID)
		}
	}

	if firstJobID == "" {
		t.Fatalf("no job_id was returned")
	}
}

func TestHTTPHandlerMethodNotAllowedOnChunks(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash/chunks?job_id=test", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleChunks(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHTTPHandlerMethodNotAllowedOnNodes(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash/nodes?job_id=test", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleNodes(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHTTPHandlerMethodNotAllowedOnQuotes(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := NewClientStore(db, nil)
	handler := newDownloadFileHTTPTestHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/getfilebyhash/quotes?job_id=test", nil)
	req = req.WithContext(context.Background())
	rec := httptest.NewRecorder()

	handler.handleQuotes(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}
