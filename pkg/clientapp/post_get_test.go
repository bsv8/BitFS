package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPostAndGetRoundTripOverP2P(t *testing.T) {
	t.Parallel()

	senderDB := openPostGetTestDB(t)
	defer senderDB.Close()
	receiverDB := openPostGetTestDB(t)
	defer receiverDB.Close()

	senderHost, _ := newSecpHost(t)
	defer senderHost.Close()
	receiverHost, receiverPubKeyHex := newSecpHost(t)
	defer receiverHost.Close()

	senderRT := &Runtime{Host: senderHost, DB: senderDB}
	receiverRT := &Runtime{Host: receiverHost, DB: receiverDB}
	registerPostGetHandlers(receiverRT)

	senderHost.Peerstore().AddAddrs(receiverHost.ID(), receiverHost.Addrs(), time.Minute)

	if _, err := receiverDB.Exec(
		`INSERT INTO seeds(seed_hash,seed_file_path,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("ab", 32),
		"/tmp/movie.mp4",
		4096,
		"movie.mp4",
		"video/mp4",
		time.Now().Unix(),
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := upsertPublishedRouteIndex(receiverDB, defaultClientGetRoute, strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("upsert route index: %v", err)
	}

	postOut, err := TriggerClientPost(context.Background(), senderRT, TriggerClientPostParams{
		To:          receiverPubKeyHex,
		Route:       routeInboxMessage,
		ContentType: "application/json",
		Body:        []byte(`{"subject":"hello","message":"world"}`),
	})
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	if !postOut.Ok || postOut.Code != "OK" {
		t.Fatalf("unexpected post response: %+v", postOut)
	}
	senderPubKeyHex, err := localPubKeyHex(senderHost)
	if err != nil {
		t.Fatalf("sender pubkey hex: %v", err)
	}

	var gotSenderPubKeyHex string
	var gotTargetInput string
	if err := receiverDB.QueryRow(`SELECT sender_pubkey_hex,target_input FROM inbox_messages ORDER BY id DESC LIMIT 1`).Scan(&gotSenderPubKeyHex, &gotTargetInput); err != nil {
		t.Fatalf("select inbox row: %v", err)
	}
	if gotSenderPubKeyHex != senderPubKeyHex || gotTargetInput != receiverPubKeyHex {
		t.Fatalf("unexpected inbox row: sender=%s target=%s", gotSenderPubKeyHex, gotTargetInput)
	}

	getOut, err := TriggerClientGet(context.Background(), senderRT, TriggerClientGetParams{
		To: receiverPubKeyHex,
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !getOut.Ok || getOut.Code != "OK" {
		t.Fatalf("unexpected get response: %+v", getOut)
	}
	var manifest routeIndexManifest
	if err := json.Unmarshal(getOut.Body, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.SeedHash != strings.Repeat("ab", 32) {
		t.Fatalf("unexpected seed hash: %s", manifest.SeedHash)
	}
	if manifest.Route != defaultClientGetRoute {
		t.Fatalf("unexpected route: %s", manifest.Route)
	}
}

func TestHTTPAPIPostGetInboxAndRouteIndex(t *testing.T) {
	t.Parallel()

	senderDB := openPostGetTestDB(t)
	defer senderDB.Close()
	receiverDB := openPostGetTestDB(t)
	defer receiverDB.Close()

	senderHost, _ := newSecpHost(t)
	defer senderHost.Close()
	receiverHost, receiverPubKeyHex := newSecpHost(t)
	defer receiverHost.Close()

	senderRT := &Runtime{Host: senderHost, DB: senderDB}
	receiverRT := &Runtime{Host: receiverHost, DB: receiverDB}
	registerPostGetHandlers(receiverRT)

	senderHost.Peerstore().AddAddrs(receiverHost.ID(), receiverHost.Addrs(), time.Minute)

	senderSrv := &httpAPIServer{rt: senderRT, db: senderDB}
	receiverSrv := &httpAPIServer{rt: receiverRT, db: receiverDB}

	if _, err := receiverDB.Exec(
		`INSERT INTO seeds(seed_hash,seed_file_path,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("cd", 32),
		"/tmp/song.mp3",
		1024,
		"song.mp3",
		"audio/mpeg",
		time.Now().Unix(),
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/routes/indexes", strings.NewReader(`{"route":"index.mp3","seed_hash":"`+strings.Repeat("cd", 32)+`"}`))
		rec := httptest.NewRecorder()
		receiverSrv.handleAdminRouteIndexes(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("admin route indexes status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/post", strings.NewReader(`{"to":"`+receiverPubKeyHex+`","route":"inbox.message","content_type":"application/json","body":{"hello":"world"}}`))
		rec := httptest.NewRecorder()
		senderSrv.handlePost(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("post api status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode post api response: %v", err)
		}
		if ok, _ := body["ok"].(bool); !ok {
			t.Fatalf("expected post ok response: %s", rec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/inbox/messages", nil)
		rec := httptest.NewRecorder()
		receiverSrv.handleInboxMessages(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("inbox list status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode inbox list: %v", err)
		}
		if body.Total != 1 || len(body.Items) != 1 {
			t.Fatalf("unexpected inbox list: %s", rec.Body.String())
		}

		detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/inbox/messages/detail?id="+strconv.FormatInt(body.Items[0].ID, 10), nil)
		detailRec := httptest.NewRecorder()
		receiverSrv.handleInboxMessageDetail(detailRec, detailReq)
		if detailRec.Code != http.StatusOK {
			t.Fatalf("inbox detail status mismatch: got=%d body=%s", detailRec.Code, detailRec.Body.String())
		}
		if !strings.Contains(detailRec.Body.String(), `"body_json"`) {
			t.Fatalf("expected decoded json body in detail: %s", detailRec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/get", strings.NewReader(`{"to":"`+receiverPubKeyHex+`","route":"index.mp3"}`))
		rec := httptest.NewRecorder()
		senderSrv.handleGet(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("get api status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), strings.Repeat("cd", 32)) {
			t.Fatalf("expected seed hash in get body: %s", rec.Body.String())
		}
	}
}

func openPostGetTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}
