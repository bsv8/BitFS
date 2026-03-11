package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/woc"
	"github.com/bsv8/BitFS/pkg/clientapp"
)

type managedDaemon struct {
	appName string
	cfg     clientapp.Config
	startup startupSummary
	dbPath  string

	rootCtx    context.Context
	rootCancel context.CancelFunc

	db  *sql.DB
	srv *http.Server

	mu            sync.RWMutex
	rt            *clientapp.Runtime
	rtCancel      context.CancelFunc
	rtProxy       *httputil.ReverseProxy
	guardStop     func()
	internalToken string
}

func runManagedDaemon(appName string, cfg clientapp.Config, startup startupSummary, dbPath string) error {
	rootCtx, rootCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer rootCancel()

	db, err := openRuntimeDB(dbPath)
	if err != nil {
		return err
	}
	d := &managedDaemon{
		appName:    appName,
		cfg:        cfg,
		startup:    startup,
		dbPath:     dbPath,
		rootCtx:    rootCtx,
		rootCancel: rootCancel,
		db:         db,
	}
	if err := d.startHTTPServer(); err != nil {
		_ = db.Close()
		return err
	}
	<-rootCtx.Done()
	return d.close()
}

func (d *managedDaemon) close() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	guardStop := d.guardStop
	d.rtCancel = nil
	d.rt = nil
	d.rtProxy = nil
	d.guardStop = nil
	d.internalToken = ""
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	if guardStop != nil {
		guardStop()
	}
	if d.srv != nil {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = d.srv.Shutdown(ctx)
		stop()
	}
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *managedDaemon) startHTTPServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/key/status", d.handleKeyStatus)
	mux.HandleFunc("/api/v1/key/new", d.handleKeyNew)
	mux.HandleFunc("/api/v1/key/import", d.handleKeyImport)
	mux.HandleFunc("/api/v1/key/export", d.handleKeyExport)
	mux.HandleFunc("/api/v1/key/unlock", d.handleKeyUnlock)
	mux.HandleFunc("/api/v1/key/lock", d.handleKeyLock)
	mux.HandleFunc("/api", d.handleAPIProxyOrLocked)
	mux.HandleFunc("/api/", d.handleAPIProxyOrLocked)
	mux.HandleFunc("/", d.handleWebAsset)

	d.srv = &http.Server{
		Addr:              d.cfg.HTTP.ListenAddr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	obs.Important("bitcast-client", "managed_api_started", map[string]any{
		"listen_addr": d.cfg.HTTP.ListenAddr,
		"appname":     d.appName,
		"locked":      true,
	})
	go func() {
		if err := d.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			obs.Error("bitcast-client", "managed_api_stopped", map[string]any{"error": err.Error()})
			d.rootCancel()
		}
	}()
	return nil
}

func (d *managedDaemon) handleWebAsset(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(webAssets, "web")
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
		return
	}
	name := strings.TrimPrefix(pathClean(r.URL.Path), "/")
	if name == "" {
		name = "index.html"
	}
	if strings.Contains(name, "..") {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
		return
	}
	data, err := fs.ReadFile(sub, name)
	if err != nil {
		if name != "index.html" {
			data, err = fs.ReadFile(sub, "index.html")
		}
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
			return
		}
		name = "index.html"
	}
	if ct := mime.TypeByExtension(filepath.Ext(name)); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (d *managedDaemon) handleAPIProxyOrLocked(w http.ResponseWriter, r *http.Request) {
	proxy := d.currentProxy()
	if proxy == nil {
		writeJSON(w, http.StatusLocked, map[string]any{"error": "client is locked"})
		return
	}
	proxy.ServeHTTP(w, r)
}

func (d *managedDaemon) handleKeyStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	_, exists, err := loadEncryptedKeyEnvelope(d.db)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	d.mu.RLock()
	unlocked := d.rt != nil
	d.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"appname":  d.appName,
		"has_key":  exists,
		"unlocked": unlocked,
	})
}

func (d *managedDaemon) handleKeyNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if d.isUnlocked() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client is unlocked, lock first"})
		return
	}
	if _, exists, err := loadEncryptedKeyEnvelope(d.db); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	} else if exists {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "encrypted key already exists"})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.Password) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "password is required"})
		return
	}
	privHex, err := generatePrivateKeyHex()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	env, err := encryptPrivateKeyEnvelope(d.appName, privHex, req.Password)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := saveEncryptedKeyEnvelope(d.db, env); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	pubHex, _ := pubHexFromPrivHex(privHex)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pubkey_hex": pubHex})
}

func (d *managedDaemon) handleKeyImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if d.isUnlocked() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client is unlocked, lock first"})
		return
	}
	if _, exists, err := loadEncryptedKeyEnvelope(d.db); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	} else if exists {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "encrypted key already exists"})
		return
	}
	var req struct {
		Cipher *encryptedKeyEnvelope `json:"cipher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if req.Cipher == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "cipher is required"})
		return
	}
	if err := saveEncryptedKeyEnvelope(d.db, *req.Cipher); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (d *managedDaemon) handleKeyExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	env, exists, err := loadEncryptedKeyEnvelope(d.db)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !exists {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "encrypted key not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cipher": env})
}

func (d *managedDaemon) handleKeyUnlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if d.isUnlocked() {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": true})
		return
	}
	env, exists, err := loadEncryptedKeyEnvelope(d.db)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !exists || env == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "encrypted key not found"})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	privHex, err := decryptPrivateKeyEnvelope(d.appName, *env, req.Password)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
		return
	}
	if err := d.startRuntime(privHex); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": true})
}

func (d *managedDaemon) handleKeyLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if err := d.stopRuntime(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": false})
}

func (d *managedDaemon) startRuntime(privHex string) error {
	d.mu.Lock()
	if d.rt != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	guardURL, stopGuard, err := woc.EnsureGuardRunning(d.rootCtx, woc.GuardRuntimeOptions{Network: d.cfg.BSV.Network})
	if err != nil {
		return err
	}

	runCfg, _, err := loadRuntimeConfigOrInit(d.appName, d.dbPath)
	if err != nil {
		stopGuard()
		return err
	}
	token, err := randomTokenHex(24)
	if err != nil {
		stopGuard()
		return err
	}
	runCfg.HTTP.AuthToken = token
	runCfg.HTTP.ListenAddr = "127.0.0.1:0"
	runIn := clientapp.NewRunInputFromConfig(runCfg, privHex)
	runIn.WebAssets = webAssets
	runIn.Chain = woc.NewGuardClient(guardURL)

	runCtx, cancel := context.WithCancel(d.rootCtx)
	rt, err := clientapp.Run(runCtx, runIn)
	if err != nil {
		cancel()
		stopGuard()
		return err
	}
	targetURL, err := url.Parse("http://" + strings.TrimSpace(rt.HTTPListenAddr()))
	if err != nil {
		_ = rt.Close()
		cancel()
		stopGuard()
		return err
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	baseDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		baseDirector(req)
		req.Header.Set("X-API-Token", token)
		req.Header.Set("Authorization", "Bearer "+token)
	}

	d.mu.Lock()
	d.rt = rt
	d.rtCancel = cancel
	d.rtProxy = proxy
	d.guardStop = stopGuard
	d.internalToken = token
	d.mu.Unlock()

	obs.Important("bitcast-client", "managed_runtime_started", map[string]any{
		"peer_id":      rt.Host.ID().String(),
		"runtime_http": rt.HTTPListenAddr(),
	})
	return nil
}

func (d *managedDaemon) stopRuntime() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	guardStop := d.guardStop
	d.rtCancel = nil
	d.rt = nil
	d.rtProxy = nil
	d.guardStop = nil
	d.internalToken = ""
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	if guardStop != nil {
		guardStop()
	}
	obs.Important("bitcast-client", "managed_runtime_stopped", map[string]any{"appname": d.appName})
	return nil
}

func (d *managedDaemon) currentProxy() *httputil.ReverseProxy {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rtProxy
}

func (d *managedDaemon) isUnlocked() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rt != nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func randomTokenHex(n int) (string, error) {
	if n <= 0 {
		n = 16
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func pathClean(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return filepath.ToSlash(filepath.Clean(p))
}
