package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/woc"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
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

	mu        sync.RWMutex
	rt        *clientapp.Runtime
	rtCancel  context.CancelFunc
	rtProxy   *httputil.ReverseProxy
	guardStop func()
}

func runManagedDaemon(appName string, cfg clientapp.Config, startup startupSummary, dbPath string) error {
	rootCtx, rootCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer rootCancel()
	logFile, logConsoleMinLevel := clientapp.ResolveLogConfig(&cfg)
	if err := obs.Init(logFile, logConsoleMinLevel); err != nil {
		return err
	}
	defer func() { _ = obs.Close() }()

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
	// 缺省启动路径进入“命令行解锁循环”：
	// - locked + 有密钥：提示输入密码；
	// - API 若先解锁：命令行输入会自动取消并提示已解锁；
	// - API 再次 lock：会重新进入等待密码状态。
	go d.cliUnlockLoop()
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
	// 设计说明：
	// managed 层通过反向代理转发到 runtime HTTP。
	// 这里不能使用 :0 作为目标地址字符串，否则代理目标不可达会稳定 502。
	runtimeHTTPAddr, err := allocateLoopbackTCPAddr()
	if err != nil {
		stopGuard()
		return err
	}
	runCfg.HTTP.ListenAddr = runtimeHTTPAddr
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

	d.mu.Lock()
	d.rt = rt
	d.rtCancel = cancel
	d.rtProxy = proxy
	d.guardStop = stopGuard
	d.mu.Unlock()

	obs.Important("bitcast-client", "managed_runtime_started", map[string]any{
		"peer_id":      rt.Host.ID().String(),
		"runtime_http": rt.HTTPListenAddr(),
	})
	return nil
}

func allocateLoopbackTCPAddr() (string, error) {
	var lastErr error
	for i := 0; i < 8; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			lastErr = err
			continue
		}
		addr := strings.TrimSpace(ln.Addr().String())
		_ = ln.Close()
		if addr != "" && !strings.HasSuffix(addr, ":0") {
			return addr, nil
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("allocate runtime http listen addr failed: %w", lastErr)
	}
	return "", fmt.Errorf("allocate runtime http listen addr failed")
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

func (d *managedDaemon) cliUnlockLoop() {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		obs.Info("bitcast-client", "cli_unlock_loop_skipped_non_interactive_stdin", map[string]any{"appname": d.appName})
		return
	}
	for {
		select {
		case <-d.rootCtx.Done():
			return
		default:
		}
		if d.isUnlocked() {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		env, exists, err := loadEncryptedKeyEnvelope(d.db)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_load_key_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if !exists || env == nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		password, cancelled, err := d.readPasswordCancelable(msg("prompt_password_unlock"))
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_read_password_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cancelled {
			// 管理 API 已解锁，等待下一轮状态变化（例如再次 lock）。
			time.Sleep(150 * time.Millisecond)
			continue
		}
		password = strings.TrimSpace(password)
		if password == "" {
			obs.Info("bitcast-client", "cli_unlock_empty_password", map[string]any{"appname": d.appName})
			continue
		}
		if d.isUnlocked() {
			fmt.Fprintln(os.Stderr, "已解锁（管理 API 已生效）")
			continue
		}
		privHex, err := decryptPrivateKeyEnvelope(d.appName, *env, password)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_decrypt_failed", map[string]any{"error": err.Error()})
			continue
		}
		if err := d.startRuntime(privHex); err != nil {
			obs.Error("bitcast-client", "cli_unlock_start_runtime_failed", map[string]any{"error": err.Error()})
			continue
		}
		obs.Important("bitcast-client", "cli_unlock_succeeded", map[string]any{"appname": d.appName})
	}
}

// readPasswordCancelable 在终端输入密码时支持“被 API 解锁后取消等待”。
// 返回 cancelled=true 表示输入过程中检测到已解锁，调用方应停止本次输入流程。
func (d *managedDaemon) readPasswordCancelable(prompt string) (password string, cancelled bool, err error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		p, e := readPassword(prompt)
		return p, false, e
	}
	fmt.Fprint(os.Stderr, prompt)
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = term.Restore(fd, oldState)
		fmt.Fprintln(os.Stderr)
	}()

	buf := make([]byte, 0, 128)
	for {
		if d.isUnlocked() {
			fmt.Fprint(os.Stderr, "\r已解锁（管理 API 已生效），取消命令行密码输入。")
			return "", true, nil
		}
		select {
		case <-d.rootCtx.Done():
			return "", false, d.rootCtx.Err()
		default:
		}

		pollFds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, pollErr := unix.Poll(pollFds, 200)
		if pollErr != nil {
			if errors.Is(pollErr, unix.EINTR) {
				continue
			}
			return "", false, pollErr
		}
		if n == 0 {
			continue
		}
		var one [1]byte
		readN, readErr := unix.Read(fd, one[:])
		if readErr != nil {
			if errors.Is(readErr, unix.EINTR) {
				continue
			}
			return "", false, readErr
		}
		if readN != 1 {
			continue
		}
		ch := one[0]
		switch ch {
		case '\r', '\n':
			return strings.TrimSpace(string(buf)), false, nil
		case 127, 8:
			if len(buf) > 0 {
				buf = buf[:len(buf)-1]
			}
		case 3:
			return "", false, context.Canceled
		default:
			// 忽略控制字符，仅收集可见输入。
			if ch >= 32 && ch <= 126 {
				buf = append(buf, ch)
			}
		}
	}
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

func pathClean(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return filepath.ToSlash(filepath.Clean(p))
}
