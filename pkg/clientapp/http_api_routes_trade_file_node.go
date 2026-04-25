package clientapp

import "net/http"

func (s *httpAPIServer) registerHTTPRouteTrade(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/direct/quotes", s.withAuth(s.handleDirectQuotes))
	mux.HandleFunc(prefix+"/v1/direct/quotes/detail", s.withAuth(s.handleDirectQuoteDetail))
	// 调试接口：proc_direct_transfer_pools 只表达协议运行态
	// 业务状态查询请使用 /v1/downloads/settlement-status
	mux.HandleFunc(prefix+"/v1/direct/transfer-pools", s.withAuth(s.handleDirectTransferPoolsDebug))
	mux.HandleFunc(prefix+"/v1/direct/transfer-pools/detail", s.withAuth(s.handleDirectTransferPoolDetailDebug))
	mux.HandleFunc(prefix+"/v1/biz_purchases", s.withAuth(s.handlePurchases))
	mux.HandleFunc(prefix+"/v1/biz_purchases/detail", s.withAuth(s.handlePurchaseDetail))
	mux.HandleFunc(prefix+"/v1/biz_purchases/summary", s.withAuth(s.handlePurchaseSummary))
	mux.HandleFunc(prefix+"/v1/downloads/settlement-status", s.withAuth(s.handleDownloadSettlementStatus))
}

func (s *httpAPIServer) registerHTTPRouteFile(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/filehash", s.withAuth(s.handleFileHash))
}

func (s *httpAPIServer) registerHTTPRouteGetFileByHash(mux *http.ServeMux, prefix string) {
	if s.downloadFileHandler == nil {
		return
	}
	mux.HandleFunc(prefix+"/v1/files/getfilebyhash", s.withAuth(s.downloadFileHandler.handleStart))
	mux.HandleFunc(prefix+"/v1/files/getfilebyhash/status", s.withAuth(s.downloadFileHandler.handleStatus))
	mux.HandleFunc(prefix+"/v1/files/getfilebyhash/chunks", s.withAuth(s.downloadFileHandler.handleChunks))
	mux.HandleFunc(prefix+"/v1/files/getfilebyhash/nodes", s.withAuth(s.downloadFileHandler.handleNodes))
	mux.HandleFunc(prefix+"/v1/files/getfilebyhash/quotes", s.withAuth(s.downloadFileHandler.handleQuotes))
}

func (s *httpAPIServer) registerHTTPRouteNode(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/libp2p/call", s.withAuth(s.handleCall))
	mux.HandleFunc(prefix+"/v1/resolvers/resolve", s.withAuth(s.handleResolverResolve))
}
