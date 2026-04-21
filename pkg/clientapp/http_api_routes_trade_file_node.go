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
	mux.HandleFunc(prefix+"/v1/gateways/events", s.withAuth(s.handleGatewayEvents))
	mux.HandleFunc(prefix+"/v1/gateways/events/detail", s.withAuth(s.handleGatewayEventDetail))
}

func (s *httpAPIServer) registerHTTPRouteFile(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/files/get-file", s.withAuth(s.handleGetFileStart))
	mux.HandleFunc(prefix+"/v1/files/get-file/plan", s.withAuth(s.handleGetFilePlan))
	mux.HandleFunc(prefix+"/v1/files/get-file/status", s.withAuth(s.handleGetFileStatus))
	mux.HandleFunc(prefix+"/v1/files/get-file/ensure", s.withAuth(s.handleGetFileEnsure))
	mux.HandleFunc(prefix+"/v1/files/get-file/content", s.withAuth(s.handleGetFileContent))
	mux.HandleFunc(prefix+"/v1/files/get-file/job", s.withAuth(s.handleGetFileJob))
	mux.HandleFunc(prefix+"/v1/files/get-file/jobs", s.withAuth(s.handleGetFileJobs))
	mux.HandleFunc(prefix+"/v1/files/get-file/cancel", s.withAuth(s.handleGetFileCancel))
	mux.HandleFunc(prefix+"/v1/filehash", s.withAuth(s.handleFileHash))
}

func (s *httpAPIServer) registerHTTPRouteNode(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/libp2p/call", s.withAuth(s.handleCall))
	mux.HandleFunc(prefix+"/v1/resolvers/resolve", s.withAuth(s.handleResolverResolve))
	mux.HandleFunc(prefix+"/v1/domains/register", s.withAuth(s.handleDomainRegister))
	mux.HandleFunc(prefix+"/v1/domains/set-target", s.withAuth(s.handleDomainSetTarget))
	mux.HandleFunc(prefix+"/v1/domains/settlement-status", s.withAuth(s.handleDomainSettlementStatus))
}
