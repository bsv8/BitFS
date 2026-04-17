package clientapp

import "net/http"

func (s *httpAPIServer) registerHTTPRouteCore(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/info", s.withAuth(s.handleInfo))
	mux.HandleFunc(prefix+"/v1/balance", s.withAuth(s.handleBalance))
}

func (s *httpAPIServer) registerHTTPRouteWallet(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/wallet/summary", s.withAuth(s.handleWalletSummary))
	mux.HandleFunc(prefix+"/v1/admin/wallet/consistency", s.withAuth(s.handleAdminWalletConsistency))
	mux.HandleFunc(prefix+"/v1/wallet/sync-once", s.withAuth(s.handleWalletSyncOnce))
	mux.HandleFunc(prefix+"/v1/wallet/order/preview", s.withAuth(s.handleWalletOrderPreview))
	mux.HandleFunc(prefix+"/v1/wallet/order/sign", s.withAuth(s.handleWalletOrderSign))
	mux.HandleFunc(prefix+"/v1/wallet/order/pay-bsv", s.withAuth(s.handleWalletOrderPayBSV))
	mux.HandleFunc(prefix+"/v1/wallet/tokens/create/preview", s.withAuth(s.handleWalletTokenCreatePreview))
	mux.HandleFunc(prefix+"/v1/wallet/tokens/create/sign", s.withAuth(s.handleWalletTokenCreateSign))
	mux.HandleFunc(prefix+"/v1/wallet/tokens/create/submit", s.withAuth(s.handleWalletTokenCreateSubmit))
	mux.HandleFunc(prefix+"/v1/wallet/tokens/send/preview", s.withAuth(s.handleWalletTokenSendPreview))
	mux.HandleFunc(prefix+"/v1/wallet/tokens/send/sign", s.withAuth(s.handleWalletTokenSendSign))
	mux.HandleFunc(prefix+"/v1/wallet/tokens/send/submit", s.withAuth(s.handleWalletTokenSendSubmit))
	mux.HandleFunc(prefix+"/v1/wallet/fund-flows", s.withAuth(s.handleWalletFundFlows))
	mux.HandleFunc(prefix+"/v1/wallet/fund-flows/detail", s.withAuth(s.handleWalletFundFlowDetail))
	mux.HandleFunc(prefix+"/v1/transactions", s.withAuth(s.handleTransactions))
	mux.HandleFunc(prefix+"/v1/transactions/detail", s.withAuth(s.handleTransactionDetail))
}
