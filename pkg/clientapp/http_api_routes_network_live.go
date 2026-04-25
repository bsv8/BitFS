package clientapp

import "net/http"

func (s *httpAPIServer) registerHTTPRouteNetworkLive(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/admin/seed-cache/stats", s.withAuth(s.handleAdminSeedCacheStats))
	mux.HandleFunc(prefix+"/v1/admin/seed-cache/presence", s.withAuth(s.handleAdminSeedCachePresence))
}

func (s *httpAPIServer) registerHTTPRouteLive(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/live/subscribe-uri", s.withAuth(s.handleLiveSubscribeURI))
	mux.HandleFunc(prefix+"/v1/live/subscribe", s.withAuth(s.handleLiveSubscribe))
	mux.HandleFunc(prefix+"/v1/live/demand/publish", s.withAuth(s.handleLiveDemandPublish))
	mux.HandleFunc(prefix+"/v1/live/quotes", s.withAuth(s.handleLiveQuotes))
	mux.HandleFunc(prefix+"/v1/live/publish/segment", s.withAuth(s.handleLivePublishSegment))
	mux.HandleFunc(prefix+"/v1/live/publish/latest", s.withAuth(s.handleLivePublishLatest))
	mux.HandleFunc(prefix+"/v1/live/latest", s.withAuth(s.handleLiveLatest))
	mux.HandleFunc(prefix+"/v1/live/plan", s.withAuth(s.handleLivePlan))
	mux.HandleFunc(prefix+"/v1/live/follow/start", s.withAuth(s.handleLiveFollowStart))
	mux.HandleFunc(prefix+"/v1/live/follow/stop", s.withAuth(s.handleLiveFollowStop))
	mux.HandleFunc(prefix+"/v1/live/follow/status", s.withAuth(s.handleLiveFollowStatus))
}

func (s *httpAPIServer) registerHTTPRouteGateway(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/gateways", s.withAuth(s.handleGateways))
	mux.HandleFunc(prefix+"/v1/gateways/master", s.withAuth(s.handleGatewayMaster))
	mux.HandleFunc(prefix+"/v1/gateways/health", s.withAuth(s.handleGatewayHealth))
}

func (s *httpAPIServer) registerHTTPRouteArbiter(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/arbiters", s.withAuth(s.handleArbiters))
	mux.HandleFunc(prefix+"/v1/arbiters/health", s.withAuth(s.handleArbiterHealth))
}
