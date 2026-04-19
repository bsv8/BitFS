package clientapp

import "net/http"

func (s *httpAPIServer) registerHTTPRouteSettings(mux *http.ServeMux, prefix string) {
	// settings 管理面只挂 HTTP；index-resolve 也只允许从这里进来。
	mux.HandleFunc(prefix+"/v1/settings/user", s.withAuth(s.handleUserSettings))
	mux.HandleFunc(prefix+"/v1/settings/user/schema", s.withAuth(s.handleUserSettingsSchema))
	if s != nil && s.rt != nil && s.rt.modules != nil {
		s.rt.modules.mountIndexResolveSettingsRoutes(s, mux, prefix)
	}
}
