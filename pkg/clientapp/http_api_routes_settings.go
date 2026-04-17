package clientapp

import "net/http"

func (s *httpAPIServer) registerHTTPRouteSettings(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/v1/settings/user", s.withAuth(s.handleUserSettings))
	mux.HandleFunc(prefix+"/v1/settings/user/schema", s.withAuth(s.handleUserSettingsSchema))
}
