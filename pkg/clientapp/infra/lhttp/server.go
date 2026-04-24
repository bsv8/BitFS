package lhttp

import (
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/infra/caps"
)

type Route struct {
	Path    string
	Handler http.HandlerFunc
}

type RouteDecl struct {
	InternalAbility string
	Routes          []Route
}

func Paths(routes ...Route) []string {
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		out = append(out, route.Path)
	}
	return out
}

func Flatten(routeSets ...[]Route) []Route {
	total := 0
	for _, set := range routeSets {
		total += len(set)
	}
	out := make([]Route, 0, total)
	for _, set := range routeSets {
		out = append(out, set...)
	}
	return out
}

func FlattenDecls(decls ...RouteDecl) []Route {
	sets := make([][]Route, 0, len(decls))
	for _, decl := range decls {
		sets = append(sets, decl.Routes)
	}
	return Flatten(sets...)
}

func ModuleSpecs(decls ...RouteDecl) []caps.ModuleSpec {
	out := make([]caps.ModuleSpec, 0, len(decls))
	for _, decl := range decls {
		out = append(out, caps.ModuleSpec{
			InternalAbility: decl.InternalAbility,
			HTTPPaths:       Paths(decl.Routes...),
		})
	}
	return out
}

func NewServeMux(routes ...Route) *http.ServeMux {
	mux := http.NewServeMux()
	for _, route := range routes {
		if route.Handler == nil {
			continue
		}
		mux.HandleFunc(route.Path, route.Handler)
	}
	return mux
}

type ServerOptions struct {
	ListenAddr        string
	Handler           http.Handler
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

type StartedServer struct {
	Server   *http.Server
	Listener net.Listener
}

func StartServer(opts ServerOptions) (*StartedServer, error) {
	srv := &http.Server{
		Addr:              opts.ListenAddr,
		Handler:           opts.Handler,
		ReadTimeout:       withDefaultDuration(opts.ReadTimeout, 10*time.Second),
		ReadHeaderTimeout: withDefaultDuration(opts.ReadHeaderTimeout, 5*time.Second),
		WriteTimeout:      withDefaultDuration(opts.WriteTimeout, 30*time.Second),
		IdleTimeout:       withDefaultDuration(opts.IdleTimeout, 60*time.Second),
	}
	ln, err := net.Listen("tcp", opts.ListenAddr)
	if err != nil {
		return nil, err
	}
	return &StartedServer{
		Server:   srv,
		Listener: ln,
	}, nil
}

func ServeInBackground(started *StartedServer, onError func(error)) {
	if started == nil || started.Server == nil || started.Listener == nil {
		return
	}
	go func() {
		if err := started.Server.Serve(started.Listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if onError != nil {
				onError(err)
			}
		}
	}()
}

func withDefaultDuration(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}
