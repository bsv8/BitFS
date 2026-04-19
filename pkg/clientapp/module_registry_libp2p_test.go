package clientapp

import (
	"context"
	"errors"
	"strings"
	"testing"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage"
)

func TestModuleRegistryLibP2PSameRouteDifferentProtocols(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	callCleanup, err := reg.RegisterLibP2PHook(LibP2PProtocolNodeCall, "shared.route", func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		if ctx == nil {
			return LibP2PResult{}, errors.New("unexpected nil ctx")
		}
		if ev.Protocol != LibP2PProtocolNodeCall {
			return LibP2PResult{}, errors.New("unexpected protocol")
		}
		if ev.Route != "shared.route" {
			return LibP2PResult{}, errors.New("unexpected call route")
		}
		if ev.Meta.MessageID != "msg-1" || ev.Meta.SenderPubkeyHex != "sender-1" {
			return LibP2PResult{}, errors.New("unexpected call meta")
		}
		if ev.ContentType != "application/json" {
			return LibP2PResult{}, errors.New("unexpected content type")
		}
		return LibP2PResult{CallResp: ncall.CallResp{Ok: true, Code: "OK", ContentType: contractmessage.ContentTypeProto, Body: []byte("ok")}}, nil
	})
	if err != nil {
		t.Fatalf("register call hook failed: %v", err)
	}
	t.Cleanup(callCleanup)

	resolveCleanup, err := reg.RegisterLibP2PHook(LibP2PProtocolNodeResolve, "shared.route", func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		if ctx == nil {
			return LibP2PResult{}, errors.New("unexpected nil ctx")
		}
		if ev.Protocol != LibP2PProtocolNodeResolve {
			return LibP2PResult{}, errors.New("unexpected protocol")
		}
		if ev.Route != "shared.route" {
			return LibP2PResult{}, errors.New("unexpected resolve route")
		}
		if ev.Req.Route != "lookup.route" {
			return LibP2PResult{}, errors.New("unexpected request route")
		}
		return LibP2PResult{ResolveManifest: routeIndexManifest{
			Route:               ev.Req.Route,
			SeedHash:            strings.Repeat("ab", 32),
			RecommendedFileName: "movie.mp4",
			MIMEHint:            "video/mp4",
			FileSize:            4096,
			UpdatedAtUnix:       123,
		}}, nil
	})
	if err != nil {
		t.Fatalf("register resolve hook failed: %v", err)
	}
	t.Cleanup(resolveCleanup)

	callOut, err := reg.DispatchLibP2P(context.Background(), LibP2PEvent{
		Protocol:    LibP2PProtocolNodeCall,
		Route:       "shared.route",
		Meta:        ncall.CallContext{MessageID: "msg-1", SenderPubkeyHex: "sender-1"},
		Req:         ncall.CallReq{To: "target-1", Route: "shared.route", ContentType: "application/json", Body: []byte(`{"hello":"world"}`)},
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("dispatch call failed: %v", err)
	}
	if !callOut.CallResp.Ok || callOut.CallResp.Code != "OK" {
		t.Fatalf("unexpected call response: %+v", callOut.CallResp)
	}

	resolveOut, err := reg.DispatchLibP2P(context.Background(), LibP2PEvent{
		Protocol: LibP2PProtocolNodeResolve,
		Route:    "shared.route",
		Req:      ncall.CallReq{Route: "lookup.route"},
	})
	if err != nil {
		t.Fatalf("dispatch resolve failed: %v", err)
	}
	if resolveOut.ResolveManifest.Route != "lookup.route" {
		t.Fatalf("unexpected resolve manifest: %+v", resolveOut.ResolveManifest)
	}
}

func TestModuleRegistryLibP2PRequestCanceled(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	called := false
	cleanup, err := reg.RegisterLibP2PHook(LibP2PProtocolNodeCall, inboxmessage.InboxMessageRoute, func(context.Context, LibP2PEvent) (LibP2PResult, error) {
		called = true
		return LibP2PResult{CallResp: ncall.CallResp{Ok: true, Code: "OK"}}, nil
	})
	if err != nil {
		t.Fatalf("register hook failed: %v", err)
	}
	t.Cleanup(cleanup)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = reg.DispatchLibP2P(ctx, LibP2PEvent{
		Protocol: LibP2PProtocolNodeCall,
		Route:    inboxmessage.InboxMessageRoute,
		Req:      ncall.CallReq{Route: inboxmessage.InboxMessageRoute},
	})
	if err == nil || moduleHookCode(err) != "REQUEST_CANCELED" {
		t.Fatalf("expected REQUEST_CANCELED, got: %v", err)
	}
	if called {
		t.Fatalf("canceled request should not reach hook")
	}
}

func TestModuleRegistryLibP2PRouteNotFound(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	_, err := reg.DispatchLibP2P(context.Background(), LibP2PEvent{
		Protocol: LibP2PProtocolNodeCall,
		Route:    "missing.route",
		Req:      ncall.CallReq{Route: "missing.route"},
	})
	if err == nil || moduleHookCode(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected ROUTE_NOT_FOUND for call, got: %v", err)
	}

	_, err = reg.DispatchLibP2P(context.Background(), LibP2PEvent{
		Protocol: LibP2PProtocolNodeResolve,
		Route:    "",
		Req:      ncall.CallReq{Route: "lookup.route"},
	})
	if err == nil || moduleHookCode(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected ROUTE_NOT_FOUND for resolve, got: %v", err)
	}
}

func TestModuleRegistryLibP2PDuplicateRegistration(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	cleanup, err := reg.RegisterLibP2PHook(LibP2PProtocolNodeCall, inboxmessage.InboxMessageRoute, func(context.Context, LibP2PEvent) (LibP2PResult, error) {
		return LibP2PResult{CallResp: ncall.CallResp{Ok: true, Code: "OK"}}, nil
	})
	if err != nil {
		t.Fatalf("register hook failed: %v", err)
	}
	t.Cleanup(cleanup)

	if _, err := reg.RegisterLibP2PHook(LibP2PProtocolNodeCall, inboxmessage.InboxMessageRoute, func(context.Context, LibP2PEvent) (LibP2PResult, error) {
		return LibP2PResult{CallResp: ncall.CallResp{Ok: true, Code: "OK"}}, nil
	}); err == nil {
		t.Fatalf("expected duplicate registration error")
	}
}
