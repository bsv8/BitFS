package clientapp

import (
	"context"
	"errors"
	"testing"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestModuleRegistryLibP2PHook(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	protoID := protocol.ID(contractprotoid.ProtoInboxMessage)

	callCleanup, err := reg.RegisterLibP2PHook(protoID, func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		if ctx == nil {
			return LibP2PResult{}, errors.New("unexpected nil ctx")
		}
		if ev.Protocol != protoID {
			return LibP2PResult{}, errors.New("unexpected protocol")
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

	callOut, err := reg.DispatchLibP2P(context.Background(), LibP2PEvent{
		Protocol:    protoID,
		Meta:        ncall.CallContext{MessageID: "msg-1", SenderPubkeyHex: "sender-1"},
		Req:         ncall.CallReq{To: "target-1", ContentType: "application/json", Body: []byte(`{"hello":"world"}`)},
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("dispatch call failed: %v", err)
	}
	if !callOut.CallResp.Ok || callOut.CallResp.Code != "OK" {
		t.Fatalf("unexpected call response: %+v", callOut.CallResp)
	}
}

func TestModuleRegistryLibP2PRequestCanceled(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	protoID := protocol.ID(contractprotoid.ProtoInboxMessage)
	called := false
	cleanup, err := reg.RegisterLibP2PHook(protoID, func(context.Context, LibP2PEvent) (LibP2PResult, error) {
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
		Protocol: protoID,
		Req:      ncall.CallReq{},
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
		Protocol: "/bsv-transfer/missing/1.0.0",
		Req:      ncall.CallReq{},
	})
	if err == nil || moduleHookCode(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected ROUTE_NOT_FOUND for call, got: %v", err)
	}
}

func TestModuleRegistryLibP2PDuplicateRegistration(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	protoID := protocol.ID(contractprotoid.ProtoInboxMessage)

	cleanup, err := reg.RegisterLibP2PHook(protoID, func(context.Context, LibP2PEvent) (LibP2PResult, error) {
		return LibP2PResult{CallResp: ncall.CallResp{Ok: true, Code: "OK"}}, nil
	})
	if err != nil {
		t.Fatalf("register hook failed: %v", err)
	}
	t.Cleanup(cleanup)

	if _, err := reg.RegisterLibP2PHook(protoID, func(context.Context, LibP2PEvent) (LibP2PResult, error) {
		return LibP2PResult{CallResp: ncall.CallResp{Ok: true, Code: "OK"}}, nil
	}); err == nil {
		t.Fatalf("expected duplicate registration error")
	}
}
