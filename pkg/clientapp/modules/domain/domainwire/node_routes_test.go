package domainwire

import (
	"context"
	"strings"
	"testing"

	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	oldproto "github.com/golang/protobuf/proto"
)

func TestHandleNodeCallListOwnedUsesSenderIdentity(t *testing.T) {
	t.Parallel()

	var capturedOwner string
	handled, resp, err := HandleNodeCall(context.Background(), NodeRouteRuntime{
		ListOwned: func(_ context.Context, meta ncall.CallContext, body ListOwnedReq) (ncall.CallResp, error) {
			capturedOwner = body.OwnerPubkeyHex
			return ncall.MarshalProto(&ListOwnedResp{
				Success:        true,
				Status:         "ok",
				OwnerPubkeyHex: body.OwnerPubkeyHex,
			})
		},
	}, ncall.CallContext{
		SenderPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, ncall.CallReq{
		Route: string(contractroute.RouteDomainV1ListOwned),
	})
	if err != nil {
		t.Fatalf("HandleNodeCall() failed: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if !resp.Ok {
		t.Fatalf("unexpected route response: %+v", resp)
	}
	if capturedOwner != "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("unexpected owner: %q", capturedOwner)
	}
	var body ListOwnedResp
	if err := oldproto.Unmarshal(resp.Body, &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body.OwnerPubkeyHex != capturedOwner {
		t.Fatalf("unexpected response body: %+v", body)
	}
}

func TestHandleNodeCallRegisterSubmitValidatesRegisterTx(t *testing.T) {
	t.Parallel()

	raw, err := oldproto.Marshal(&RegisterSubmitReq{ClientID: "legacy"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	handled, resp, err := HandleNodeCall(context.Background(), NodeRouteRuntime{}, ncall.CallContext{
		SenderPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, ncall.CallReq{
		Route: string(contractroute.RouteDomainV1RegisterSubmit),
		Body:  raw,
	})
	if err != nil {
		t.Fatalf("HandleNodeCall() failed: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if resp.Ok || resp.Code != "BAD_REQUEST" || resp.Message != "register_tx required" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHandleNodeCallUnknownRouteFallsThrough(t *testing.T) {
	t.Parallel()

	handled, resp, err := HandleNodeCall(context.Background(), NodeRouteRuntime{}, ncall.CallContext{}, ncall.CallReq{
		Route: "domain.v1.unknown",
	})
	if err != nil {
		t.Fatalf("HandleNodeCall() failed: %v", err)
	}
	if handled {
		t.Fatalf("expected handled=false, resp=%+v", resp)
	}
}

func TestHandleNodeCallRejectsDuplicateContractBodyErrors(t *testing.T) {
	t.Parallel()

	handled, resp, err := HandleNodeCall(context.Background(), NodeRouteRuntime{}, ncall.CallContext{}, ncall.CallReq{
		Route: string(contractroute.RouteDomainV1Resolve),
	})
	if err != nil {
		t.Fatalf("HandleNodeCall() failed: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if resp.Ok || resp.Code != "BAD_REQUEST" || !strings.Contains(resp.Message, "request body missing") {
		t.Fatalf("unexpected response: %+v", resp)
	}
}