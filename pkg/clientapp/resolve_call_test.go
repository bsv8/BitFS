package clientapp

import (
	"context"
	"testing"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
	oldproto "github.com/golang/protobuf/proto"
)

func TestTriggerPeerCallFallsBackToDomainResolve(t *testing.T) {
	t.Parallel()

	senderDB := openResolveCallTestDB(t)
	defer senderDB.Close()
	receiverDB := openResolveCallTestDB(t)
	defer receiverDB.Close()

	senderHost, _ := newSecpHost(t)
	defer senderHost.Close()
	receiverHost, receiverPubkeyHex := newSecpHost(t)
	defer receiverHost.Close()

	senderRT := newRuntimeForTest(t, Config{}, "", withRuntimeHost(senderHost), withRuntimeStore(newClientDB(senderDB, nil)))
	senderRT.modules = newModuleRegistry()
	receiverRT := newRuntimeForTest(t, Config{}, "", withRuntimeHost(receiverHost), withRuntimeStore(newClientDB(receiverDB, nil)))
	receiverRT.modules = newModuleRegistry()

	if _, err := senderRT.modules.RegisterDomainResolveHook(domainbiz.ResolveProviderName, func(ctx context.Context, domain string) (string, error) {
		if domain != "movie.david" {
			return "", domainbiz.NewError(domainbiz.CodeDomainNotResolved, "domain not resolved")
		}
		return receiverPubkeyHex, nil
	}); err != nil {
		t.Fatalf("register domain resolve hook: %v", err)
	}

	registerNodeRouteHandlers(receiverRT, receiverRT.DB())
	if _, err := receiverRT.modules.RegisterLibP2PHook(LibP2PProtocolNodeCall, "test.peer_call.resolve", func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		resp, err := marshalNodeCallProto(&contractmessage.CapabilitiesShowBody{NodePubkeyHex: receiverPubkeyHex})
		if err != nil {
			return LibP2PResult{}, err
		}
		return LibP2PResult{CallResp: resp}, nil
	}); err != nil {
		t.Fatalf("register node call hook: %v", err)
	}
	senderHost.Peerstore().AddAddrs(receiverHost.ID(), receiverHost.Addrs(), time.Minute)

	resp, err := TriggerPeerCall(context.Background(), senderRT, TriggerPeerCallParams{
		To:          "movie.david",
		Route:       "test.peer_call.resolve",
		ContentType: contractmessage.ContentTypeProto,
		Store:       senderRT.DB(),
	})
	if err != nil {
		t.Fatalf("trigger peer call: %v", err)
	}
	if !resp.Ok {
		t.Fatalf("peer call not ok: %+v", resp)
	}
	var body contractmessage.CapabilitiesShowBody
	if err := oldproto.Unmarshal(resp.Body, &body); err != nil {
		t.Fatalf("unmarshal response body: %v", err)
	}
	if body.NodePubkeyHex != receiverPubkeyHex {
		t.Fatalf("node pubkey mismatch: got=%s want=%s", body.NodePubkeyHex, receiverPubkeyHex)
	}
}
