package chain_tx_v1

import (
	"context"
	"errors"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
)

func TestApplyLocalBroadcastWalletTxCallsApplier(t *testing.T) {
	t.Parallel()

	var gotRaw []byte
	var gotTrigger string
	host := &fakeChainTxPaymentHost{
		applyFn: func(ctx context.Context, rawTx []byte, trigger string) error {
			gotRaw = append([]byte(nil), rawTx...)
			gotTrigger = trigger
			return nil
		},
	}

	svc := &service{}
	svc.applyLocalBroadcastWalletTx(context.Background(), host, "abc123", []byte{0x01, 0x02, 0x03}, "/bsv-transfer/domain/register_lock_paid/1.0.0")

	if string(gotRaw) != string([]byte{0x01, 0x02, 0x03}) {
		t.Fatalf("raw tx mismatch: got=%x", gotRaw)
	}
	if gotTrigger != "/bsv-transfer/domain/register_lock_paid/1.0.0" {
		t.Fatalf("trigger mismatch: got=%q", gotTrigger)
	}
}

func TestApplyLocalBroadcastWalletTxNoopWithoutApplier(t *testing.T) {
	t.Parallel()

	svc := &service{}
	svc.applyLocalBroadcastWalletTx(context.Background(), &fakeChainTxPaymentHostNoApplier{}, "abc123", []byte{0x01}, "trigger")
}

func TestBuildChainTxUsesProvidedSpendableUTXOs(t *testing.T) {
	t.Parallel()

	clientPrivHex := "1111111111111111111111111111111111111111111111111111111111111111"
	gatewayPrivHex := "2222222222222222222222222222222222222222222222222222222222222222"
	clientPriv, err := ec.PrivateKeyFromHex(clientPrivHex)
	if err != nil {
		t.Fatalf("parse client private key: %v", err)
	}
	gatewayPriv, err := ec.PrivateKeyFromHex(gatewayPrivHex)
	if err != nil {
		t.Fatalf("parse gateway private key: %v", err)
	}
	clientAddr, err := script.NewAddressFromPublicKey(clientPriv.PubKey(), false)
	if err != nil {
		t.Fatalf("derive client address: %v", err)
	}
	gatewayPub := gatewayPriv.PubKey()

	svc := &service{}
	actor := &poolcore.Actor{
		Name:    "client",
		PrivKey: clientPriv,
		Addr:    clientAddr.AddressString,
	}
	built, err := svc.buildChainTx(actor, []poolcore.UTXO{
		{TxID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Vout: 0, Value: 2000},
	}, gatewayPub, []byte("quote-bytes"), payflow.ServiceQuote{ChargeAmountSatoshi: 1000}, "/bsv-transfer/domain/register_lock_paid/1.0.0")
	if err != nil {
		t.Fatalf("build chain tx: %v", err)
	}
	if built.TxID == "" {
		t.Fatal("txid is empty")
	}
	if built.MinerFeeSatoshi == 0 {
		t.Fatal("miner fee should be positive")
	}
}

type fakeChainTxPaymentHost struct {
	applyFn func(context.Context, []byte, string) error
}

func (h *fakeChainTxPaymentHost) PeerCallRaw(context.Context, string, string, moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error) {
	return moduleapi.PeerCallResponse{}, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHost) QuoteService(context.Context, string, string, moduleapi.ServiceQuoteRequest) (moduleapi.ServiceQuoteResponse, error) {
	return moduleapi.ServiceQuoteResponse{}, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHost) PaymentFacade() moduleapi.PaymentFacade { return nil }

func (h *fakeChainTxPaymentHost) Actor() (*poolcore.Actor, error) { return &poolcore.Actor{}, nil }

func (h *fakeChainTxPaymentHost) SendProto(context.Context, string, string, any, any) error {
	return errors.New("not implemented")
}

func (h *fakeChainTxPaymentHost) WalletUTXOs(context.Context) ([]poolcore.UTXO, error) {
	return nil, nil
}

func (h *fakeChainTxPaymentHost) PoolInfo(context.Context, string) (poolcore.InfoResp, error) {
	return poolcore.InfoResp{}, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHost) GetGatewayPubkey(peer.ID) (*ec.PublicKey, error) {
	return nil, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHost) ApplyLocalBroadcastWalletTxBytes(ctx context.Context, rawTx []byte, trigger string) error {
	if h.applyFn == nil {
		return nil
	}
	return h.applyFn(ctx, rawTx, trigger)
}

type fakeChainTxPaymentHostNoApplier struct{}

func (h *fakeChainTxPaymentHostNoApplier) PeerCallRaw(context.Context, string, string, moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error) {
	return moduleapi.PeerCallResponse{}, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHostNoApplier) QuoteService(context.Context, string, string, moduleapi.ServiceQuoteRequest) (moduleapi.ServiceQuoteResponse, error) {
	return moduleapi.ServiceQuoteResponse{}, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHostNoApplier) PaymentFacade() moduleapi.PaymentFacade { return nil }

func (h *fakeChainTxPaymentHostNoApplier) Actor() (*poolcore.Actor, error) {
	return &poolcore.Actor{}, nil
}

func (h *fakeChainTxPaymentHostNoApplier) SendProto(context.Context, string, string, any, any) error {
	return errors.New("not implemented")
}

func (h *fakeChainTxPaymentHostNoApplier) WalletUTXOs(context.Context) ([]poolcore.UTXO, error) {
	return nil, nil
}

func (h *fakeChainTxPaymentHostNoApplier) PoolInfo(context.Context, string) (poolcore.InfoResp, error) {
	return poolcore.InfoResp{}, errors.New("not implemented")
}

func (h *fakeChainTxPaymentHostNoApplier) GetGatewayPubkey(peer.ID) (*ec.PublicKey, error) {
	return nil, errors.New("not implemented")
}
