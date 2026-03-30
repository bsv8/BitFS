package clientapp

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
)

type feePoolProofArgs struct {
	Session              *feePoolSession
	ClientActor          *poolcore.Actor
	GatewayPub           *ec.PublicKey
	ServiceQuoteRaw      []byte
	ServiceQuote         payflow.ServiceQuote
	ChargeReason         string
	NextSequence         uint32
	NextServerAmount     uint64
	ServiceDeadlineUnix  int64
}

type feePoolProofBuilt struct {
	UpdatedTx          *txsdk.Transaction
	ProofIntent        []byte
	SignedProofCommit  []byte
	ProofStatePayload  []byte
	AcceptedChargeHash string
}

func buildFeePoolUpdatedTxWithProof(args feePoolProofArgs) (feePoolProofBuilt, error) {
	if args.Session == nil || strings.TrimSpace(args.Session.CurrentTxHex) == "" {
		return feePoolProofBuilt{}, fmt.Errorf("fee pool session current tx missing")
	}
	if args.ClientActor == nil || args.ClientActor.PrivKey == nil || args.ClientActor.PubKey == nil {
		return feePoolProofBuilt{}, fmt.Errorf("client actor missing")
	}
	if args.GatewayPub == nil {
		return feePoolProofBuilt{}, fmt.Errorf("gateway public key missing")
	}
	if len(args.ServiceQuoteRaw) == 0 {
		return feePoolProofBuilt{}, fmt.Errorf("service quote required")
	}
	gatewayQuoteHash, err := payflow.HashServiceQuote(args.ServiceQuote)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	nextSequence := args.NextSequence
	nextServerAmount := args.NextServerAmount
	chargeAmount := args.ServiceQuote.ChargeAmountSatoshi
	baseTx, err := ce.LoadTx(
		args.Session.CurrentTxHex,
		nil,
		nextSequence,
		nextServerAmount,
		args.GatewayPub,
		args.ClientActor.PubKey,
		args.Session.PoolAmountSat,
	)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	intent := payflow.ChargeIntent{
		GatewayPubkeyHex:    strings.ToLower(hex.EncodeToString(args.GatewayPub.Compressed())),
		ClientPubkeyHex:     strings.ToLower(strings.TrimSpace(args.ClientActor.PubHex)),
		SpendTxID:           strings.TrimSpace(args.Session.SpendTxID),
		GatewayQuoteHash:    gatewayQuoteHash,
		ChargeReason:        strings.TrimSpace(args.ChargeReason),
		ChargeAmountSatoshi: chargeAmount,
		SequenceNumber:      nextSequence,
		ServerAmountBefore:  args.Session.ServerAmount,
		ServerAmountAfter:   nextServerAmount,
		ServiceDeadlineUnix: args.ServiceDeadlineUnix,
	}
	intentRaw, err := payflow.MarshalIntent(intent)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	intentHash, err := payflow.HashIntent(intent)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	updateTemplateHash, err := payflow.UpdateTemplateHashFromTxHex(baseTx.Hex())
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	commit := payflow.ClientCommit{
		IntentHash:          intentHash,
		ClientPubkeyHex:     strings.ToLower(strings.TrimSpace(args.ClientActor.PubHex)),
		SpendTxID:           strings.TrimSpace(args.Session.SpendTxID),
		SequenceNumber:      nextSequence,
		ServerAmountBefore:  args.Session.ServerAmount,
		ChargeAmountSatoshi: chargeAmount,
		ServerAmountAfter:   nextServerAmount,
		UpdateTemplateHash:  updateTemplateHash,
		CreatedAtUnix:       time.Now().Unix(),
	}
	signedCommit, err := payflow.SignClientCommitEnvelope(commit, args.ClientActor.PrivKey)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	prevState, _, err := payflow.ExtractProofStateFromTxHex(args.Session.CurrentTxHex)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	commitHash, err := payflow.HashClientCommit(commit)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	accepted := payflow.AcceptedCharge{
		IntentHash:          intentHash,
		ClientCommitHash:    commitHash,
		SpendTxID:           strings.TrimSpace(args.Session.SpendTxID),
		SequenceNumber:      nextSequence,
		ServerAmountBefore:  args.Session.ServerAmount,
		ChargeAmountSatoshi: chargeAmount,
		ServerAmountAfter:   nextServerAmount,
		ServiceDeadlineUnix: args.ServiceDeadlineUnix,
		PrevAcceptedHash:    prevState.AcceptedTipHash,
	}
	state, err := payflow.BuildNextProofState(prevState, accepted)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	acceptedHash, err := payflow.HashAcceptedCharge(accepted)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	stateRaw, err := payflow.MarshalProofState(state)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	updatedTx, err := ce.LoadTxWithProof(
		args.Session.CurrentTxHex,
		nil,
		nextSequence,
		nextServerAmount,
		args.GatewayPub,
		args.ClientActor.PubKey,
		args.Session.PoolAmountSat,
		stateRaw,
	)
	if err != nil {
		return feePoolProofBuilt{}, err
	}
	return feePoolProofBuilt{
		UpdatedTx:          updatedTx,
		ProofIntent:        intentRaw,
		SignedProofCommit:  append([]byte(nil), signedCommit...),
		ProofStatePayload:  stateRaw,
		AcceptedChargeHash: acceptedHash,
	}, nil
}
