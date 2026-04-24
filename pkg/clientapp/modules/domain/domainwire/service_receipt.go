package domainwire

import (
	"encoding/json"

	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
)

const (
	ServiceTypeResolveName  = contractroute.RouteDomainV1Resolve
	ServiceTypeQueryName    = contractroute.RouteDomainV1Query
	ServiceTypeRegisterLock = contractroute.RouteDomainV1Lock
	ServiceTypeSetTarget    = contractroute.RouteDomainV1SetTarget
)

func MarshalResolveNameServicePayload(resp ResolveNamePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.OwnerPubkeyHex,
		resp.TargetPubkeyHex,
		resp.ExpireAtUnix,
		resp.SignedRecordJSON,
		resp.Error,
	})
}

func MarshalQueryNameServicePayload(resp QueryNamePaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.Available,
		resp.Locked,
		resp.Registered,
		resp.OwnerPubkeyHex,
		resp.TargetPubkeyHex,
		resp.ExpireAtUnix,
		resp.LockExpiresAtUnix,
		resp.RegisterPriceSatoshi,
		resp.RegisterSubmitFeeSatoshi,
		resp.RegisterLockFeeSatoshi,
		resp.SetTargetFeeSatoshi,
		resp.ResolveFeeSatoshi,
		resp.QueryFeeSatoshi,
		resp.SignedRecordJSON,
		resp.Error,
	})
}

func MarshalRegisterLockServicePayload(resp RegisterLockPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.TargetPubkeyHex,
		resp.LockExpiresAtUnix,
		resp.SignedQuoteJSON,
		resp.Error,
	})
}

func MarshalSetTargetServicePayload(resp SetTargetPaidResp) ([]byte, error) {
	return json.Marshal([]any{
		resp.Status,
		resp.Name,
		resp.OwnerPubkeyHex,
		resp.TargetPubkeyHex,
		resp.ExpireAtUnix,
		resp.SignedRecordJSON,
		resp.Error,
	})
}
