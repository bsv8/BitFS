package clientapp

import (
	"encoding/hex"
	"encoding/json"

	broadcastmodule "github.com/bsv8/BitFS/pkg/clientapp/modules/broadcast"
)

func marshalSignedNodeReachabilityAnnouncement(ann broadcastmodule.NodeReachabilityAnnouncement) ([]byte, error) {
	ann = ann.Normalize()
	if err := broadcastmodule.VerifyNodeReachabilityAnnouncement(ann); err != nil {
		return nil, err
	}
	return json.Marshal([]any{ann.UnsignedArray(), hex.EncodeToString(ann.Signature)})
}
