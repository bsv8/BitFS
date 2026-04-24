package domainwire

import (
	"bytes"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/script"
)

// BuildSignedEnvelopeOpReturnScript 统一构造 domain 注册使用的完整签名原文 OP_RETURN。
// 设计说明：
// - 链上只接受完整签名原文，不做“部分字段”折中，避免验签语义被削弱；
// - 客户端与服务端都复用这套编码规则，防止一边能写一边读不出来。
func BuildSignedEnvelopeOpReturnScript(payload []byte) (*script.Script, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil, fmt.Errorf("op_return payload missing")
	}
	out := &script.Script{}
	if err := out.AppendOpcodes(script.OpFALSE, script.OpRETURN); err != nil {
		return nil, fmt.Errorf("build op_return prefix: %w", err)
	}
	if err := out.AppendPushData(payload); err != nil {
		return nil, fmt.Errorf("build op_return payload: %w", err)
	}
	return out, nil
}

// ExtractSignedEnvelopeOpReturnPayload 提取完整签名原文 payload。
// 注意：
// - go-sdk 的 Chunks() 默认会把 OP_RETURN 后续原样塞进 RETURN chunk.Data，不适合这里；
// - 这里使用 ParseOps()，按真实 pushdata 逐段读取，再拼回完整原文。
func ExtractSignedEnvelopeOpReturnPayload(lockingScript *script.Script) ([]byte, error) {
	if lockingScript == nil || !lockingScript.IsData() {
		return nil, fmt.Errorf("op_return output required")
	}
	ops, err := lockingScript.ParseOps()
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("op_return payload missing")
	}
	start := 0
	if ops[0].Op == script.OpFALSE {
		if len(ops) < 2 || ops[1].Op != script.OpRETURN {
			return nil, fmt.Errorf("op_return script invalid")
		}
		start = 2
	} else if ops[0].Op == script.OpRETURN {
		start = 1
	} else {
		return nil, fmt.Errorf("op_return script invalid")
	}
	payload := make([]byte, 0, len(lockingScript.Bytes()))
	for _, op := range ops[start:] {
		if len(op.Data) == 0 {
			continue
		}
		payload = append(payload, op.Data...)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("op_return payload missing")
	}
	return payload, nil
}
