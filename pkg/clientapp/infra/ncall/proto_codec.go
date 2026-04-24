package ncall

import (
	"fmt"
	"strings"

	oldproto "github.com/golang/protobuf/proto"
)

// MarshalProto 统一把 protobuf 响应封装成 node.call 输出。
// 设计说明：
// - node.call 的公开壳只关心“是否成功 + body + content_type”；
// - 具体业务 message 的 protobuf 结构由各模块自己维护；
// - 这样 gateway/domain/arbiter 不必各自复制一遍样板封装代码。
func MarshalProto(msg oldproto.Message) (CallResp, error) {
	body, err := oldproto.Marshal(msg)
	if err != nil {
		return CallResp{}, err
	}
	return CallResp{
		Ok:          true,
		Code:        "OK",
		ContentType: ContentTypeProto,
		Body:        body,
	}, nil
}

// DecodeProto 统一处理 node.call protobuf body 的空体和解码错误。
func DecodeProto(route string, raw []byte, out oldproto.Message, required bool) error {
	if out == nil {
		return fmt.Errorf("proto body target missing")
	}
	if len(raw) == 0 {
		if required {
			return fmt.Errorf("request body missing")
		}
		return nil
	}
	if err := oldproto.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("invalid protobuf body: route=%s: %w", strings.TrimSpace(route), err)
	}
	return nil
}
