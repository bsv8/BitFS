package indexresolve

import (
	"errors"
	"strings"

	oldproto "github.com/golang/protobuf/proto"
)

// Error 是模块统一错误壳。
//
// 设计说明：
// - 只保留码和值，不再拆出第二套错误体系；
// - 业务、HTTP、OBS、P2P 都围绕这套口径做映射；
// - 错误文本只给人看，错误码才是流程判断依据。
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

func NewError(code, message string) error {
	return &Error{Code: strings.TrimSpace(code), Message: strings.TrimSpace(message)}
}

func CodeOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.Code)
	}
	return ""
}

func MessageOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.Message)
	}
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

// RouteItem 是 settings 列表和写入结果的共享类型。
type RouteItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// SeedItem 是 route 解析时会用到的种子摘要。
type SeedItem struct {
	SeedHash            string `json:"seed_hash"`
	RecommendedFileName string `json:"recommended_file_name"`
	MIMEHint            string `json:"mime_hint"`
	FileSize            int64  `json:"file_size"`
}

// Manifest 是解析出口的共享结果。
//
// 设计说明：
// - 保留 protobuf 兼容只为现有链路复用，不再包一层 Service；
// - 行为入口已经收拢到 Biz*，这里仅保存传输类型。
type Manifest struct {
	Route               string `protobuf:"bytes,1,opt,name=route,proto3" json:"route"`
	SeedHash            string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	RecommendedFileName string `protobuf:"bytes,3,opt,name=recommended_file_name,json=recommendedFileName,proto3" json:"recommended_file_name,omitempty"`
	MIMEHint            string `protobuf:"bytes,4,opt,name=mime_hint,json=mimeHint,proto3" json:"mime_hint,omitempty"`
	FileSize            int64  `protobuf:"varint,5,opt,name=file_size,json=fileSize,proto3" json:"file_size,omitempty"`
	UpdatedAtUnix       int64  `protobuf:"varint,6,opt,name=updated_at_unix,json=updatedAtUnix,proto3" json:"updated_at_unix"`
}

func (m *Manifest) Reset()         { *m = Manifest{} }
func (m *Manifest) String() string { return oldproto.CompactTextString(m) }
func (*Manifest) ProtoMessage()    {}
