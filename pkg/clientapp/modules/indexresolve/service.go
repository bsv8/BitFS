package indexresolve

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	oldproto "github.com/golang/protobuf/proto"
)

type RuntimeReader interface {
	NodePubkeyHex() string
}

type Store interface {
	ListIndexResolveRoutes(ctx context.Context) ([]RouteItem, error)
	ResolveIndexRoute(ctx context.Context, route string) (Manifest, error)
	UpsertIndexResolveRoute(ctx context.Context, route string, seedHash string, updatedAtUnix int64) (RouteItem, error)
	DeleteIndexResolveRoute(ctx context.Context, route string) error
	GetIndexResolveSeed(ctx context.Context, seedHash string) (SeedItem, error)
}

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

type Service struct {
	store         Store
	nodePubkeyHex string
	closed        uint32
}

type RouteItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

type SeedItem struct {
	SeedHash            string `json:"seed_hash"`
	RecommendedFileName string `json:"recommended_file_name"`
	MIMEHint            string `json:"mime_hint"`
	FileSize            int64  `json:"file_size"`
}

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

func NewService(store Store, runtime RuntimeReader) *Service {
	svc := &Service{store: store}
	if runtime != nil {
		svc.nodePubkeyHex = strings.TrimSpace(runtime.NodePubkeyHex())
	}
	return svc
}

func (s *Service) Enabled() bool {
	return s != nil && atomic.LoadUint32(&s.closed) == 0 && s.store != nil
}

func (s *Service) NodePubkeyHex() string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(s.nodePubkeyHex)
}

func (s *Service) Capability() *contractmessage.CapabilityItem {
	if s == nil || !s.Enabled() {
		return nil
	}
	return CapabilityItem()
}

func (s *Service) Close() {
	if s == nil {
		return
	}
	atomic.StoreUint32(&s.closed, 1)
}

func (s *Service) Resolve(ctx context.Context, rawRoute string) (Manifest, error) {
	if s == nil || !s.Enabled() {
		return Manifest{}, moduleDisabledErr()
	}
	if ctx == nil {
		return Manifest{}, NewError("BAD_REQUEST", "ctx is required")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return Manifest{}, err
	}
	manifest, err := s.store.ResolveIndexRoute(ctx, route)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Manifest{}, routeNotFoundErr()
		}
		var typed *Error
		if errors.As(err, &typed) {
			return Manifest{}, err
		}
		return Manifest{}, err
	}
	return manifest, nil
}

func (s *Service) List(ctx context.Context) ([]RouteItem, error) {
	if s == nil || !s.Enabled() {
		return nil, moduleDisabledErr()
	}
	if ctx == nil {
		return nil, NewError("BAD_REQUEST", "ctx is required")
	}
	return s.store.ListIndexResolveRoutes(ctx)
}

func (s *Service) Upsert(ctx context.Context, rawRoute string, rawSeedHash string, nowUnix int64) (RouteItem, error) {
	if s == nil || !s.Enabled() {
		return RouteItem{}, moduleDisabledErr()
	}
	if ctx == nil {
		return RouteItem{}, NewError("BAD_REQUEST", "ctx is required")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return RouteItem{}, err
	}
	seedHash, err := NormalizeSeedHashHex(rawSeedHash)
	if err != nil {
		return RouteItem{}, err
	}
	seed, err := s.store.GetIndexResolveSeed(ctx, seedHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RouteItem{}, seedNotFoundErr()
		}
		return RouteItem{}, err
	}
	if strings.TrimSpace(seed.SeedHash) == "" {
		return RouteItem{}, seedNotFoundErr()
	}
	if nowUnix <= 0 {
		nowUnix = time.Now().Unix()
	}
	return s.store.UpsertIndexResolveRoute(ctx, route, seedHash, nowUnix)
}

func (s *Service) Delete(ctx context.Context, rawRoute string) error {
	if s == nil || !s.Enabled() {
		return moduleDisabledErr()
	}
	if ctx == nil {
		return NewError("BAD_REQUEST", "ctx is required")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return err
	}
	return s.store.DeleteIndexResolveRoute(ctx, route)
}

func MarshalManifestPB(m Manifest) ([]byte, error) {
	return oldproto.Marshal(&m)
}

func UnmarshalManifestPB(raw []byte) (Manifest, error) {
	var out Manifest
	if err := oldproto.Unmarshal(raw, &out); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest failed: %w", err)
	}
	return out, nil
}
