package clientapp

import oldproto "github.com/golang/protobuf/proto"

func (m *healthReq) Reset()         { *m = healthReq{} }
func (m *healthReq) String() string { return oldproto.CompactTextString(m) }
func (*healthReq) ProtoMessage()    {}

func (m *routeIndexManifest) Reset()         { *m = routeIndexManifest{} }
func (m *routeIndexManifest) String() string { return oldproto.CompactTextString(m) }
func (*routeIndexManifest) ProtoMessage()    {}

func (m *resolverResolveResp) Reset()         { *m = resolverResolveResp{} }
func (m *resolverResolveResp) String() string { return oldproto.CompactTextString(m) }
func (*resolverResolveResp) ProtoMessage()    {}

func (m *healthResp) Reset()         { *m = healthResp{} }
func (m *healthResp) String() string { return oldproto.CompactTextString(m) }
func (*healthResp) ProtoMessage()    {}
