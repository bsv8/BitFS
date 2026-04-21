package clientapp

import oldproto "github.com/golang/protobuf/proto"

func (m *healthReq) Reset()         { *m = healthReq{} }
func (m *healthReq) String() string { return oldproto.CompactTextString(m) }
func (*healthReq) ProtoMessage()    {}

func (m *healthResp) Reset()         { *m = healthResp{} }
func (m *healthResp) String() string { return oldproto.CompactTextString(m) }
func (*healthResp) ProtoMessage()    {}
