package clientapp

import oldproto "github.com/golang/protobuf/proto"

func (m *healthReq) Reset()         { *m = healthReq{} }
func (m *healthReq) String() string { return oldproto.CompactTextString(m) }
func (*healthReq) ProtoMessage()    {}

func (m *healthResp) Reset()         { *m = healthResp{} }
func (m *healthResp) String() string { return oldproto.CompactTextString(m) }
func (*healthResp) ProtoMessage()    {}

func (m *seedGetReq) Reset()         { *m = seedGetReq{} }
func (m *seedGetReq) String() string { return oldproto.CompactTextString(m) }
func (*seedGetReq) ProtoMessage()    {}

func (m *seedGetResp) Reset()         { *m = seedGetResp{} }
func (m *seedGetResp) String() string { return oldproto.CompactTextString(m) }
func (*seedGetResp) ProtoMessage()    {}

func (m *directQuoteSubmitReq) Reset()         { *m = directQuoteSubmitReq{} }
func (m *directQuoteSubmitReq) String() string { return oldproto.CompactTextString(m) }
func (*directQuoteSubmitReq) ProtoMessage()    {}

func (m *directQuoteSubmitResp) Reset()         { *m = directQuoteSubmitResp{} }
func (m *directQuoteSubmitResp) String() string { return oldproto.CompactTextString(m) }
func (*directQuoteSubmitResp) ProtoMessage()    {}

func (m *directDealAcceptReq) Reset()         { *m = directDealAcceptReq{} }
func (m *directDealAcceptReq) String() string { return oldproto.CompactTextString(m) }
func (*directDealAcceptReq) ProtoMessage()    {}

func (m *directDealAcceptResp) Reset()         { *m = directDealAcceptResp{} }
func (m *directDealAcceptResp) String() string { return oldproto.CompactTextString(m) }
func (*directDealAcceptResp) ProtoMessage()    {}

func (m *directSessionOpenReq) Reset()         { *m = directSessionOpenReq{} }
func (m *directSessionOpenReq) String() string { return oldproto.CompactTextString(m) }
func (*directSessionOpenReq) ProtoMessage()    {}

func (m *directSessionOpenResp) Reset()         { *m = directSessionOpenResp{} }
func (m *directSessionOpenResp) String() string { return oldproto.CompactTextString(m) }
func (*directSessionOpenResp) ProtoMessage()    {}

func (m *directSessionCloseReq) Reset()         { *m = directSessionCloseReq{} }
func (m *directSessionCloseReq) String() string { return oldproto.CompactTextString(m) }
func (*directSessionCloseReq) ProtoMessage()    {}

func (m *directSessionCloseResp) Reset()         { *m = directSessionCloseResp{} }
func (m *directSessionCloseResp) String() string { return oldproto.CompactTextString(m) }
func (*directSessionCloseResp) ProtoMessage()    {}

func (m *directTransferPoolOpenReq) Reset()         { *m = directTransferPoolOpenReq{} }
func (m *directTransferPoolOpenReq) String() string { return oldproto.CompactTextString(m) }
func (*directTransferPoolOpenReq) ProtoMessage()    {}

func (m *directTransferPoolOpenResp) Reset()         { *m = directTransferPoolOpenResp{} }
func (m *directTransferPoolOpenResp) String() string { return oldproto.CompactTextString(m) }
func (*directTransferPoolOpenResp) ProtoMessage()    {}

func (m *directTransferPoolPayReq) Reset()         { *m = directTransferPoolPayReq{} }
func (m *directTransferPoolPayReq) String() string { return oldproto.CompactTextString(m) }
func (*directTransferPoolPayReq) ProtoMessage()    {}

func (m *directTransferPoolPayResp) Reset()         { *m = directTransferPoolPayResp{} }
func (m *directTransferPoolPayResp) String() string { return oldproto.CompactTextString(m) }
func (*directTransferPoolPayResp) ProtoMessage()    {}

func (m *directTransferPoolCloseReq) Reset()         { *m = directTransferPoolCloseReq{} }
func (m *directTransferPoolCloseReq) String() string { return oldproto.CompactTextString(m) }
func (*directTransferPoolCloseReq) ProtoMessage()    {}

func (m *directTransferPoolCloseResp) Reset()         { *m = directTransferPoolCloseResp{} }
func (m *directTransferPoolCloseResp) String() string { return oldproto.CompactTextString(m) }
func (*directTransferPoolCloseResp) ProtoMessage()    {}
