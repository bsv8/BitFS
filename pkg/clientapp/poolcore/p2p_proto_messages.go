package poolcore

import oldproto "github.com/golang/protobuf/proto"

func (m *InfoReq) Reset()         { *m = InfoReq{} }
func (m *InfoReq) String() string { return oldproto.CompactTextString(m) }
func (*InfoReq) ProtoMessage()    {}

func (m *InfoResp) Reset()         { *m = InfoResp{} }
func (m *InfoResp) String() string { return oldproto.CompactTextString(m) }
func (*InfoResp) ProtoMessage()    {}

func (m *ServiceQuoteReq) Reset()         { *m = ServiceQuoteReq{} }
func (m *ServiceQuoteReq) String() string { return oldproto.CompactTextString(m) }
func (*ServiceQuoteReq) ProtoMessage()    {}

func (m *ServiceQuoteResp) Reset()         { *m = ServiceQuoteResp{} }
func (m *ServiceQuoteResp) String() string { return oldproto.CompactTextString(m) }
func (*ServiceQuoteResp) ProtoMessage()    {}

func (m *CreateReq) Reset()         { *m = CreateReq{} }
func (m *CreateReq) String() string { return oldproto.CompactTextString(m) }
func (*CreateReq) ProtoMessage()    {}

func (m *CreateResp) Reset()         { *m = CreateResp{} }
func (m *CreateResp) String() string { return oldproto.CompactTextString(m) }
func (*CreateResp) ProtoMessage()    {}

func (m *BaseTxReq) Reset()         { *m = BaseTxReq{} }
func (m *BaseTxReq) String() string { return oldproto.CompactTextString(m) }
func (*BaseTxReq) ProtoMessage()    {}

func (m *BaseTxResp) Reset()         { *m = BaseTxResp{} }
func (m *BaseTxResp) String() string { return oldproto.CompactTextString(m) }
func (*BaseTxResp) ProtoMessage()    {}

func (m *PayConfirmReq) Reset()         { *m = PayConfirmReq{} }
func (m *PayConfirmReq) String() string { return oldproto.CompactTextString(m) }
func (*PayConfirmReq) ProtoMessage()    {}

func (m *PayConfirmResp) Reset()         { *m = PayConfirmResp{} }
func (m *PayConfirmResp) String() string { return oldproto.CompactTextString(m) }
func (*PayConfirmResp) ProtoMessage()    {}

func (m *CloseReq) Reset()         { *m = CloseReq{} }
func (m *CloseReq) String() string { return oldproto.CompactTextString(m) }
func (*CloseReq) ProtoMessage()    {}

func (m *CloseResp) Reset()         { *m = CloseResp{} }
func (m *CloseResp) String() string { return oldproto.CompactTextString(m) }
func (*CloseResp) ProtoMessage()    {}

func (m *StateReq) Reset()         { *m = StateReq{} }
func (m *StateReq) String() string { return oldproto.CompactTextString(m) }
func (*StateReq) ProtoMessage()    {}

func (m *StateResp) Reset()         { *m = StateResp{} }
func (m *StateResp) String() string { return oldproto.CompactTextString(m) }
func (*StateResp) ProtoMessage()    {}
