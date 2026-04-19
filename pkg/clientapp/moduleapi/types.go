package moduleapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
)

// Error 是模块对外统一错误壳。
//
// 设计说明：
// - 这里只保留码和值，不再引入各模块自己的错误体系；
// - 错误文本只给人看，错误码才给流程判断；
// - 主干在把模块错误转回内部钩子时，只认这里这套口径。
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
	return &Error{
		Code:    strings.TrimSpace(code),
		Message: strings.TrimSpace(message),
	}
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

// Conn 是模块可见的最小数据库连接能力。
type Conn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Store 是模块可见的最小存储能力。
//
// 设计说明：
// - 模块只拿能力，不拿 *sql.DB；
// - 串行能力由主干统一提供，模块只看是否存在；
// - 查询/写入必须在闭包里完成，避免句柄泄露到外层。
type Store interface {
	Conn
	Do(context.Context, func(Conn) error) error
	SerialAccess() bool
}

// HTTPHandler 是模块注册 HTTP 路由时的处理函数。
type HTTPHandler func(http.ResponseWriter, *http.Request)

// OpenHook 是模块启动阶段的生命周期钩子。
type OpenHook func(context.Context) error

// CloseHook 是模块关闭阶段的生命周期钩子。
type CloseHook func(context.Context) error

// SettingsHook 是模块 settings 动作处理钩子。
type SettingsHook func(context.Context, string, map[string]any) (map[string]any, error)

// OBSControlHook 是模块 OBS 动作处理钩子。
type OBSControlHook func(context.Context, string, map[string]any) (OBSActionResponse, error)

// OBSActionResponse 是模块 OBS 动作的统一返回壳。
type OBSActionResponse struct {
	OK      bool
	Result  string
	Error   string
	Payload map[string]any
}

// DomainResolveHook 是域名解析 provider 钩子。
type DomainResolveHook func(context.Context, string) (string, error)

// LibP2PProtocol 是模块使用的 libp2p 协议分支名。
type LibP2PProtocol string

const (
	LibP2PProtocolNodeCall    LibP2PProtocol = "node.call"
	LibP2PProtocolNodeResolve LibP2PProtocol = "node.resolve"
)

// PeerCallRequest 是模块发起远端节点调用时使用的出站请求。
type PeerCallRequest struct {
	To                   string
	Route                string
	ContentType          string
	Body                 []byte
	PaymentMode          string
	PaymentScheme        string
	ServiceQuote         []byte
	RequireActiveFeePool bool
}

// LibP2PRequest 是入站 libp2p 事件携带的请求壳。
type LibP2PRequest struct {
	To             string
	Route          string
	ContentType    string
	Body           []byte
	PaymentScheme  string
	PaymentPayload []byte
}

// CallResponse 是模块间调用结果的统一外壳。
type CallResponse struct {
	Ok                   bool
	Code                 string
	Message              string
	ContentType          string
	Body                 []byte
	PaymentSchemes       []*PaymentOption
	PaymentReceiptScheme string
	PaymentReceipt       []byte
	ServiceQuote         []byte
	ServiceReceipt       []byte
}

// PeerCallResponse 作为 PeerCall 的返回结果，和 CallResponse 保持同一口径。
type PeerCallResponse = CallResponse

// PaymentOption 是节点支付选项的公共描述。
type PaymentOption struct {
	Scheme                   string
	PaymentDomain            string
	AmountSatoshi            uint64
	Description              string
	MinimumPoolAmountSatoshi uint64
	FeeRateSatPerByteMilli   uint64
	LockBlocks               uint32
	PricingMode              string
	ServiceQuantity          uint64
	ServiceQuantityUnit      string
	QuoteStatus              string
}

// ResolveManifest 是 libp2p resolve 返回的公共结果。
type ResolveManifest struct {
	Route               string
	SeedHash            string
	RecommendedFileName string
	MIMEHint            string
	FileSize            int64
	UpdatedAtUnix       int64
}

// LibP2PEvent 是模块注册的 libp2p 钩子输入。
type LibP2PEvent struct {
	Protocol        LibP2PProtocol
	Route           string
	MessageID       string
	SenderPubkeyHex string
	Request         LibP2PRequest
}

// LibP2PResult 是模块注册的 libp2p 钩子输出。
type LibP2PResult struct {
	CallResp        CallResponse
	ResolveManifest ResolveManifest
}

// LibP2PHook 是模块的 libp2p 处理钩子。
type LibP2PHook func(context.Context, LibP2PEvent) (LibP2PResult, error)

// PeerNode 是节点快照里的单个节点描述。
type PeerNode struct {
	Enabled                   bool
	Addr                      string
	Pubkey                    string
	ListenOfferPaymentSatoshi uint64
}

// LockedFunction 是模块本地白名单项。
type LockedFunction struct {
	ID               string
	Module           string
	Package          string
	Symbol           string
	Signature        string
	ObsControlAction string
	Note             string
}

// Installer 是模块统一安装入口。
type Installer func(context.Context, Host) (func(), error)

// Host 是模块接入主干时可见的窄能力面。
//
// 设计说明：
// - 模块只通过这个面接入主干；
// - 这里不暴露 Runtime、*sql.DB、httpAPIServer；
// - 注册和执行分开，避免模块直接摸主干内部结构。
type Host interface {
	Store() Store

	InstallModule(ModuleSpec) (func(), error)
	RegisterCapability(moduleID string, version uint32) (func(), error)
	RegisterLibP2P(protocol LibP2PProtocol, route string, hook LibP2PHook) (func(), error)
	RegisterHTTPRoute(path string, handler HTTPHandler) (func(), error)
	RegisterSettingsAction(action string, hook SettingsHook) (func(), error)
	RegisterOBSAction(action string, hook OBSControlHook) (func(), error)
	RegisterDomainResolveHook(name string, hook DomainResolveHook) (func(), error)
	RegisterOpenHook(hook OpenHook) (func(), error)
	RegisterCloseHook(hook CloseHook) (func(), error)
	RegisterModuleLockProvider(module string, provider func() []LockedFunction) (func(), error)

	PeerCall(context.Context, PeerCallRequest) (PeerCallResponse, error)
	GatewaySnapshot() []PeerNode
}

type HTTPRoute struct {
	Path    string
	Handler HTTPHandler
}

type LibP2PRoute struct {
	Protocol LibP2PProtocol
	Route    string
	Handler  LibP2PHook
}

type SettingsAction struct {
	Action  string
	Handler SettingsHook
}

type OBSAction struct {
	Action  string
	Handler OBSControlHook
}

type DomainResolver struct {
	Name    string
	Handler DomainResolveHook
}

type ModuleSpec struct {
	ID      string
	Version uint32

	HTTP            []HTTPRoute
	LibP2P          []LibP2PRoute
	Settings        []SettingsAction
	OBS             []OBSAction
	DomainResolvers []DomainResolver
	OpenHooks       []OpenHook
	CloseHooks      []CloseHook

	ModuleLockProvider func() []LockedFunction
	ModuleLockName     string
	Cleanup            func()
}
