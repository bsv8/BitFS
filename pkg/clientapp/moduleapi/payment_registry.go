package moduleapi

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// PaymentRegistry is the registry for payment capabilities.
type PaymentRegistry interface {
	RegisterQuotedServicePayer(scheme string, executor QuotedServicePayer) (cleanup func(), err error)
	RegisterServiceCoverageSession(scheme string, executor ServiceCoverageSession) (cleanup func(), err error)
	RegisterTradeSession(scheme string, executor TradeSession) (cleanup func(), err error)

	DispatchQuotedServicePayer(ctx context.Context, scheme string, quote []byte, request ServiceCallRequest, targetPeer string) (ServiceCallResponse, []byte, error)

	DispatchServiceCoverageSession(ctx context.Context, scheme, ability string, args interface{}) (interface{}, error)
	DispatchTradeSession(ctx context.Context, scheme, ability string, args interface{}) (interface{}, error)

	GetRegisteredSchemes() []string
	GetSchemeAbility(scheme string) []string
}

type paymentRegistry struct {
	mu sync.RWMutex

	quotedServicePayers    map[string]QuotedServicePayer
	serviceCoverageSessions map[string]ServiceCoverageSession
	tradeSessions          map[string]TradeSession

	coverageSessionsByRef map[string]string
	tradeSessionsByRef     map[string]string

	nextHookID uint64
	hooks      map[uint64]paymentRegistryHook
}

type paymentRegistryHook struct {
	id uint64
	fn func()
}

func newPaymentRegistry() *paymentRegistry {
	return &paymentRegistry{
		quotedServicePayers:    make(map[string]QuotedServicePayer),
		serviceCoverageSessions: make(map[string]ServiceCoverageSession),
		tradeSessions:          make(map[string]TradeSession),
		coverageSessionsByRef:  make(map[string]string),
		tradeSessionsByRef:      make(map[string]string),
		hooks:                  make(map[uint64]paymentRegistryHook),
	}
}

func NewPaymentRegistry() PaymentRegistry {
	return newPaymentRegistry()
}

func (r *paymentRegistry) RegisterQuotedServicePayer(scheme string, executor QuotedServicePayer) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		return nil, fmt.Errorf("scheme is required")
	}
	if executor == nil {
		return nil, fmt.Errorf("executor is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.quotedServicePayers[scheme]; exists {
		return nil, fmt.Errorf("quoted service payer already registered for scheme: %s", scheme)
	}

	r.quotedServicePayers[scheme] = executor
	r.nextHookID++
	hookID := r.nextHookID

	once := sync.Once{}
	cleanup := func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.quotedServicePayers, scheme)
			delete(r.hooks, hookID)
			r.mu.Unlock()
		})
	}

	r.hooks[hookID] = paymentRegistryHook{id: hookID, fn: cleanup}
	return cleanup, nil
}

func (r *paymentRegistry) RegisterServiceCoverageSession(scheme string, executor ServiceCoverageSession) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		return nil, fmt.Errorf("scheme is required")
	}
	if executor == nil {
		return nil, fmt.Errorf("executor is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.serviceCoverageSessions[scheme]; exists {
		return nil, fmt.Errorf("service coverage session already registered for scheme: %s", scheme)
	}

	r.serviceCoverageSessions[scheme] = executor
	r.nextHookID++
	hookID := r.nextHookID

	once := sync.Once{}
	cleanup := func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.serviceCoverageSessions, scheme)
			delete(r.hooks, hookID)
			r.mu.Unlock()
		})
	}

	r.hooks[hookID] = paymentRegistryHook{id: hookID, fn: cleanup}
	return cleanup, nil
}

func (r *paymentRegistry) RegisterTradeSession(scheme string, executor TradeSession) (func(), error) {
	if r == nil {
		return func() {}, nil
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		return nil, fmt.Errorf("scheme is required")
	}
	if executor == nil {
		return nil, fmt.Errorf("executor is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tradeSessions[scheme]; exists {
		return nil, fmt.Errorf("trade session already registered for scheme: %s", scheme)
	}

	r.tradeSessions[scheme] = executor
	r.nextHookID++
	hookID := r.nextHookID

	once := sync.Once{}
	cleanup := func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.tradeSessions, scheme)
			delete(r.hooks, hookID)
			r.mu.Unlock()
		})
	}

	r.hooks[hookID] = paymentRegistryHook{id: hookID, fn: cleanup}
	return cleanup, nil
}

func (r *paymentRegistry) DispatchQuotedServicePayer(ctx context.Context, scheme string, quote []byte, request ServiceCallRequest, targetPeer string) (ServiceCallResponse, []byte, error) {
	if r == nil {
		return ServiceCallResponse{}, nil, UnavailableScheme(scheme)
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		return ServiceCallResponse{}, nil, UnavailableScheme("")
	}

	r.mu.RLock()
	executor, exists := r.quotedServicePayers[scheme]
	r.mu.RUnlock()

	if !exists {
		return ServiceCallResponse{}, nil, UnavailableScheme(scheme)
	}

	resp, receipt, err := executor.PayQuotedService(ctx, scheme, quote, request, targetPeer)
	if err != nil {
		return resp, receipt, err
	}
	return resp, receipt, nil
}

type EnsureCoverageArgs struct {
	TargetPeer        string
	RequiredDuration  uint64
}

type EnsureCoverageResult struct {
	SessionRef        string
	CoverageExpiresAt int64
}

type RenewCoverageArgs struct {
	SessionRef         string
	AdditionalDuration uint64
}

type RotateCoverageArgs struct {
	Scheme     string
	SessionRef string
	NewAmount  uint64
}

type RotateCoverageResult struct {
	NewSessionRef string
}

type CloseCoverageArgs struct {
	SessionRef string
	Reason     string
}

func (r *paymentRegistry) DispatchServiceCoverageSession(ctx context.Context, scheme, ability string, args interface{}) (interface{}, error) {
	if r == nil {
		return nil, UnavailableScheme(scheme)
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		sessionRef := extractSessionRef(args)
		if sessionRef == "" {
			return nil, UnavailableScheme("")
		}
		var resolvedScheme string
		r.mu.RLock()
		resolvedScheme = r.coverageSessionsByRef[sessionRef]
		r.mu.RUnlock()
		if resolvedScheme == "" {
			return nil, SessionNotFound(sessionRef)
		}
		scheme = resolvedScheme
	}

	r.mu.RLock()
	executor, exists := r.serviceCoverageSessions[scheme]
	r.mu.RUnlock()

	if !exists {
		return nil, UnavailableScheme(scheme)
	}

	switch strings.ToLower(strings.TrimSpace(ability)) {
	case "ensure":
		a, ok := args.(EnsureCoverageArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for ensure coverage")
		}
		sessionRef, coverageExpiresAt, err := executor.EnsureCoverage(ctx, scheme, a.TargetPeer, a.RequiredDuration)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.coverageSessionsByRef[sessionRef] = scheme
		r.mu.Unlock()
		return EnsureCoverageResult{SessionRef: sessionRef, CoverageExpiresAt: coverageExpiresAt}, nil

	case "renew":
		a, ok := args.(RenewCoverageArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for renew coverage")
		}
		exp, err := executor.RenewCoverage(ctx, a.SessionRef, a.AdditionalDuration)
		return exp, err

	case "rotate":
		a, ok := args.(RotateCoverageArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for rotate coverage")
		}
		newRef, err := executor.RotateCoverage(ctx, scheme, a.SessionRef, a.NewAmount)
		if err != nil {
			return nil, err
		}
		if newRef != "" {
			r.mu.Lock()
			r.coverageSessionsByRef[newRef] = scheme
			delete(r.coverageSessionsByRef, a.SessionRef)
			r.mu.Unlock()
		}
		return RotateCoverageResult{NewSessionRef: newRef}, nil

	case "close":
		a, ok := args.(CloseCoverageArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for close coverage")
		}
		err := executor.CloseCoverage(ctx, a.SessionRef, a.Reason)
		if err == nil {
			r.mu.Lock()
			delete(r.coverageSessionsByRef, a.SessionRef)
			r.mu.Unlock()
		}
		return nil, err

	default:
		return nil, UnsupportedAbility(scheme, ability)
	}
}

type OpenTradeSessionArgs struct {
	TradeID     string
	Buyer       string
	Seller      string
	Arbiter     string
	TotalAmount uint64
	ChunkCount  uint32
}

type OpenTradeSessionResult struct {
	SessionRef   string
	InitialEntry TradeEntry
}

type AppendTradeEntryArgs struct {
	SessionRef string
	Entry      TradeEntry
}

type OpenArbitrationArgs struct {
	SessionRef string
	ChunkIndex uint32
	Evidence   string
}

type OpenArbitrationResult struct {
	CaseID string
}

type CloseTradeSessionArgs struct {
	SessionRef string
	FinalEntry TradeEntry
}

func (r *paymentRegistry) DispatchTradeSession(ctx context.Context, scheme, ability string, args interface{}) (interface{}, error) {
	if r == nil {
		return nil, UnavailableScheme(scheme)
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		sessionRef := extractSessionRefFromTradeArgs(args)
		if sessionRef == "" {
			return nil, UnavailableScheme("")
		}
		var resolvedScheme string
		r.mu.RLock()
		resolvedScheme = r.tradeSessionsByRef[sessionRef]
		r.mu.RUnlock()
		if resolvedScheme == "" {
			return nil, SessionNotFound(sessionRef)
		}
		scheme = resolvedScheme
	}

	r.mu.RLock()
	executor, exists := r.tradeSessions[scheme]
	r.mu.RUnlock()

	if !exists {
		return nil, UnavailableScheme(scheme)
	}

	switch strings.ToLower(strings.TrimSpace(ability)) {
	case "open":
		a, ok := args.(OpenTradeSessionArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for open trade session")
		}
		sessionRef, initialEntry, err := executor.OpenTradeSession(ctx, scheme, a.TradeID, a.Buyer, a.Seller, a.Arbiter, a.TotalAmount, a.ChunkCount)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.tradeSessionsByRef[sessionRef] = scheme
		r.mu.Unlock()
		return OpenTradeSessionResult{SessionRef: sessionRef, InitialEntry: initialEntry}, nil

	case "append":
		a, ok := args.(AppendTradeEntryArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for append trade entry")
		}
		entry, err := executor.AppendTradeEntry(ctx, a.SessionRef, a.Entry)
		return entry, err

	case "arbitrate":
		a, ok := args.(OpenArbitrationArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for open arbitration")
		}
		caseID, err := executor.OpenArbitration(ctx, a.SessionRef, a.ChunkIndex, a.Evidence)
		if err != nil {
			return nil, err
		}
		return OpenArbitrationResult{CaseID: caseID}, nil

	case "close":
		a, ok := args.(CloseTradeSessionArgs)
		if !ok {
			return nil, fmt.Errorf("invalid args for close trade session")
		}
		result, err := executor.CloseTradeSession(ctx, a.SessionRef, a.FinalEntry)
		if err == nil {
			r.mu.Lock()
			delete(r.tradeSessionsByRef, a.SessionRef)
			r.mu.Unlock()
		}
		return result, err

	default:
		return nil, UnsupportedAbility(scheme, ability)
	}
}

func (r *paymentRegistry) GetRegisteredSchemes() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.quotedServicePayers)+len(r.serviceCoverageSessions)+len(r.tradeSessions))
	seen := make(map[string]struct{}, len(out))

	addScheme := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" {
			return
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}

	for scheme := range r.quotedServicePayers {
		addScheme(scheme)
	}
	for scheme := range r.serviceCoverageSessions {
		addScheme(scheme)
	}
	for scheme := range r.tradeSessions {
		addScheme(scheme)
	}

	return out
}

func (r *paymentRegistry) GetSchemeAbility(scheme string) []string {
	if r == nil {
		return nil
	}
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var abilities []string
	if _, exists := r.quotedServicePayers[scheme]; exists {
		abilities = append(abilities, "QuotedServicePayer")
	}
	if _, exists := r.serviceCoverageSessions[scheme]; exists {
		abilities = append(abilities, "ServiceCoverageSession")
	}
	if _, exists := r.tradeSessions[scheme]; exists {
		abilities = append(abilities, "TradeSession")
	}

	return abilities
}

func extractSessionRef(args interface{}) string {
	switch a := args.(type) {
	case EnsureCoverageArgs:
		return ""
	case RenewCoverageArgs:
		return a.SessionRef
	case RotateCoverageArgs:
		return a.SessionRef
	case CloseCoverageArgs:
		return a.SessionRef
	default:
		return ""
	}
}

func extractSessionRefFromTradeArgs(args interface{}) string {
	switch a := args.(type) {
	case OpenTradeSessionArgs:
		return ""
	case AppendTradeEntryArgs:
		return a.SessionRef
	case OpenArbitrationArgs:
		return a.SessionRef
	case CloseTradeSessionArgs:
		return a.SessionRef
	default:
		return ""
	}
}
