package poolcore

import "testing"

func TestNewServiceCatalogBuildsLookupAndBoundReasons(t *testing.T) {
	catalog, err := NewServiceCatalog(
		BillableServiceDecl{
			ServiceType:  "domain.v1.resolve",
			ChargeReason: "domain_resolve_fee",
			Quote: func(BillableServiceContext) (BillableServiceDecision, error) {
				return BillableServiceDecision{ChargeAmountSatoshi: 1}, nil
			},
		},
		BillableServiceDecl{
			ServiceType:  "domain.v1.query",
			ChargeReason: "domain_query_fee",
			Quote: func(BillableServiceContext) (BillableServiceDecision, error) {
				return BillableServiceDecision{ChargeAmountSatoshi: 2}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("NewServiceCatalog() error = %v", err)
	}
	if _, ok := catalog.lookup(" DOMAIN.V1.RESOLVE "); !ok {
		t.Fatal("expected normalized service lookup to succeed")
	}
	bound := catalog.BoundQuoteChargeReasons()
	if _, ok := bound["domain_resolve_fee"]; !ok {
		t.Fatal("domain_resolve_fee missing from bound reasons")
	}
	if _, ok := bound["domain_query_fee"]; !ok {
		t.Fatal("domain_query_fee missing from bound reasons")
	}
}

func TestNewServiceCatalogRejectsDuplicateServiceType(t *testing.T) {
	_, err := NewServiceCatalog(
		BillableServiceDecl{
			ServiceType: "domain.v1.resolve",
			Quote: func(BillableServiceContext) (BillableServiceDecision, error) {
				return BillableServiceDecision{ChargeAmountSatoshi: 1}, nil
			},
		},
		BillableServiceDecl{
			ServiceType: " DOMAIN.V1.RESOLVE ",
			Quote: func(BillableServiceContext) (BillableServiceDecision, error) {
				return BillableServiceDecision{ChargeAmountSatoshi: 1}, nil
			},
		},
	)
	if err == nil {
		t.Fatal("expected duplicate service_type error")
	}
}
