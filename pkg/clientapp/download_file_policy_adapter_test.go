package clientapp

import (
	"context"
	"testing"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

func TestDownloadFilePolicyAllowsSeedPriceAndChunkPriceIndependently(t *testing.T) {
	t.Parallel()

	policy := newDownloadFilePolicyAdapter()
	selected, ok, reason, err := policy.SelectQuote(context.Background(), filedownload.StartRequest{
		MaxSeedPrice:  50,
		MaxChunkPrice: 30,
	}, []filedownload.QuoteReport{
		{
			SellerPubkey:  "seller_a",
			SeedPriceSat:  10,
			ChunkPriceSat: 20,
			ChunkCount:    10,
		},
	})
	if err != nil {
		t.Fatalf("select quote failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected quote to pass, got reason=%s", reason)
	}
	if selected.SellerPubkey != "seller_a" {
		t.Fatalf("expected seller_a selected, got %s", selected.SellerPubkey)
	}
}

func TestDownloadFilePolicyBlocksBySeedPriceOnly(t *testing.T) {
	t.Parallel()

	policy := newDownloadFilePolicyAdapter()
	_, ok, reason, err := policy.SelectQuote(context.Background(), filedownload.StartRequest{
		MaxSeedPrice:  9,
		MaxChunkPrice: 30,
	}, []filedownload.QuoteReport{
		{
			SellerPubkey:  "seller_a",
			SeedPriceSat:  10,
			ChunkPriceSat: 20,
			ChunkCount:    10,
		},
	})
	if err != nil {
		t.Fatalf("select quote failed: %v", err)
	}
	if ok {
		t.Fatalf("expected quote to be blocked")
	}
	if reason != "blocked_by_budget" {
		t.Fatalf("expected blocked_by_budget, got %s", reason)
	}
}

func TestDownloadFilePolicyIgnoresTotalForBudgetGate(t *testing.T) {
	t.Parallel()

	policy := newDownloadFilePolicyAdapter()
	selected, ok, reason, err := policy.SelectQuote(context.Background(), filedownload.StartRequest{
		MaxSeedPrice:  10,
		MaxChunkPrice: 10,
	}, []filedownload.QuoteReport{
		{
			SellerPubkey:  "seller_a",
			SeedPriceSat:  10,
			ChunkPriceSat: 10,
			ChunkCount:    1000,
		},
	})
	if err != nil {
		t.Fatalf("select quote failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected quote to pass, got reason=%s", reason)
	}
	if selected.SellerPubkey != "seller_a" {
		t.Fatalf("expected seller_a selected, got %s", selected.SellerPubkey)
	}
}
