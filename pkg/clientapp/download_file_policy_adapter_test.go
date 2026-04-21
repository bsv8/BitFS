package clientapp

import (
	"context"
	"testing"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

func TestDownloadFilePolicyAllowsSeedPriceAndChunkPriceIndependently(t *testing.T) {
	t.Parallel()

	policy := newDownloadFilePolicyAdapter()
	selection, err := policy.SelectQuotes(context.Background(), filedownload.StartRequest{
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
	if selection.Primary.SellerPubkey != "seller_a" {
		t.Fatalf("expected seller_a selected, got %s", selection.Primary.SellerPubkey)
	}
	if len(selection.Candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(selection.Candidates))
	}
}

func TestDownloadFilePolicyBlocksBySeedPriceOnly(t *testing.T) {
	t.Parallel()

	policy := newDownloadFilePolicyAdapter()
	selection, err := policy.SelectQuotes(context.Background(), filedownload.StartRequest{
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
	if len(selection.Candidates) > 0 {
		t.Fatalf("expected quote to be blocked")
	}
	if selection.Reason != "blocked_by_budget" {
		t.Fatalf("expected blocked_by_budget, got %s", selection.Reason)
	}
}

func TestDownloadFilePolicyIgnoresTotalForBudgetGate(t *testing.T) {
	t.Parallel()

	policy := newDownloadFilePolicyAdapter()
	selection, err := policy.SelectQuotes(context.Background(), filedownload.StartRequest{
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
	if selection.Primary.SellerPubkey != "seller_a" {
		t.Fatalf("expected seller_a selected, got %s", selection.Primary.SellerPubkey)
	}
	if len(selection.Rejected) != 0 {
		t.Fatalf("expected no rejected quotes, got %d", len(selection.Rejected))
	}
}
