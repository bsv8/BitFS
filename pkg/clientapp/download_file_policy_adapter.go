package clientapp

import (
	"context"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFilePolicyAdapter struct{}

func newDownloadFilePolicyAdapter() *downloadFilePolicyAdapter {
	return &downloadFilePolicyAdapter{}
}

func (a *downloadFilePolicyAdapter) SelectQuote(ctx context.Context, req filedownload.StartRequest, quotes []filedownload.QuoteReport) (filedownload.QuoteReport, bool, string, error) {
	if len(quotes) == 0 {
		return filedownload.QuoteReport{}, false, "quote_unavailable", nil
	}

	var best filedownload.QuoteReport
	bestCost := ^uint64(0)
	bestFound := false
	var rejectedReason string

	for _, quote := range quotes {
		if req.MaxSeedPrice > 0 && quote.SeedPriceSat > req.MaxSeedPrice {
			rejectedReason = "blocked_by_budget"
			continue
		}
		if req.MaxChunkPrice > 0 && quote.ChunkPriceSat > req.MaxChunkPrice {
			rejectedReason = "blocked_by_budget"
			continue
		}
		total := quote.SeedPriceSat
		if quote.ChunkCount > 0 {
			if quote.ChunkPriceSat > 0 && uint64(quote.ChunkCount) > (^uint64(0))/quote.ChunkPriceSat {
				rejectedReason = "blocked_by_budget"
				continue
			}
			total += quote.ChunkPriceSat * uint64(quote.ChunkCount)
		}
		if !bestFound || total < bestCost || (total == bestCost && strings.Compare(quote.SellerPubkey, best.SellerPubkey) < 0) {
			best = quote
			bestCost = total
			bestFound = true
			rejectedReason = ""
		}
	}
	if !bestFound {
		if rejectedReason == "" {
			rejectedReason = "blocked_by_budget"
		}
		return filedownload.QuoteReport{}, false, rejectedReason, nil
	}
	return best, true, "", nil
}

var _ filedownload.DownloadPolicy = (*downloadFilePolicyAdapter)(nil)
