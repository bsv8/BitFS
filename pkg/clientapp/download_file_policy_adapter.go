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

func (a *downloadFilePolicyAdapter) SelectQuotes(ctx context.Context, req filedownload.StartRequest, quotes []filedownload.QuoteReport) (filedownload.QuoteSelection, error) {
	if len(quotes) == 0 {
		return filedownload.QuoteSelection{Reason: "quote_unavailable"}, nil
	}
	out := filedownload.QuoteSelection{
		Candidates: make([]filedownload.QuoteReport, 0, len(quotes)),
		Rejected:   make([]filedownload.QuoteReport, 0),
	}
	var best filedownload.QuoteReport
	bestCost := ^uint64(0)
	bestFound := false

	for _, quote := range quotes {
		rejected := false
		reason := ""
		if req.MaxSeedPrice > 0 && quote.SeedPriceSat > req.MaxSeedPrice {
			rejected = true
			reason = "blocked_by_budget"
		}
		if !rejected && req.MaxChunkPrice > 0 && quote.ChunkPriceSat > req.MaxChunkPrice {
			rejected = true
			reason = "blocked_by_budget"
		}
		if rejected {
			quote.RejectReason = reason
			out.Rejected = append(out.Rejected, quote)
			continue
		}
		out.Candidates = append(out.Candidates, quote)
		total := quote.SeedPriceSat
		if quote.ChunkCount > 0 {
			if quote.ChunkPriceSat > 0 && uint64(quote.ChunkCount) > (^uint64(0))/quote.ChunkPriceSat {
				quote.RejectReason = "blocked_by_budget"
				out.Rejected = append(out.Rejected, quote)
				continue
			}
			total += quote.ChunkPriceSat * uint64(quote.ChunkCount)
		}
		if !bestFound || total < bestCost || (total == bestCost && strings.Compare(quote.SellerPubkey, best.SellerPubkey) < 0) {
			best = quote
			bestCost = total
			bestFound = true
		}
	}
	if !bestFound {
		return filedownload.QuoteSelection{
			Rejected: out.Rejected,
			Reason:   "blocked_by_budget",
		}, nil
	}
	out.Primary = best
	if len(out.Candidates) == 0 {
		out.Candidates = []filedownload.QuoteReport{best}
	}
	return out, nil
}

var _ filedownload.DownloadPolicy = (*downloadFilePolicyAdapter)(nil)
