package clientapp

import (
	"strings"
	"testing"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
)

func TestValidateDemandPublishPaidResp(t *testing.T) {
	t.Run("success false", func(t *testing.T) {
		err := validateDemandPublishPaidResp(dual2of2.DemandPublishPaidResp{
			Success: false,
			Status:  "active",
			Error:   "charge rejected",
		})
		if err == nil {
			t.Fatalf("expected error when success=false")
		}
		if !strings.Contains(err.Error(), "gateway demand publish rejected") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success true with empty demand id", func(t *testing.T) {
		err := validateDemandPublishPaidResp(dual2of2.DemandPublishPaidResp{
			Success:  true,
			Status:   "ok",
			DemandID: "",
		})
		if err == nil {
			t.Fatalf("expected error when demand_id is empty")
		}
		if !strings.Contains(err.Error(), "empty demand_id") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success true with demand id", func(t *testing.T) {
		err := validateDemandPublishPaidResp(dual2of2.DemandPublishPaidResp{
			Success:  true,
			Status:   "ok",
			DemandID: "dmd_x1",
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})
}

func TestValidateLiveDemandPublishPaidResp(t *testing.T) {
	t.Run("success false", func(t *testing.T) {
		err := validateLiveDemandPublishPaidResp(dual2of2.LiveDemandPublishPaidResp{
			Success: false,
			Status:  "active",
			Error:   "charge rejected",
		})
		if err == nil {
			t.Fatalf("expected error when success=false")
		}
		if !strings.Contains(err.Error(), "gateway live demand publish rejected") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success true with empty demand id", func(t *testing.T) {
		err := validateLiveDemandPublishPaidResp(dual2of2.LiveDemandPublishPaidResp{
			Success:  true,
			Status:   "ok",
			DemandID: "",
		})
		if err == nil {
			t.Fatalf("expected error when demand_id is empty")
		}
		if !strings.Contains(err.Error(), "empty demand_id") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
