package clientapp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	oldproto "github.com/golang/protobuf/proto"
)

type apiRouteCallRequest struct {
	To              string          `json:"to"`
	Route           string          `json:"route"`
	ContentType     string          `json:"content_type"`
	Body            json.RawMessage `json:"body,omitempty"`
	BodyBase64      string          `json:"body_base64,omitempty"`
	PaymentMode     string          `json:"payment_mode,omitempty"`
	PaymentScheme   string          `json:"payment_scheme,omitempty"`
	ServiceQuoteB64 string          `json:"service_quote_base64,omitempty"`
}

func (s *httpAPIServer) handleCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req apiRouteCallRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	body, err := decodeRouteCallBody(req.Body, req.BodyBase64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	serviceQuote, err := decodeOptionalBase64(req.ServiceQuoteB64, "service_quote_base64")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	resp, err := TriggerPeerCall(r.Context(), s.rt, TriggerPeerCallParams{
		To:            req.To,
		Route:         req.Route,
		ContentType:   req.ContentType,
		Body:          body,
		Store:         s.store,
		PaymentMode:   req.PaymentMode,
		PaymentScheme: req.PaymentScheme,
		ServiceQuote:  serviceQuote,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, routeCallHTTPResponse(resp.Ok, resp.Code, resp.Message, resp.ContentType, resp.Body, routeCallPaymentHTTPExtras(resp)))
}

func (s *httpAPIServer) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		To    string `json:"to"`
		Route string `json:"route"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerPeerResolve(r.Context(), s.rt, TriggerPeerResolveParams{To: req.To, Route: req.Route, Store: s.store})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, routeCallHTTPResponse(resp.Ok, resp.Code, resp.Message, resp.ContentType, resp.Body))
}

func decodeRouteCallBody(raw json.RawMessage, bodyBase64 string) ([]byte, error) {
	if strings.TrimSpace(bodyBase64) != "" {
		if len(strings.TrimSpace(string(raw))) != 0 && strings.TrimSpace(string(raw)) != "null" {
			return nil, errors.New("body and body_base64 are mutually exclusive")
		}
		out, err := base64.StdEncoding.DecodeString(strings.TrimSpace(bodyBase64))
		if err != nil {
			return nil, errors.New("invalid body_base64")
		}
		return out, nil
	}
	if len(strings.TrimSpace(string(raw))) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	return append([]byte(nil), raw...), nil
}

func decodeOptionalBase64(raw string, field string) ([]byte, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}
	out, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, errors.New("invalid " + strings.TrimSpace(field))
	}
	return out, nil
}

func routeCallHTTPResponse(ok bool, code, message, contentType string, body []byte, extras ...func(map[string]any)) map[string]any {
	out := map[string]any{
		"ok":      ok,
		"code":    strings.TrimSpace(code),
		"message": strings.TrimSpace(message),
	}
	if strings.TrimSpace(contentType) != "" {
		out["content_type"] = strings.TrimSpace(contentType)
	}
	attachHTTPBodyPayload(out, contentType, body)
	for _, extra := range extras {
		if extra != nil {
			extra(out)
		}
	}
	return out
}

func routeCallPaymentHTTPExtras(resp ncall.CallResp) func(map[string]any) {
	return func(out map[string]any) {
		if len(resp.PaymentSchemes) > 0 {
			out["payment_schemes"] = resp.PaymentSchemes
		}
		if strings.TrimSpace(resp.PaymentReceiptScheme) != "" {
			out["payment_receipt_scheme"] = strings.TrimSpace(resp.PaymentReceiptScheme)
		}
		if len(resp.PaymentReceipt) > 0 {
			out["payment_receipt_base64"] = base64.StdEncoding.EncodeToString(resp.PaymentReceipt)
			if strings.TrimSpace(resp.PaymentReceiptScheme) == ncall.PaymentSchemePool2of2V1 {
				var receipt ncall.FeePool2of2Receipt
				if err := oldproto.Unmarshal(resp.PaymentReceipt, &receipt); err == nil {
					out["payment_receipt"] = receipt
				}
			}
		}
		if len(resp.ServiceQuote) > 0 {
			out["service_quote_base64"] = base64.StdEncoding.EncodeToString(resp.ServiceQuote)
			if json.Valid(resp.ServiceQuote) {
				var quote any
				if err := json.Unmarshal(resp.ServiceQuote, &quote); err == nil {
					out["service_quote"] = quote
				}
			}
		}
		if len(resp.ServiceReceipt) > 0 {
			out["service_receipt_base64"] = base64.StdEncoding.EncodeToString(resp.ServiceReceipt)
		}
	}
}

func attachHTTPBodyPayload(out map[string]any, contentType string, body []byte) {
	if len(body) == 0 {
		return
	}
	out["body_base64"] = base64.StdEncoding.EncodeToString(body)
	if strings.Contains(strings.ToLower(strings.TrimSpace(contentType)), "json") && json.Valid(body) {
		var decoded any
		if err := json.Unmarshal(body, &decoded); err == nil {
			out["body_json"] = decoded
		}
	}
}
