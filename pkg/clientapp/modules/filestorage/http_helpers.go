package filestorage

import (
	"net/http"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/modulekit"
)

func writeModuleOK(w http.ResponseWriter, data any) {
	modulekit.WriteOK(w, data)
}

func writeModuleError(w http.ResponseWriter, status int, code, message string) {
	modulekit.WriteError(w, status, code, message)
}

func httpStatusFromErr(err error) int {
	switch moduleapi.CodeOf(err) {
	case "BAD_REQUEST":
		return http.StatusBadRequest
	case "NOT_FOUND":
		return http.StatusNotFound
	case "MODULE_DISABLED":
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
