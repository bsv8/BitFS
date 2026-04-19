package indexresolve

import (
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func toModuleAPIError(err error) error {
	if err == nil {
		return nil
	}
	if code := CodeOf(err); code != "" {
		return moduleapi.NewError(code, MessageOf(err))
	}
	if code := moduleapi.CodeOf(err); code != "" {
		return err
	}
	return moduleapi.NewError("INTERNAL_ERROR", err.Error())
}
