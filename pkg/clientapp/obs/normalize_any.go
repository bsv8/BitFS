package obs

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

const maxNormalizeDepth = 32

// NormalizeAny 把任意值转成适合 JSON 输出的结构。
// 设计说明：
// - 不做前后缀截断，不做 `...` 压缩；
// - `[]byte` 固定转完整 hex；
// - 保留循环引用保护，避免观测链路把进程拖死；
// - 对实现了 JSON 编码接口的类型，优先按 JSON 语义展开。
func NormalizeAny(v any) any {
	seen := map[uintptr]bool{}
	return normalizeAnyValue(reflect.ValueOf(v), 0, seen)
}

func normalizeAnyValue(rv reflect.Value, depth int, seen map[uintptr]bool) any {
	if !rv.IsValid() {
		return nil
	}
	if depth >= maxNormalizeDepth {
		return fallbackNormalize(rv)
	}

	for rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	if normalized, ok := normalizeJSONMarshaler(rv); ok {
		return normalized
	}

	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		ptr := rv.Pointer()
		if ptr != 0 {
			if seen[ptr] {
				return "<cycle>"
			}
			seen[ptr] = true
			defer delete(seen, ptr)
		}
		return normalizeAnyValue(rv.Elem(), depth+1, seen)
	}

	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return rv.Uint()
	case reflect.Float32, reflect.Float64:
		return rv.Float()
	case reflect.String:
		return rv.String()
	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return hex.EncodeToString(rv.Bytes())
		}
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, normalizeAnyValue(rv.Index(i), depth+1, seen))
		}
		return out
	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(b), rv)
			return hex.EncodeToString(b)
		}
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, normalizeAnyValue(rv.Index(i), depth+1, seen))
		}
		return out
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key()
			key := ""
			if k.IsValid() && k.Kind() == reflect.String {
				key = k.String()
			} else {
				key = fmt.Sprint(k.Interface())
			}
			out[key] = normalizeAnyValue(iter.Value(), depth+1, seen)
		}
		return out
	case reflect.Struct:
		out := make(map[string]any, rv.NumField())
		rt := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}
			name := f.Name
			if tag, ok := f.Tag.Lookup("json"); ok {
				tagName := strings.TrimSpace(strings.Split(tag, ",")[0])
				if tagName == "-" {
					continue
				}
				if tagName != "" {
					name = tagName
				}
			}
			out[name] = normalizeAnyValue(rv.Field(i), depth+1, seen)
		}
		return out
	default:
		return fallbackNormalize(rv)
	}
}

func normalizeJSONMarshaler(rv reflect.Value) (any, bool) {
	if !rv.IsValid() {
		return nil, false
	}
	if rv.CanInterface() {
		if marshaler, ok := rv.Interface().(json.Marshaler); ok {
			if normalized, ok := marshalJSONToAny(marshaler); ok {
				return normalized, true
			}
		}
	}
	if rv.CanAddr() {
		if marshaler, ok := rv.Addr().Interface().(json.Marshaler); ok {
			if normalized, ok := marshalJSONToAny(marshaler); ok {
				return normalized, true
			}
		}
	}
	return nil, false
}

func marshalJSONToAny(m json.Marshaler) (any, bool) {
	if m == nil {
		return nil, false
	}
	raw, err := m.MarshalJSON()
	if err != nil {
		return nil, false
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw), true
	}
	return out, true
}

func fallbackNormalize(rv reflect.Value) any {
	if rv.IsValid() && rv.CanInterface() {
		return fmt.Sprint(rv.Interface())
	}
	return fmt.Sprint(rv)
}
