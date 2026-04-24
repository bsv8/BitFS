package sqliteactor

import (
	"database/sql/driver"
	"fmt"
	"math"
	"time"
)

// normalizeDriverValue 只做可入库参数类型收口。
// 设计说明：
// - 这里只负责把 Go 值整理成 sqlite driver 能吃的值；
// - 不碰业务语义，不做展示，不做日志加工；
// - 一旦遇到超范围或不支持的值，直接报英文错误，避免把脏值送进 driver。
func normalizeDriverValue(v any) (driver.Value, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return x, nil
	case string:
		return x, nil
	case bool:
		return x, nil
	case time.Time:
		return x, nil
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return nil, fmt.Errorf("sqlite parameter uint overflows int64: %d", x)
		}
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return nil, fmt.Errorf("sqlite parameter uint64 overflows int64: %d", x)
		}
		return int64(x), nil
	default:
		return nil, fmt.Errorf("unsupported sqlite parameter type %T", v)
	}
}

// normalizeNamedValue 直接把 NamedValue 收口到 driver.Value。
func normalizeNamedValue(nv *driver.NamedValue) error {
	if nv == nil {
		return nil
	}
	v, err := normalizeDriverValue(nv.Value)
	if err != nil {
		return err
	}
	nv.Value = v
	return nil
}

// normalizeNamedValues 供 fallback 路径使用，避免漏网。
func normalizeNamedValues(args []driver.NamedValue) ([]driver.Value, error) {
	if len(args) == 0 {
		return nil, nil
	}
	out := make([]driver.Value, 0, len(args))
	for _, arg := range args {
		v, err := normalizeDriverValue(arg.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
