package sketchutil

func ToFloat(v interface{}) (float64, bool) {
	switch value := v.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case int64:
		return float64(value), true
	case int32:
		return float64(value), true
	case int16:
		return float64(value), true
	case int8:
		return float64(value), true
	case int:
		return float64(value), true
	case uint64:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint:
		return float64(value), true
	default:
		return 0, false
	}
}
