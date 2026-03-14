package encoding

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func ParseByteSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("size is required")
	}
	value, unit, err := parseHumanValueAndUnit(raw)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, errors.New("size must be >= 0")
	}
	if unit == "" {
		return floatToInt64(value)
	}
	mult, ok := byteUnitMultiplier(strings.ToLower(strings.TrimSpace(unit)))
	if !ok {
		return 0, fmt.Errorf("unsupported size unit: %s", unit)
	}
	return floatToInt64(value * mult)
}

func HumanBytes(v int64) string {
	if v <= 0 {
		return "0 B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := float64(v)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f %s", value, units[unit])
	}
	return fmt.Sprintf("%.2f %s", value, units[unit])
}

func HumanRate(bps float64) string {
	if bps <= 0 {
		return "0 B/s"
	}
	units := []string{"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"}
	unit := 0
	for bps >= 1024 && unit < len(units)-1 {
		bps /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f %s", bps, units[unit])
	}
	return fmt.Sprintf("%.2f %s", bps, units[unit])
}

func parseHumanValueAndUnit(raw string) (float64, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, "", errors.New("value is required")
	}
	cut := 0
	for cut < len(raw) && (raw[cut] == '.' || (raw[cut] >= '0' && raw[cut] <= '9')) {
		cut++
	}
	if cut == 0 {
		return 0, "", errors.New("numeric value is required")
	}
	numPart := strings.TrimSpace(raw[:cut])
	unitPart := strings.TrimSpace(raw[cut:])
	v, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, "", err
	}
	return v, unitPart, nil
}

func byteUnitMultiplier(unit string) (float64, bool) {
	switch strings.TrimSpace(strings.ToLower(unit)) {
	case "", "b", "byte", "bytes":
		return 1, true
	case "kb":
		return 1000, true
	case "mb":
		return 1000 * 1000, true
	case "gb":
		return 1000 * 1000 * 1000, true
	case "tb":
		return 1000 * 1000 * 1000 * 1000, true
	case "kib":
		return 1024, true
	case "mib":
		return 1024 * 1024, true
	case "gib":
		return 1024 * 1024 * 1024, true
	case "tib":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func floatToInt64(v float64) (int64, error) {
	if v < 0 {
		return 0, errors.New("value must be >= 0")
	}
	if v > float64(^uint64(0)>>1) {
		return 0, errors.New("value overflows int64")
	}
	return int64(v + 0.5), nil
}
