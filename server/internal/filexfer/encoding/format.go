package encoding

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var humanByteUnits = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
var humanRateUnits = []string{"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s", "EiB/s"}

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
	return formatHumanBinary(float64(v), humanByteUnits, 0)
}

func HumanBytesFixedWidth(v int64, width int) string {
	return formatHumanBinary(float64(v), humanByteUnits, width)
}

func HumanRate(bps float64) string {
	return formatHumanBinary(bps, humanRateUnits, 0)
}

func HumanRateFixedWidth(bps float64, width int) string {
	return formatHumanBinary(bps, humanRateUnits, width)
}

func formatHumanBinary(value float64, units []string, width int) string {
	scaled, unit, decimals := scaleHumanBinary(value, units)
	formatted := formatHumanValue(scaled, unit, decimals)
	if width > 0 {
		return fmt.Sprintf("%*s", width, formatted)
	}
	return formatted
}

func HumanCount(n uint64, width int) string {
	if width <= 0 {
		return ""
	}

	raw := strconv.FormatUint(n, 10)
	if len(raw) <= width {
		return fmt.Sprintf("%*s", width, raw)
	}

	units := []string{"K", "M", "B", "T", "P"}

	v := float64(n)
	u := -1
	for v >= 1000 && u < len(units)-1 {
		v /= 1000
		u++
	}

	if u >= 0 {
		suffix := units[u]
		for decimals := 1; decimals >= 0; decimals-- {
			s := fmt.Sprintf("%.*f%s", decimals, v, suffix)
			if len(s) <= width {
				return fmt.Sprintf("%*s", width, s)
			}
		}

		s := fmt.Sprintf("%.0f%s", v, suffix)
		if len(s) <= width {
			return fmt.Sprintf("%*s", width, s)
		}
	}

	if len(raw) > width {
		return raw[len(raw)-width:]
	}
	return fmt.Sprintf("%*s", width, raw)
}

func scaleHumanBinary(value float64, units []string) (scaled float64, unit string, decimals int) {
	if len(units) == 0 {
		return value, "", 0
	}
	absValue := value
	if absValue < 0 {
		absValue = -absValue
	}
	unitIdx := 0
	for absValue >= 1024 && unitIdx < len(units)-1 {
		absValue /= 1024
		value /= 1024
		unitIdx++
	}
	if unitIdx == 0 {
		return value, units[unitIdx], 0
	}
	return value, units[unitIdx], 2
}

func formatHumanValue(value float64, unit string, decimals int) string {
	if decimals == 0 {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.2f %s", value, unit)
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
