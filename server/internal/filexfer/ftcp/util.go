package ftcp

import "fmt"

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func humanRate(bps float64) string {
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
