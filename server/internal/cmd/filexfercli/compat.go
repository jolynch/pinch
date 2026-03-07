package filexfercli

import (
	intlimit "github.com/jolynch/pinch/internal/filexfer/limit"
)

func parseByteSize(raw string) (int64, error) {
	return intlimit.ParseByteSize(raw)
}
