package utils

import (
	"strconv"
	"time"
)

func KillAfter(timeout time.Duration) (string, string) {
	val := int64(timeout / time.Second)
	if val < 1 {
		val = 1
	}
	return strconv.FormatInt(val, 10), strconv.FormatInt(val+10, 10)
}
