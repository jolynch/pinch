package ftcp

import (
	"fmt"
	"strings"
)

type Verb uint8

const (
	VerbUnknown Verb = iota
	VerbAUTH
	VerbTXFER
	VerbSEND
	VerbACK
	VerbCXSUM
	VerbSTATUS
)

func ParseVerb(token string) (Verb, error) {
	switch strings.ToUpper(strings.TrimSpace(token)) {
	case "AUTH":
		return VerbAUTH, nil
	case "TXFER":
		return VerbTXFER, nil
	case "SEND":
		return VerbSEND, nil
	case "ACK":
		return VerbACK, nil
	case "CXSUM":
		return VerbCXSUM, nil
	case "STATUS":
		return VerbSTATUS, nil
	default:
		return VerbUnknown, fmt.Errorf("unknown verb: %s", token)
	}
}
