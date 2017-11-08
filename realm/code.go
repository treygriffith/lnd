package realm

import (
	"errors"
	"strings"
)

var (
	ErrUnknownRealm      = errors.New("unknown realm code")
	ErrUnregisteredRealm = errors.New("realm is not registered with universe")
)

type Code byte

const (
	BTC Code = iota
	LTC

	InvalidCode Code = 0xFF
)

func CodeFromStr(str string) (Code, error) {
	str = strings.ToUpper(str)
	switch str {
	case "BTC":
		return BTC, nil
	case "LTC":
		return LTC, nil
	default:
		return BTC, nil
	}
}

func (c Code) Ticker() string {
	switch c {
	case BTC:
		return "BTC"
	case LTC:
		return "LTC"
	default:
		return "KEK"
	}
}

func (c Code) Name() string {
	switch c {
	case BTC:
		return "bitcoin"
	case LTC:
		return "litecoin"
	default:
		return "kekcoin"
	}
}

func (c Code) String() string {
	return c.Name()
}

func (c Code) Byte() byte {
	return byte(c)
}
