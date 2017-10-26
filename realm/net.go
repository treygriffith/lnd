package realm

import (
	"errors"
	"strings"
)

var (
	ErrUnknownNetwork = errors.New("unknown network")
)

type Network byte

const (
	MainNet Network = iota
	TestNet
	SimNet
	RegNet

	InvalidNet Network = 0xFF
)

func NetworkFromStr(str string) (Network, error) {
	str = strings.ToLower(str)
	switch str {
	case "mainnet":
		return MainNet, nil
	case "testnet":
		return TestNet, nil
	case "simnet":
		return SimNet, nil
	case "regnet":
		return RegNet, nil
	default:
		return InvalidNet, ErrUnknownNetwork
	}
}

func (n Network) Name() string {
	switch n {
	case MainNet:
		return "mainnet"
	case TestNet:
		return "testnet"
	case SimNet:
		return "simnet"
	case RegNet:
		return "regnet"
	default:
		return "keknet"
	}
}

func (n Network) String() string {
	return n.Name()
}
