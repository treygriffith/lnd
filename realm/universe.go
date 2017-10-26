package realm

import (
	"errors"
	"sync"

	"github.com/roasbeef/btcd/chaincfg"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
)

var (
	ErrIncompatibleNetworks = errors.New("realms must use the same network")
)

type Universe interface {
	RegisterParam(Code, *Params) error
	RegisterControl(Code, *ChainControl)
	SetPrimaryRealm(Code)
	PrimaryRealm() Code

	Realms() []Code
	Hash(Code) (*chainhash.Hash, error)
	Param(Code) (*Params, error)
	Control(Code) (*ChainControl, error)
}

type universe struct {
	mu sync.RWMutex

	net Network

	hashes   map[Code]*chainhash.Hash
	params   map[Code]*Params
	controls map[Code]*ChainControl

	primaryRealm Code
}

type Params struct {
	*chaincfg.Params
	RpcPort string
	Network Network
}

// TODO(conner) uncomment after config parses network as global param
//func NewUniverse(net Network) *universe {
func NewUniverse() *universe {
	return &universe{
		// TODO(conner) uncomment after config parses network as global
		// param
		//net:      net,
		hashes:   make(map[Code]*chainhash.Hash),
		params:   make(map[Code]*Params),
		controls: make(map[Code]*ChainControl),
	}
}

func (u *universe) RegisterParam(c Code, params *Params) error {
	// TODO(conner) uncomment after config parses network as global param
	/*
		if params.Network != u.net {
			return ErrIncompatibleNetworks
		}
	*/

	u.mu.Lock()
	defer u.mu.Unlock()

	u.hashes[c] = params.GenesisHash
	u.params[c] = params

	return nil
}

func (u *universe) RegisterControl(c Code, ctrl *ChainControl) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.controls[c] = ctrl
}

func (u *universe) SetPrimaryRealm(c Code) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.primaryRealm = c
}

func (u *universe) PrimaryRealm() Code {
	u.mu.RLock()
	defer u.mu.RUnlock()

	return u.primaryRealm
}

func (u *universe) Hash(c Code) (*chainhash.Hash, error) {
	u.mu.RLock()
	hash, ok := u.hashes[c]
	u.mu.RUnlock()

	if ok {
		return hash, nil
	}

	return nil, ErrUnregisteredRealm
}

func (u *universe) Param(c Code) (*Params, error) {
	u.mu.RLock()
	param, ok := u.params[c]
	u.mu.RUnlock()

	if ok {
		return param, nil
	}

	return nil, ErrUnregisteredRealm
}

func (u *universe) Control(c Code) (*ChainControl, error) {
	u.mu.RLock()
	ctrl, ok := u.controls[c]
	u.mu.RUnlock()

	if ok {
		return ctrl, nil
	}

	return nil, ErrUnregisteredRealm
}

func (u *universe) Realms() []Code {
	u.mu.RLock()
	defer u.mu.RUnlock()

	realms := make([]Code, 0, len(u.params))
	for code := range u.params {
		realms = append(realms, code)
	}

	return realms
}
