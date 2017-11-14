package realm

import (
	"errors"
	"sync"

	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/routing/chainview"
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

	RealmChainMap() map[byte]chainhash.Hash
	ChainRealmMap() map[chainhash.Hash]byte
	ChainMap() map[chainhash.Hash]lnwallet.BlockChainIO
	ChainViewMap() map[chainhash.Hash]chainview.FilteredChainView
	NotifierMap() map[chainhash.Hash]chainntnfs.ChainNotifier
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

func (u *universe) ChainRealmMap() map[chainhash.Hash]byte {
	u.mu.RLock()
	defer u.mu.RUnlock()

	chainRealmMap := make(map[chainhash.Hash]byte)
	for code, hash := range u.hashes {
		chainRealmMap[*hash] = code.Byte()
	}

	return chainRealmMap
}

func (u *universe) RealmChainMap() map[byte]chainhash.Hash {
	u.mu.RLock()
	defer u.mu.RUnlock()

	realmChainMap := make(map[byte]chainhash.Hash)
	for code, hash := range u.hashes {
		realmChainMap[code.Byte()] = *hash
	}

	return realmChainMap
}

func (u *universe) ChainMap() map[chainhash.Hash]lnwallet.BlockChainIO {
	u.mu.RLock()
	defer u.mu.RUnlock()

	chainMap := make(map[chainhash.Hash]lnwallet.BlockChainIO)
	for code, hash := range u.hashes {
		if cc, ok := u.controls[code]; ok {
			chainMap[*hash] = cc.ChainIO
		}
	}

	return chainMap
}

func (u *universe) ChainViewMap() map[chainhash.Hash]chainview.FilteredChainView {
	u.mu.RLock()
	defer u.mu.RUnlock()

	chainViewMap := make(map[chainhash.Hash]chainview.FilteredChainView)
	for code, hash := range u.hashes {
		if cc, ok := u.controls[code]; ok {
			chainViewMap[*hash] = cc.ChainView
		}
	}

	return chainViewMap
}

func (u *universe) NotifierMap() map[chainhash.Hash]chainntnfs.ChainNotifier {
	u.mu.RLock()
	defer u.mu.RUnlock()

	notifierMap := make(map[chainhash.Hash]chainntnfs.ChainNotifier)
	for code, hash := range u.hashes {
		if cc, ok := u.controls[code]; ok {
			notifierMap[*hash] = cc.ChainNotifier
		}
	}

	return notifierMap
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
