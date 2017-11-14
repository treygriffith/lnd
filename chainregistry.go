package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lightninglabs/neutrino"
	"github.com/lightningnetwork/lnd/chainntnfs/btcdnotify"
	"github.com/lightningnetwork/lnd/chainntnfs/neutrinonotify"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/btcwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/realm"
	"github.com/lightningnetwork/lnd/routing/chainview"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/rpcclient"
	bchain "github.com/roasbeef/btcwallet/chain"
	"github.com/roasbeef/btcwallet/walletdb"
)

// defaultBitcoinForwardingPolicy is the default forwarding policy used for
// Bitcoin channels.
var defaultBitcoinForwardingPolicy = htlcswitch.ForwardingPolicy{
	MinHTLC:       0,
	BaseFee:       lnwire.NewMSatFromSatoshis(1),
	FeeRate:       1,
	TimeLockDelta: 144,
	ExchangeRates: map[htlcswitch.NetworkHop]float64{
		htlcswitch.LitecoinHop: 100.0,
	},
	InterRealmTimeScale: map[htlcswitch.NetworkHop]float64{
		htlcswitch.LitecoinHop: 4.0,
	},
}

// defaultLitecoinForwardingPolicy is the default forwarding policy used for
// Litecoin channels.
var defaultLitecoinForwardingPolicy = htlcswitch.ForwardingPolicy{
	MinHTLC:       0,
	BaseFee:       1,
	FeeRate:       1,
	TimeLockDelta: 576,
	ExchangeRates: map[htlcswitch.NetworkHop]float64{
		htlcswitch.BitcoinHop: 0.01,
	},
	InterRealmTimeScale: map[htlcswitch.NetworkHop]float64{
		htlcswitch.BitcoinHop: 0.25,
	},
}

// defaultChannelConstraints is the default set of channel constraints that are
// meant to be used when initially funding a channel.
//
// TODO(roasbeef): have one for both chains
var defaultChannelConstraints = channeldb.ChannelConstraints{
	DustLimit:        lnwallet.DefaultDustLimit(),
	MaxAcceptedHtlcs: lnwallet.MaxHTLCNumber / 2,
}

/*
// chainCode is an enum-like structure for keeping track of the chains
// currently supported within lnd.
type chainCode uint32

const (
	// bitcoinChain is Bitcoin's testnet chain.
	bitcoinChain chainCode = iota

	// litecoinChain is Litecoin's testnet chain.
	litecoinChain
)

// String returns a string representation of the target chainCode.
func (c chainCode) String() string {
	switch c {
	case bitcoinChain:
		return "bitcoin"
	case litecoinChain:
		return "litecoin"
	default:
		return "kekcoin"
	}
}
*/

// newChainControlFromConfig attempts to create a chainControl instance
// according to the parameters in the passed lnd configuration. Currently two
// branches of chainControl instances exist: one backed by a running btcd
// full-node, and the other backed by a running neutrino light client instance.
func newChainControlFromConfig(cfg *config, chanDB *channeldb.DB,
	privateWalletPw, publicWalletPw []byte) (func(), error) {

	realms := universe.Realms()

	var cleanUpFuncs = make([]func() error, 0, len(realms))

	cleanUp := func() {
		for _, cleanUpFunc := range cleanUpFuncs {
			cleanUpFunc()
		}
	}

	for _, realmCode := range realms {
		var chainConf *chainConfig
		switch realmCode {
		case realm.BTC:
			chainConf = cfg.Bitcoin
		case realm.LTC:
			chainConf = cfg.Litecoin
		default:
			return nil, fmt.Errorf("Cannot create chain control "+
				"for unknown chain %v", realmCode)
		}

		ltndLog.Infof("Creating chain control for: %v", realmCode)

		cc := &realm.ChainControl{}

		switch realmCode {
		case realm.BTC:
			cc.RoutingPolicy = defaultBitcoinForwardingPolicy
			cc.FeeEstimator = lnwallet.StaticFeeEstimator{
				FeeRate: 50,
			}
		case realm.LTC:
			cc.RoutingPolicy = defaultLitecoinForwardingPolicy
			cc.FeeEstimator = lnwallet.StaticFeeEstimator{
				FeeRate: 200,
			}
		default:
			return nil, fmt.Errorf("Default routing policy for chain %v "+
				"is unknown", realmCode)
		}

		netParams, err := universe.Param(realmCode)
		if err != nil {
			return nil, fmt.Errorf("No network params registered for chain %v", realmCode)
		}

		walletConfig := &btcwallet.Config{
			PrivatePass:  privateWalletPw,
			PublicPass:   publicWalletPw,
			DataDir:      chainConf.ChainDir,
			NetParams:    netParams.Params,
			FeeEstimator: cc.FeeEstimator,
		}

		// If spv mode is active, then we'll be using a distinct set of
		// chainControl interfaces that interface directly with the p2p network
		// of the selected chain.
		if cfg.NeutrinoMode.Active {
			// First we'll open the database file for neutrino, creating
			// the database if needed.
			dbName := filepath.Join(cfg.DataDir, "neutrino.db")
			nodeDatabase, err := walletdb.Create("bdb", dbName)
			if err != nil {
				return cleanUp, err
			}

			// With the database open, we can now create an instance of the
			// neutrino light client. We pass in relevant configuration
			// parameters required.
			config := neutrino.Config{
				DataDir:      cfg.DataDir,
				Database:     nodeDatabase,
				ChainParams:  *netParams.Params,
				AddPeers:     cfg.NeutrinoMode.AddPeers,
				ConnectPeers: cfg.NeutrinoMode.ConnectPeers,
			}
			neutrino.WaitForMoreCFHeaders = time.Second * 1
			neutrino.MaxPeers = 8
			neutrino.BanDuration = 5 * time.Second
			svc, err := neutrino.NewChainService(config)
			if err != nil {
				return cleanUp, fmt.Errorf("unable to create neutrino: %v", err)
			}
			svc.Start()

			// Next we'll create the instances of the ChainNotifier and
			// FilteredChainView interface which is backed by the neutrino
			// light client.
			cc.ChainNotifier, err = neutrinonotify.New(svc)
			if err != nil {
				return cleanUp, err
			}
			cc.ChainView, err = chainview.NewCfFilteredChainView(svc)
			if err != nil {
				return cleanUp, err
			}

			// Finally, we'll set the chain source for btcwallet, and
			// create our clean up function which simply closes the
			// database.
			walletConfig.ChainSource = bchain.NewNeutrinoClient(svc)

			cleanUpFuncs = append(cleanUpFuncs, nodeDatabase.Close)
		} else {
			// Otherwise, we'll be speaking directly via RPC to a node.
			//
			// So first we'll load btcd/ltcd's TLS cert for the RPC
			// connection. If a raw cert was specified in the config, then
			// we'll set that directly. Otherwise, we attempt to read the
			// cert from the path specified in the config.
			var rpcCert []byte
			if chainConf.RawRPCCert != "" {
				rpcCert, err = hex.DecodeString(chainConf.RawRPCCert)
				if err != nil {
					return cleanUp, err
				}
			} else {
				certFile, err := os.Open(chainConf.RPCCert)
				if err != nil {
					return cleanUp, err
				}
				rpcCert, err = ioutil.ReadAll(certFile)
				if err != nil {
					return cleanUp, err
				}
				if err := certFile.Close(); err != nil {
					return cleanUp, err
				}
			}

			// If the specified host for the btcd/ltcd RPC server already
			// has a port specified, then we use that directly. Otherwise,
			// we assume the default port according to the selected chain
			// parameters.
			var btcdHost string
			if strings.Contains(chainConf.RPCHost, ":") {
				btcdHost = chainConf.RPCHost
			} else {
				btcdHost = fmt.Sprintf("%v:%v", chainConf.RPCHost, netParams.RpcPort)
			}

			btcdUser := chainConf.RPCUser
			btcdPass := chainConf.RPCPass
			rpcConfig := &rpcclient.ConnConfig{
				Host:                 btcdHost,
				Endpoint:             "ws",
				User:                 btcdUser,
				Pass:                 btcdPass,
				Certificates:         rpcCert,
				DisableTLS:           false,
				DisableConnectOnNew:  true,
				DisableAutoReconnect: false,
			}
			cc.ChainNotifier, err = btcdnotify.New(rpcConfig)
			if err != nil {
				return cleanUp, err
			}

			// Finally, we'll create an instance of the default
			// chain view to be used within the routing layer.
			cc.ChainView, err = chainview.NewBtcdFilteredChainView(*rpcConfig)
			if err != nil {
				srvrLog.Errorf("unable to create chain view: "+
					"%v", err)
				return cleanUp, err
			}

			// Create a special websockets rpc client for btcd which
			// will be used by the wallet for notifications, calls,
			// etc.
			chainRPC, err := bchain.NewRPCClient(netParams.Params,
				btcdHost, btcdUser, btcdPass, rpcCert, false, 1)
			if err != nil {
				return cleanUp, err
			}

			walletConfig.ChainSource = chainRPC
		}

		wc, err := btcwallet.New(*walletConfig)
		if err != nil {
			srvrLog.Errorf("unable to create wallet controller: %v", err)
			return cleanUp, err
		}

		cc.MsgSigner = wc
		cc.Signer = wc
		cc.ChainIO = wc

		// Create, and start the lnwallet, which handles the core
		// payment channel logic, and exposes control via proxy state
		// machines.
		walletCfg := lnwallet.Config{
			Database:           chanDB,
			Notifier:           cc.ChainNotifier,
			WalletController:   wc,
			Signer:             cc.Signer,
			FeeEstimator:       cc.FeeEstimator,
			ChainIO:            cc.ChainIO,
			DefaultConstraints: defaultChannelConstraints,
			NetParams:          *netParams.Params,
		}
		wallet, err := lnwallet.NewLightningWallet(walletCfg)
		if err != nil {
			srvrLog.Errorf("unable to create wallet: %v", err)
			return nil, err
		}
		cc.Wallet = wallet

		universe.RegisterControl(realmCode, cc)

		srvrLog.Infof("Starting %v LightningWallet", realmCode)

		if err := wallet.Startup(); err != nil {
			srvrLog.Errorf("unable to start wallet: %v", err)
			return nil, err
		}

		ltndLog.Infof("Opened %v LightningWallet", realmCode)
	}

	return cleanUp, nil
}

var (
	// bitcoinGenesis is the genesis hash of Bitcoin's testnet chain.
	bitcoinGenesis = chainhash.Hash([chainhash.HashSize]byte{
		0x6f, 0xe2, 0x8c, 0x0a, 0xb6, 0xf1, 0xb3, 0x72,
		0xc1, 0xa6, 0xa2, 0x46, 0xae, 0x63, 0xf7, 0x4f,
		0x93, 0x1e, 0x83, 0x65, 0xe1, 0x5a, 0x08, 0x9c,
		0x68, 0xd6, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00,
	})

	// litecoinGenesis is the genesis hash of Litecoin's testnet4 chain.
	litecoinGenesis = chainhash.Hash([chainhash.HashSize]byte{
		0xa0, 0x29, 0x3e, 0x4e, 0xeb, 0x3d, 0xa6, 0xe6,
		0xf5, 0x6f, 0x81, 0xed, 0x59, 0x5f, 0x57, 0x88,
		0x0d, 0x1a, 0x21, 0x56, 0x9e, 0x13, 0xee, 0xfd,
		0xd9, 0x51, 0x28, 0x4b, 0x5a, 0x62, 0x66, 0x49,
	})

	// chainMap is a simple index that maps a chain's genesis hash to the
	// chainCode enum for that chain.
	chainMap = map[chainhash.Hash]realm.Code{
		bitcoinGenesis:  realm.BTC,
		litecoinGenesis: realm.LTC,
	}

	// reverseChainMap is the inverse of the chainMap above: it maps the
	// chain enum for a chain to its genesis hash.
	reverseChainMap = map[realm.Code]chainhash.Hash{
		realm.BTC: bitcoinGenesis,
		realm.LTC: litecoinGenesis,
	}

	// chainDNSSeeds is a map of a chain's hash to the set of DNS seeds
	// that will be use to bootstrap peers upon first startup.
	//
	// The first item in the array is the primary host we'll use to attempt
	// the SRV lookup we require. If we're unable to receive a response
	// over UDP, then we'll fall back to manual TCP resolution. The second
	// item in the array is a special A record that we'll query in order to
	// receive the IP address of the current authoritative DNS server for
	// the network seed.
	//
	// TODO(roasbeef): extend and collapse these and chainparams.go into
	// struct like chaincfg.Params
	chainDNSSeeds = map[chainhash.Hash][][2]string{
		bitcoinGenesis: {
			{
				"nodes.lightning.directory",
				"soa.nodes.lightning.directory",
			},
		},
	}
)

/*
// chainRegistry keeps track of the current chains
type chainRegistry struct {
	sync.RWMutex

	activeChains map[chainCode]*chainControl
	netParams    map[chainCode]*bitcoinNetParams

	primaryChain chainCode
}

// newChainRegistry creates a new chainRegistry.
func newChainRegistry() *chainRegistry {
	return &chainRegistry{
		activeChains: make(map[chainCode]*chainControl),
		netParams:    make(map[chainCode]*bitcoinNetParams),
	}
}

// RegisterChain assigns an active chainControl instance to a target chain
// identified by its chainCode.
func (c *chainRegistry) RegisterChain(newChain chainCode, cc *chainControl) {
	c.Lock()
	defer c.Unlock()

	c.activeChains[newChain] = cc
}

func (c *chainRegistry) RegisterParams(newChain chainCode, p *bitcoinNetParams) {
	c.Lock()
	defer c.Unlock()

	c.netParams[newChain] = p
}

// LookupChain attempts to lookup an active chainControl instance for the
// target chain.
func (c *chainRegistry) LookupChain(targetChain chainCode) (*chainControl, bool) {
	c.RLock()
	cc, ok := c.activeChains[targetChain]
	c.RUnlock()

	return cc, ok
}

func (c *chainRegistry) LookupParams(targetChain chainCode) (*bitcoinNetParams, bool) {
	c.RLock()
	params, ok := c.netParams[targetChain]
	c.RUnlock()

	return params, ok
}

// LookupChainByHash attempts to look up an active chainControl which
// corresponds to the passed genesis hash.
func (c *chainRegistry) LookupChainByHash(chainHash chainhash.Hash) (*chainControl, bool) {
	c.RLock()
	defer c.RUnlock()

	targetChain, ok := chainMap[chainHash]
	if !ok {
		return nil, ok
	}

	cc, ok := c.activeChains[targetChain]
	return cc, ok
}

// RegisterPrimaryChain sets a target chain as the "home chain" for lnd.
func (c *chainRegistry) RegisterPrimaryChain(cc chainCode) {
	c.Lock()
	defer c.Unlock()

	c.primaryChain = cc
}

// PrimaryChain returns the primary chain for this running lnd instance. The
// primary chain is considered the "home base" while the other registered
// chains are treated as secondary chains.
func (c *chainRegistry) PrimaryChain() chainCode {
	c.RLock()
	defer c.RUnlock()

	return c.primaryChain
}

// ActiveChains returns the total number of active chains.
func (c *chainRegistry) ActiveChains() []chainCode {
	c.RLock()
	defer c.RUnlock()

	chains := make([]chainCode, 0, len(c.netParams))
	for activeChain := range c.netParams {
		chains = append(chains, activeChain)
	}

	return chains
}

// NumActiveChains returns the total number of active chains.
func (c *chainRegistry) NumActiveChains() uint32 {
	c.RLock()
	defer c.RUnlock()

	return uint32(len(c.activeChains))
}
*/
