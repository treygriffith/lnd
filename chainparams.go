package main

import (
	"github.com/lightningnetwork/lnd/realm"
	litecoinCfg "github.com/ltcsuite/ltcd/chaincfg"
	"github.com/roasbeef/btcd/chaincfg"
	bitcoinCfg "github.com/roasbeef/btcd/chaincfg"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
)

// bitcoinTestNetParams contains parameters specific to the 3rd version of the
// test network.
var bitcoinTestNetParams = realm.Params{
	Params:  &bitcoinCfg.TestNet3Params,
	RpcPort: "18334",
	Network: realm.TestNet,
}

// liteTestNetParams contains parameters specific to the 4th version of the
// test network.
var litecoinTestNetParams = realm.Params{
	Params:  ltcToBtcParams(&litecoinCfg.TestNet4Params),
	RpcPort: "19334",
	Network: realm.TestNet,
}

// bitcoinSimNetParams contains parameters specific to the simulation test
// network.
var bitcoinSimNetParams = realm.Params{
	Params:  &bitcoinCfg.SimNetParams,
	RpcPort: "18556",
	Network: realm.SimNet,
}

// litecoinSimNetParams contains parameters specific to the simulation test
// network.
var litecoinSimNetParams = realm.Params{
	Params:  ltcToBtcParams(&litecoinCfg.SimNetParams),
	RpcPort: "19556",
	Network: realm.SimNet,
}

// regTestNetParams contains parameters specific to a local regtest network.
var regTestNetParams = realm.Params{
	Params:  &bitcoinCfg.RegressionNetParams,
	RpcPort: "18334",
	Network: realm.RegNet,
}

// applyLitecoinParams applies the relevant chain configuration parameters that
// differ for litecoin to the chain parameters typed for btcsuite derivation.
// This function is used in place of using something like interface{} to
// abstract over _which_ chain (or fork) the parameters are for.
func ltcToBtcParams(params *litecoinCfg.Params) *bitcoinCfg.Params {
	p := &bitcoinCfg.Params{}

	p.Name = params.Name
	p.Net = wire.BitcoinNet(params.Net)
	p.DefaultPort = params.DefaultPort
	p.CoinbaseMaturity = params.CoinbaseMaturity

	p.GenesisHash = &chainhash.Hash{}
	copy(p.GenesisHash[:], params.GenesisHash[:])

	// Address encoding magics
	p.PubKeyHashAddrID = params.PubKeyHashAddrID
	p.ScriptHashAddrID = params.ScriptHashAddrID
	p.PrivateKeyID = params.PrivateKeyID
	p.WitnessPubKeyHashAddrID = params.WitnessPubKeyHashAddrID
	p.WitnessScriptHashAddrID = params.WitnessScriptHashAddrID
	p.Bech32HRPSegwit = params.Bech32HRPSegwit

	copy(p.HDPrivateKeyID[:], params.HDPrivateKeyID[:])
	copy(p.HDPublicKeyID[:], params.HDPublicKeyID[:])

	p.HDCoinType = params.HDCoinType

	checkPoints := make([]chaincfg.Checkpoint, len(params.Checkpoints))
	for i := 0; i < len(params.Checkpoints); i++ {
		var chainHash chainhash.Hash
		copy(chainHash[:], params.Checkpoints[i].Hash[:])

		checkPoints[i] = chaincfg.Checkpoint{
			Height: params.Checkpoints[i].Height,
			Hash:   &chainHash,
		}
	}
	p.Checkpoints = checkPoints

	return p
}
