package realm

import (
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/routing/chainview"
)

// chainControl couples the three primary interfaces lnd utilizes for a
// particular chain together. A single chainControl instance will exist for all
// the chains lnd is currently active on.
type ChainControl struct {
	ChainIO lnwallet.BlockChainIO

	FeeEstimator lnwallet.FeeEstimator

	Signer lnwallet.Signer

	MsgSigner lnwallet.MessageSigner

	ChainNotifier chainntnfs.ChainNotifier

	ChainView chainview.FilteredChainView

	Wallet *lnwallet.LightningWallet

	RoutingPolicy htlcswitch.ForwardingPolicy
}
