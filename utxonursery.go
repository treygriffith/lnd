package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/roasbeef/btcd/blockchain"
	"github.com/roasbeef/btcd/txscript"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcutil"
)

//                     OUTPUT STATE TRANSITIONS IN UTXO NURSERY
//
//       ┌────────────────┐            ┌──────────────┐
//       │ Commit Outputs │            │ HTLC Outputs │
//       └────────────────┘            └──────────────┘
//                │                            │
//                │                            │
//                │                            │
//    ┌─ ─ ─ ─ ─ ─┼─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
//    │           │                            │                               │
//    │           │                            │                               │
//    │           │                            │             UTXO NURSERY      │
//    │           │                            V                               │
//    │           │                        ┌──────┐                            │
//    │           │                        │ CRIB │                            │
//    │           │                        └──────┘                            │
//    │           │                            │                               │
//    │           │                            │                               │
//    │           V                            |                               │
//    │       ┌──────┐                         V    Wait CLTV                  │
//    │       │ PSCL │                        [ ]       +                      │
//    │       └──────┘                         |   Publish Txn                 │
//    │           │                            │                               │
//    │           │                            │                               │
//    │           V ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐    V ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐         │
//    │          ( )   waitForPromotion       ( )  waitForEnrollment           │
//    │           ' └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘    | └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         │
//    │           │                            │                               │
//    │           │                            │                               │
//    │           │                            V                               │
//    │           │                        ┌──────┐                            │
//    │           └— — — — — — — — — — — —>│ KNDR │                            │
//    │                                    └──────┘                            │
//    │                                        │                               │
//    │                                        │                               │
//    │                                        |                               │
//    │                                        V     Wait CSV                  │
//    │                                       [ ]       +                      │
//    │                                        |   Publish Txn                 │
//    │                                        │                               │
//    │                                        │                               │
//    │                                        V ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐         │
//    │                                       ( )  waitForGraduation           │
//    │                                        | └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         │
//    │                                        │                               │
//    │                                        │                               │
//    │                                        V                               │
//    │                                     ┌──────┐                           │
//    │                                     │ GRAD │                           │
//    │                                     └──────┘                           │
//    │                                        │                               │
//    │                                        │                               │
//    │                                        │                               │
//    │                                        │                               │
//    └─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
//                                             │
//                                             │
//                                             │
//                                             V
//                                    ┌────────────────┐
//                                    │ Wallet Outputs │
//                                    └────────────────┘

var byteOrder = binary.BigEndian

var (
	// ErrContractNotFound is returned when the nursery is unable to
	// retreive information about a queried contract.
	ErrContractNotFound = fmt.Errorf("unable to locate contract")
)

// NurseryConfig abstracts the required subsystems used by the utxo nursery. An
// instance of NurseryConfig is passed to newUtxoNursery during instantiationn.
type NurseryConfig struct {
	// ChainIO is used by the utxo nursery to determine the current block
	// height, which drives the incubation of the nursery's outputs.
	ChainIO lnwallet.BlockChainIO

	// ConfDepth is the number of blocks the nursery store waits before
	// determining outputs in the chain as confirmed.
	ConfDepth uint32

	// PruningDepth is the number of blocks after which the nursery purges
	// its persistent state.
	PruningDepth uint32

	// DB provides access to a user's channels, such that they can be marked
	// fully closed after incubation has concluded.
	DB *channeldb.DB

	// Estimator is used when crafting sweep transactions to estimate the
	// necessary fee relative to the expected size of the sweep transaction.
	Estimator lnwallet.FeeEstimator

	// GenSweepScript generates a P2WKH script belonging to the wallet where
	// funds can be swept.
	GenSweepScript func() ([]byte, error)

	// Notifier provides the utxo nursery the ability to subscribe to
	// transaction confirmation events, which advance outputs through their
	// persistence state transitions.
	Notifier chainntnfs.ChainNotifier

	// PublishTransaction facilitates the process of broadcasting a signed
	// transaction to the appropriate network.
	PublishTransaction func(*wire.MsgTx) error

	// Signer is used by the utxo nursery to generate valid witnesses at the
	// time the incubated outputs need to be spent.
	Signer lnwallet.Signer

	// Store provides access to and modification of the persistent state
	// maintained about the utxo nursery's incubating outputs.
	Store NurseryStore
}

// utxoNursery is a system dedicated to incubating time-locked outputs created
// by the broadcast of a commitment transaction either by us, or the remote
// peer. The nursery accepts outputs and "incubates" them until they've reached
// maturity, then sweep the outputs into the source wallet. An output is
// considered mature after the relative time-lock within the pkScript has
// passed. As outputs reach their maturity age, they're swept in batches into
// the source wallet, returning the outputs so they can be used within future
// channels, or regular Bitcoin transactions.
type utxoNursery struct {
	started uint32
	stopped uint32

	cfg *NurseryConfig

	mu            sync.Mutex
	currentHeight uint32

	quit chan struct{}
	wg   sync.WaitGroup
}

// newUtxoNursery creates a new instance of the utxoNursery from a
// ChainNotifier and LightningWallet instance.
func newUtxoNursery(cfg *NurseryConfig) *utxoNursery {
	return &utxoNursery{
		cfg:  cfg,
		quit: make(chan struct{}),
	}
}

// Start launches all goroutines the utxoNursery needs to properly carry out
// its duties.
func (u *utxoNursery) Start() error {
	if !atomic.CompareAndSwapUint32(&u.started, 0, 1) {
		return nil
	}

	utxnLog.Tracef("Starting UTXO nursery")

	// 1. Flushing all fully-graduated channels from the pipeline.

	// Load any pending close channels, which represents the super set of
	// all channels that may still be incubating.
	pendingCloseChans, err := u.cfg.DB.FetchClosedChannels(true)
	if err != nil {
		utxnLog.Errorf("Unable to fetch closing channels: %v", err)
		return err
	}

	// Ensure that all mature channels have been marked as fully closed in
	// the channeldb.
	for _, pendingClose := range pendingCloseChans {
		if err := u.closeAndRemoveIfMature(&pendingClose.ChanPoint); err != nil {
			return err
		}
	}

	// TODO(conner): check if any fully closed channels can be removed from
	// utxn.

	// 2. Restart spend ntfns for any preschool outputs.

	// Query the database for the most recently processed block. We'll use
	// this to restrict the search space when asking for confirmation
	// notifications, and also to scan the chain to graduate now mature
	// outputs.
	lastGraduatedHeight, err := u.cfg.Store.LastFinalizedHeight()
	if err != nil {
		return err
	}

	// Spawn spend notifications for any outputs found in preschool.
	// NOTE: These next step *may* spawn go routines, thus from this point
	// forward, we must close the nursery's quit channel if we detect a
	// failure to ensure they terminate.
	if err := u.reloadPreschool(lastGraduatedHeight); err != nil {
		close(u.quit)
		return err
	}

	// 3. Replay all crib and kindergarten outputs from last finalized to
	// current best height.

	u.currentHeight = lastGraduatedHeight
	if err := u.reloadClasses(lastGraduatedHeight); err != nil {
		close(u.quit)
		return err
	}

	// 4. Now that we are finalized, start watching for new blocks.

	// Register with the notifier to receive notifications for each newly
	// connected block. We register during startup to ensure that no blocks
	// are missed while we are handling blocks that were missed during the
	// time the UTXO nursery was unavailable.
	newBlockChan, err := u.cfg.Notifier.RegisterBlockEpochNtfn()
	if err != nil {
		close(u.quit)
		return err
	}

	u.wg.Add(1)
	go u.incubator(newBlockChan, lastGraduatedHeight)

	return nil
}

// Stop gracefully shuts down any lingering goroutines launched during normal
// operation of the utxoNursery.
func (u *utxoNursery) Stop() error {
	if !atomic.CompareAndSwapUint32(&u.stopped, 0, 1) {
		return nil
	}

	utxnLog.Infof("UTXO nursery shutting down")

	close(u.quit)
	u.wg.Wait()

	return nil
}

// reloadPreschool re-initializes the chain notifier with all of the outputs
// that had been saved to the "preschool" database bucket prior to shutdown.
func (u *utxoNursery) reloadPreschool(heightHint uint32) error {
	psclOutputs, err := u.cfg.Store.FetchPreschools()
	if err != nil {
		return err
	}

	for i, kid := range psclOutputs {
		txID := kid.OutPoint().Hash

		confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(
			&txID, u.cfg.ConfDepth, heightHint)
		if err != nil {
			return err
		}

		utxnLog.Infof("Preschool outpoint %v re-registered for confirmation "+
			"notification.", kid.OutPoint())

		u.wg.Add(1)
		go u.waitForPromotion(&psclOutputs[i], confChan)
	}

	return nil
}

// reloadClasses replays the graduation of all kindergarten and crib outputs for
// heights that have not been finalized.  This allows the nursery to
// reinitialize all state to continue sweeping outputs, even in the event that
// we missed blocks while offline. reloadClasses is called during the startup of
// the UTXO Nursery.
func (u *utxoNursery) reloadClasses(lastGraduatedHeight uint32) error {
	// Get the most recently mined block.
	_, bestHeight, err := u.cfg.ChainIO.GetBestBlock()
	if err != nil {
		return err
	}

	// If we haven't yet seen any registered force closes, or we're already
	// caught up with the current best chain, then we can exit early.
	if lastGraduatedHeight == 0 || uint32(bestHeight) == lastGraduatedHeight {
		return nil
	}

	utxnLog.Infof("Processing outputs from missed blocks. Starting with "+
		"blockHeight: %v, to current blockHeight: %v", lastGraduatedHeight,
		bestHeight)

	// Loop through and check for graduating outputs at each of the missed
	// block heights.
	for graduationHeight := lastGraduatedHeight + 1; graduationHeight <= uint32(bestHeight); graduationHeight++ {
		// Each attempt at graduation is protected by a lock, since
		// there may be background processes attempting to modify the
		// database concurrently.
		u.mu.Lock()
		if err := u.graduateClass(graduationHeight); err != nil {
			u.mu.Unlock()
			utxnLog.Errorf("Failed to graduate outputs at height=%v: %v",
				graduationHeight, err)
			return err
		}
		u.mu.Unlock()
	}

	utxnLog.Infof("UTXO Nursery is now fully synced")

	return nil
}

// graduateClass handles the steps involved in spending outputs whose CSV or
// CLTV delay expires at the nursery's current height. This method is called
// each time a new block arrives, or during startup to catch up on heights we
// may have missed while the nursery was offline.
func (u *utxoNursery) graduateClass(classHeight uint32) error {

	// Record this height as the nursery's current best height.
	u.currentHeight = classHeight

	// First fetch the set of outputs that we can "graduate" at this
	// particular block height. We can graduate an output once we've
	// reached its height maturity.
	kgtnOutputs, cribOutputs, err := u.cfg.Store.FetchClass(classHeight)
	if err != nil {
		return err
	}

	// If we're able to graduate any outputs, then create a single
	// transaction which sweeps them all into the wallet.
	if len(kgtnOutputs) > 0 {
		err := u.sweepGraduatingOutputs(classHeight, kgtnOutputs)
		if err != nil {
			return err
		}
	}

	for i := range cribOutputs {
		output := &cribOutputs[i]

		// Broadcast HTLC transaction
		// TODO(conner): handle concrete error types returned from publication
		err = u.cfg.PublishTransaction(output.timeoutTx)
		if err != nil &&
			!strings.Contains(err.Error(), "TX rejected:") {
			utxnLog.Errorf("Unable to broadcast baby tx: "+
				"%v, %v", err,
				spew.Sdump(output.timeoutTx))
			return err
		}

		birthTxID := output.OutPoint().Hash

		// Register for the confirmation of baby tx
		confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(
			&birthTxID, u.cfg.ConfDepth, classHeight)
		if err != nil {
			return err
		}

		utxnLog.Infof("Baby output %v registered for promotion "+
			"notification.", output.OutPoint())

		u.wg.Add(1)
		go u.waitForEnrollment(output, confChan)

	}

	// Can't finalize height below the reorg safety depth.
	if u.cfg.PruningDepth >= classHeight {
		return nil
	}

	// Finalize and purge all state below the threshold height.
	heightToFinalize := classHeight - u.cfg.PruningDepth
	if err := u.cfg.Store.FinalizeHeight(heightToFinalize); err != nil {
		utxnLog.Errorf("Failed to finalize height %d", heightToFinalize)
		return err
	}

	utxnLog.Infof("Successfully finalized height %d ", heightToFinalize)

	return nil
}

// sweepGraduatingOutputs generates and broadcasts the transaction that
// transfers control of funds from a channel commitment transaction to the
// user's wallet.
func (u *utxoNursery) sweepGraduatingOutputs(classHeight uint32, kgtnOutputs []kidOutput) error {
	// Create a transaction which sweeps all the newly mature outputs into
	// a output controlled by the wallet.
	// TODO(roasbeef): car be more intelligent about buffering outputs to
	// be more efficient on-chain.

	// Gather the CSV delayed inputs to our sweep transaction, and construct
	// an estimate for the weight of the sweep transaction.
	inputs := make([]CsvSpendableOutput, 0, len(kgtnOutputs))

	var txWeight uint64
	txWeight += 4*lnwallet.BaseSweepTxSize + lnwallet.WitnessHeaderSize

	for i := range kgtnOutputs {
		input := &kgtnOutputs[i]

		var witnessWeight uint64
		switch input.WitnessType() {
		case lnwallet.CommitmentTimeLock:
			witnessWeight = lnwallet.ToLocalTimeoutWitnessSize

		case lnwallet.HtlcOfferedTimeout:
			witnessWeight = lnwallet.OfferedHtlcTimeoutWitnessSize

		default:
			utxnLog.Warnf("kindergarten output in nursery store "+
				"contains unexpected witness type: %v",
				input.WitnessType())
			continue
		}

		txWeight += 4 * lnwallet.InputSize
		txWeight += witnessWeight

		inputs = append(inputs, input)
	}

	sweepTx, err := u.createSweepTx(txWeight, inputs)
	if err != nil {
		// TODO(roasbeef): retry logic?
		utxnLog.Errorf("unable to create sweep tx: %v", err)
		return nil
	}

	utxnLog.Infof("Sweeping %v time-locked outputs "+
		"with sweep tx (txid=%v): %v", len(kgtnOutputs),
		sweepTx.TxHash(),
		newLogClosure(func() string {
			return spew.Sdump(sweepTx)
		}))

	// With the sweep transaction fully signed, broadcast the transaction
	// to the network. Additionally, we can stop tracking these outputs as
	// they've just been swept.
	// TODO(conner): handle concrete error types returned from publication
	if err := u.cfg.PublishTransaction(sweepTx); err != nil &&
		!strings.Contains(err.Error(), "TX rejected:") {
		utxnLog.Errorf("unable to broadcast sweep tx: %v, %v",
			err, spew.Sdump(sweepTx))
		return err
	}

	sweepTxID := sweepTx.TxHash()

	utxnLog.Infof("Registering sweep tx %v for confs at height %d",
		sweepTxID, classHeight)

	confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(
		&sweepTxID, u.cfg.ConfDepth, classHeight)
	if err != nil {
		utxnLog.Errorf("unable to register notification for "+
			"sweep confirmation: %v", sweepTxID)
		return err
	}

	u.wg.Add(1)
	go u.waitForGraduation(classHeight, kgtnOutputs, confChan)

	return nil
}

// createSweepTx creates a final sweeping transaction with all witnesses in
// place for all inputs. The created transaction has a single output sending
// all the funds back to the source wallet.
func (u *utxoNursery) createSweepTx(txWeight uint64,
	inputs []CsvSpendableOutput) (*wire.MsgTx, error) {

	pkScript, err := u.cfg.GenSweepScript()
	if err != nil {
		return nil, err
	}

	var totalSum btcutil.Amount
	for _, o := range inputs {
		totalSum += o.Amount()
	}

	feePerWeight := u.cfg.Estimator.EstimateFeePerWeight(1)
	txFee := btcutil.Amount(txWeight * feePerWeight)

	sweepAmt := int64(totalSum - txFee)

	sweepTx := wire.NewMsgTx(2)
	sweepTx.AddTxOut(&wire.TxOut{
		PkScript: pkScript,
		Value:    sweepAmt,
	})
	for _, input := range inputs {
		sweepTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: *input.OutPoint(),
			// TODO(roasbeef): assumes pure block delays
			Sequence: input.BlocksToMaturity(),
		})
	}

	// Before signing the transaction, check to ensure that it meets some
	// basic validity requirements.
	btx := btcutil.NewTx(sweepTx)
	if err := blockchain.CheckTransactionSanity(btx); err != nil {
		return nil, err
	}

	hashCache := txscript.NewTxSigHashes(sweepTx)

	// With all the inputs in place, use each output's unique witness
	// function to generate the final witness required for spending.

	addWitness := func(idx int, tso CsvSpendableOutput) error {
		witness, err := tso.BuildWitness(u.cfg.Signer, sweepTx, hashCache, idx)
		if err != nil {
			return err
		}

		sweepTx.TxIn[idx].Witness = witness

		return nil
	}

	for i, input := range inputs {
		if err := addWitness(i, input); err != nil {
			return nil, err
		}
	}

	return sweepTx, nil
}

// IncubateOutputs sends a request to utxoNursery to incubate the outputs
// defined within the summary of a closed channel. Individually, as all outputs
// reach maturity, they'll be swept back into the wallet.
func (u *utxoNursery) IncubateOutputs(closeSummary *lnwallet.ForceCloseSummary) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	var (
		commOutput  *kidOutput
		htlcOutputs = make([]babyOutput, 0, len(closeSummary.HtlcResolutions))
	)

	// 1. Build all the spendable outputs that we will try to incubate.

	// It could be that our to-self output was below the dust limit. In that
	// case the SignDescriptor would be nil and we would not have that
	// output to incubate.
	if closeSummary.SelfOutputSignDesc != nil {
		selfOutput := makeKidOutput(
			&closeSummary.SelfOutpoint,
			&closeSummary.ChanPoint,
			closeSummary.SelfOutputMaturity,
			lnwallet.CommitmentTimeLock,
			closeSummary.SelfOutputSignDesc,
		)

		// We'll skip any zero value'd outputs as this indicates we
		// don't have a settled balance within the commitment
		// transaction.
		if selfOutput.Amount() > 0 {
			commOutput = &selfOutput
		}
	}

	for i := range closeSummary.HtlcResolutions {
		htlcRes := closeSummary.HtlcResolutions[i]

		htlcOutpoint := &wire.OutPoint{
			Hash:  htlcRes.SignedTimeoutTx.TxHash(),
			Index: 0,
		}

		utxnLog.Infof("htlc resolution with expiry: %v",
			htlcRes.Expiry)

		htlcOutput := makeBabyOutput(
			htlcOutpoint,
			&closeSummary.ChanPoint,
			closeSummary.SelfOutputMaturity,
			lnwallet.HtlcOfferedTimeout,
			&htlcRes,
		)

		if htlcOutput.Amount() > 0 {
			htlcOutputs = append(htlcOutputs, htlcOutput)
		}

	}

	// 2. Persist the outputs we intended to sweep in the nursery store

	if err := u.cfg.Store.Incubate(commOutput, htlcOutputs); err != nil {
		utxnLog.Infof("Unable to persist incubation of channel %v: %v",
			&closeSummary.ChanPoint, err)
		return err
	}

	// 3. If we are incubating a preschool output, register for a spend
	// notification that will transition it to the kindergarten bucket.

	if commOutput != nil {
		commitTxID := commOutput.OutPoint().Hash

		// Register for a notification that will trigger graduation from
		// preschool to kindergarten when the channel close transaction
		// has been confirmed.
		confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(
			&commitTxID, u.cfg.ConfDepth, u.currentHeight)
		if err != nil {
			utxnLog.Errorf("Unable to register preschool output %v for "+
				"confirmation: %v", commitTxID, err)
			return err
		}

		utxnLog.Infof("Added kid output to pscl: %v",
			commOutput.OutPoint())

		// Launch a dedicated goroutine that will move the output from
		// the preschool bucket to the kindergarten bucket once the
		// channel close transaction has been confirmed.
		u.wg.Add(1)
		go u.waitForPromotion(commOutput, confChan)
	}

	return nil
}

// incubator is tasked with driving all state transitions that are dependent on
// the current height of the blockchain. As new blocks arrive, the incubator
// will attempt spend outputs at the latest height. The asynchronous
// confirmation of these spends will either 1) move a crib output into the
// kindergarten bucket or 2) move a kindergarten output into the graduated
// bucket. The incubator is also designed to purge all state below the config's
// PruningDepth to avoid indefinitely persisting stale data.
func (u *utxoNursery) incubator(newBlockChan *chainntnfs.BlockEpochEvent,
	startingHeight uint32) {

	defer u.wg.Done()
	defer newBlockChan.Cancel()

	u.currentHeight = startingHeight
out:
	for {
		select {
		case epoch, ok := <-newBlockChan.Epochs:
			// If the epoch channel has been closed, then the
			// ChainNotifier is exiting which means the daemon is
			// as well. Therefore, we exit early also in order to
			// ensure the daemon shuts down gracefully, yet
			// swiftly.
			if !ok {
				return
			}

			// TODO(roasbeef): if the BlockChainIO is rescanning
			// will give stale data

			// A new block has just been connected to the main
			// chain which means we might be able to graduate some
			// outputs out of the kindergarten bucket. Graduation
			// entails successfully sweeping a time-locked output.
			height := uint32(epoch.Height)

			u.mu.Lock()
			if err := u.graduateClass(height); err != nil {
				utxnLog.Errorf("error while graduating "+
					"kindergarten outputs: %v", err)
			}
			u.mu.Unlock()

		case <-u.quit:
			break out
		}
	}
}

// contractMaturityReport is a report that details the maturity progress of a
// particular force closed contract.
type contractMaturityReport struct {
	// chanPoint is the channel point of the original contract that is now
	// awaiting maturity within the utxoNursery.
	chanPoint wire.OutPoint

	// limboBalance is the total number of frozen coins within this
	// contract.
	limboBalance btcutil.Amount

	// confirmationHeight is the block height that this output originally
	// confirmed at.
	confirmationHeight uint32

	// maturityRequirement is the input age required for this output to
	// reach maturity.
	maturityRequirement uint32

	// maturityHeight is the absolute block height that this output will mature
	// at.
	maturityHeight uint32
}

// NurseryReport attempts to return a nursery report stored for the target
// outpoint. A nursery report details the maturity/sweeping progress for a
// contract that was previously force closed. If a report entry for the target
// chanPoint is unable to be constructed, then an error will be returned.
func (u *utxoNursery) NurseryReport(
	chanPoint *wire.OutPoint) (*contractMaturityReport, error) {

	utxnLog.Infof("NurseryReport: building nursery report for channel %v",
		chanPoint)

	var report *contractMaturityReport
	if err := u.cfg.Store.ForChanOutputs(chanPoint, func(k, v []byte) error {
		var prefix [4]byte
		copy(prefix[:], k[:4])

		switch string(prefix[:]) {
		case string(psclPrefix), string(kndrPrefix):

			// information for this immature output.
			var kid kidOutput
			kidReader := bytes.NewReader(v)
			err := kid.Decode(kidReader)
			if err != nil {
				return err
			}

			utxnLog.Infof("NurseryReport: found kid output: %v",
				kid.OutPoint())

			// TODO(roasbeef): should actually be list of outputs
			report = &contractMaturityReport{
				chanPoint:           *chanPoint,
				limboBalance:        kid.Amount(),
				maturityRequirement: kid.BlocksToMaturity(),
			}

			// If the confirmation height is set, then this means the
			// contract has been confirmed, and we know the final maturity
			// height.
			if kid.ConfHeight() != 0 {
				report.confirmationHeight = kid.ConfHeight()
				report.maturityHeight = (kid.BlocksToMaturity() +
					kid.ConfHeight())
			}

		case string(cribPrefix):
			utxnLog.Infof("NurseryReport: found crib output: %x", k[4:])

		case string(gradPrefix):

			utxnLog.Infof("NurseryReport: found grad output: %x", k[4:])
		default:
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return report, nil
}

// waitForEnrollment watches for the confirmation of an htlc timeout
// transaction, and attempts to move the htlc output from the crib bucket to the
// kindergarten bucket upon success.
func (u *utxoNursery) waitForEnrollment(baby *babyOutput,
	confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case txConfirmation, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan "+
				"closed, can't advance baby output %v",
				baby.OutPoint())
			return
		}

		baby.SetConfHeight(txConfirmation.BlockHeight)

	case <-u.quit:
		return
	}

	// TODO(conner): add retry logic?

	u.mu.Lock()
	defer u.mu.Unlock()

	err := u.cfg.Store.CribToKinder(baby)
	if err != nil {
		utxnLog.Errorf("Unable to move htlc output from "+
			"crib to kindergarten bucket: %v", err)
		return
	}

	utxnLog.Infof("Htlc output %v promoted to "+
		"kindergarten", baby.OutPoint())
}

// waitForPromotion is intended to be run as a goroutine that will wait until a
// channel force close commitment transaction has been included in a confirmed
// block. Once the transaction has been confirmed (as reported by the Chain
// Notifier), waitForPromotion will delete the output from the "preschool"
// database bucket and atomically add it to the "kindergarten" database bucket.
// This is the second step in the output incubation process.
func (u *utxoNursery) waitForPromotion(kid *kidOutput,
	confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case txConfirmation, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan "+
				"closed, can't advance output %v",
				kid.OutPoint())
			return
		}

		kid.SetConfHeight(txConfirmation.BlockHeight)

	case <-u.quit:
		return
	}

	// TODO(conner): add retry logic?

	u.mu.Lock()
	defer u.mu.Unlock()

	err := u.cfg.Store.PreschoolToKinder(kid)
	if err != nil {
		utxnLog.Errorf("Unable to move kid output "+
			"from preschool to kindergarten bucket: %v",
			err)
		return
	}

	utxnLog.Infof("Preschool output %v promoted to "+
		"kindergarten", kid.OutPoint())
}

// waitForGraduation watches for the confirmation of a sweep transaction
// containing a batch of kindergarten outputs. Once confirmation has been
// received, the nursery will mark those outputs as fully graduated, and proceed
// to mark any mature channels as fully closed in channeldb.
// NOTE(conner): this method MUST be called as a go routine.
func (u *utxoNursery) waitForGraduation(classHeight uint32, kgtnOutputs []kidOutput,
	confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case _, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan closed, can't"+
				" advance %v graduating outputs", len(kgtnOutputs))
			return
		}

	case <-u.quit:
		return
	}

	// TODO(conner): add retry logic?

	u.mu.Lock()
	defer u.mu.Unlock()

	// Mark the confirmed kindergarten outputs as graduated.
	if err := u.cfg.Store.GraduateKinder(kgtnOutputs); err != nil {
		utxnLog.Errorf("Unable to award diplomas to %v"+
			"graduating outputs: %v", len(kgtnOutputs), err)
		return
	}

	utxnLog.Infof("Graduated %d kindergarten outputs from height %d",
		len(kgtnOutputs), classHeight)

	// Iterate over the kid outputs and construct a set of all channel
	// points to which they belong.
	var possibleCloses = make(map[wire.OutPoint]struct{})
	for _, kid := range kgtnOutputs {
		possibleCloses[*kid.OriginChanPoint()] = struct{}{}

	}

	// Attempt to close each channel, only doing so if all of the channel's
	// outputs have been graduated.
	for chanPoint := range possibleCloses {
		if err := u.closeAndRemoveIfMature(&chanPoint); err != nil {
			utxnLog.Errorf("Failed to close and remove channel %v", chanPoint)
			return
		}
	}

	if err := u.cfg.Store.TryFinalizeClass(classHeight); err != nil {
		utxnLog.Errorf("Attempt to finalize height %d failed", classHeight)
		return
	}

	lastHeight, _ := u.cfg.Store.LastFinalizedHeight()

	utxnLog.Errorf("Successfully finalized height %d of %d", lastHeight, classHeight)
}

// closeAndRemoveIfMature removes a particular channel from the channel index
// if and only if all of its outputs have been marked graduated. If the channel
// still has ungraduated outputs, the method will succeed without altering the
// database state.
func (u *utxoNursery) closeAndRemoveIfMature(chanPoint *wire.OutPoint) error {
	isMature, err := u.cfg.Store.IsMatureChannel(chanPoint)
	if err == ErrContractNotFound {
		return nil
	} else if err != nil {
		utxnLog.Errorf("Unable to determine maturity of "+
			"channel %v", chanPoint)
		return err
	}

	if !isMature {
		utxnLog.Errorf("Not closing immature channel %v", chanPoint)
		return nil
	}

	utxnLog.Infof("Attempting to close mature channel %v", chanPoint)

	// Now that the sweeping transaction has been broadcast, for
	// each of the immature outputs, we'll mark them as being fully
	// closed within the database.
	err = u.cfg.DB.MarkChanFullyClosed(chanPoint)
	if err != nil {
		utxnLog.Errorf("Unable to mark channel %v as fully "+
			"closed: %v", chanPoint, err)
		return err
	}

	utxnLog.Infof("Successfully marked channel %v as fully closed", chanPoint)

	if err := u.cfg.Store.RemoveChannel(chanPoint); err != nil {
		utxnLog.Errorf("Unable to remove channel %v from "+
			"nursery store: %v", chanPoint, err)
		return err
	}

	utxnLog.Infof("Successfully removed channel %v from nursery store", chanPoint)

	return nil
}

// newSweepPkScript creates a new public key script which should be used to
// sweep any time-locked, or contested channel funds into the wallet.
// Specifically, the script generated is a version 0,
// pay-to-witness-pubkey-hash (p2wkh) output.
func newSweepPkScript(wallet lnwallet.WalletController) ([]byte, error) {
	sweepAddr, err := wallet.NewAddress(lnwallet.WitnessPubKey, false)
	if err != nil {
		return nil, err
	}

	return txscript.PayToAddrScript(sweepAddr)
}

// CsvSpendableOutput is a SpendableOutput that contains all of the information
// necessary to construct, sign, and sweep an output locked with a CSV delay.
type CsvSpendableOutput interface {
	SpendableOutput

	// ConfHeight returns the height at which this output was confirmed.
	// A zero value indicates that the output has not been confirmed.
	ConfHeight() uint32

	// SetConfHeight marks the height at which the output is confirmed in
	// the chain.
	SetConfHeight(height uint32)

	// BlocksToMaturity returns the relative timelock, as a number of
	// blocks, that must be built on top of the confirmation height before
	// the output can be spent.
	BlocksToMaturity() uint32

	// OriginChanPoint returns the outpoint of the channel from which this
	// output is derived.
	OriginChanPoint() *wire.OutPoint
}

// babyOutput is an HTLC output that is in the earliest stage of upbringing.
// Each babyOutput carries a presigned timeout transction, which should be
// broadcast at the appropriate CLTV expiry, and its future kidOutput self. If
// all goes well, and the timeout transaction is successfully confirmed, the
// the now-mature kidOutput will be unwrapped and continue its journey through
// the nursery.
type babyOutput struct {
	// expiry is the absolute block height at which the timeoutTx should be
	// broadcast to the network.
	expiry uint32

	// timeoutTx is a fully-signed transaction that, upon confirmation,
	// transitions the htlc into the delay+claim stage.
	timeoutTx *wire.MsgTx

	// kidOutput represents the CSV output to be swept after the timeoutTx has
	// been broadcast and confirmed.
	kidOutput
}

// makeBabyOutput constructs a baby output the wraps a future kidOutput. The
// provided sign descriptors and witness types will be used once the output
// reaches the delay and claim stage.
func makeBabyOutput(outpoint, originChanPoint *wire.OutPoint,
	blocksToMaturity uint32, witnessType lnwallet.WitnessType,
	htlcResolution *lnwallet.OutgoingHtlcResolution) babyOutput {

	kid := makeKidOutput(outpoint, originChanPoint,
		blocksToMaturity, witnessType,
		&htlcResolution.SweepSignDesc)

	return babyOutput{
		kidOutput: kid,
		expiry:    htlcResolution.Expiry,
		timeoutTx: htlcResolution.SignedTimeoutTx,
	}
}

// Encode writes the baby output to the given io.Writer.
func (bo *babyOutput) Encode(w io.Writer) error {
	var scratch [4]byte
	byteOrder.PutUint32(scratch[:], bo.expiry)
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	if err := bo.timeoutTx.Serialize(w); err != nil {
		return err
	}

	return bo.kidOutput.Encode(w)
}

// Decode reconstructs a baby output using the provided io.Reader.
func (bo *babyOutput) Decode(r io.Reader) error {
	var scratch [4]byte
	if _, err := r.Read(scratch[:]); err != nil {
		return err
	}
	bo.expiry = byteOrder.Uint32(scratch[:])

	bo.timeoutTx = new(wire.MsgTx)
	if err := bo.timeoutTx.Deserialize(r); err != nil {
		return err
	}

	return bo.kidOutput.Decode(r)
}

// kidOutput represents an output that's waiting for a required blockheight
// before its funds will be available to be moved into the user's wallet.  The
// struct includes a WitnessGenerator closure which will be used to generate
// the witness required to sweep the output once it's mature.
//
// TODO(roasbeef): rename to immatureOutput?
type kidOutput struct {
	breachedOutput

	originChanPoint wire.OutPoint

	// TODO(roasbeef): using block timeouts everywhere currently, will need
	// to modify logic later to account for MTP based timeouts.
	blocksToMaturity uint32
	confHeight       uint32
}

func makeKidOutput(outpoint, originChanPoint *wire.OutPoint,
	blocksToMaturity uint32, witnessType lnwallet.WitnessType,
	signDescriptor *lnwallet.SignDescriptor) kidOutput {

	return kidOutput{
		breachedOutput: makeBreachedOutput(
			outpoint, witnessType, signDescriptor,
		),
		originChanPoint:  *originChanPoint,
		blocksToMaturity: blocksToMaturity,
	}
}

func (k *kidOutput) OriginChanPoint() *wire.OutPoint {
	return &k.originChanPoint
}

func (k *kidOutput) BlocksToMaturity() uint32 {
	return k.blocksToMaturity
}

func (k *kidOutput) SetConfHeight(height uint32) {
	k.confHeight = height
}

func (k *kidOutput) ConfHeight() uint32 {
	return k.confHeight
}

// Encode converts a KidOutput struct into a form suitable for on-disk database
// storage. Note that the signDescriptor struct field is included so that the
// output's witness can be generated by createSweepTx() when the output becomes
// spendable.
func (k *kidOutput) Encode(w io.Writer) error {
	var scratch [8]byte
	byteOrder.PutUint64(scratch[:], uint64(k.Amount()))
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	if err := writeOutpoint(w, k.OutPoint()); err != nil {
		return err
	}
	if err := writeOutpoint(w, k.OriginChanPoint()); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch[:4], k.BlocksToMaturity())
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch[:4], k.ConfHeight())
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}

	byteOrder.PutUint16(scratch[:2], uint16(k.WitnessType()))
	if _, err := w.Write(scratch[:2]); err != nil {
		return err
	}

	return lnwallet.WriteSignDescriptor(w, k.SignDesc())
}

// Decode takes a byte array representation of a kidOutput and converts it to an
// struct. Note that the witnessFunc method isn't added during deserialization
// and must be added later based on the value of the witnessType field.
func (k *kidOutput) Decode(r io.Reader) error {
	var scratch [8]byte

	if _, err := r.Read(scratch[:]); err != nil {
		return err
	}
	k.amt = btcutil.Amount(byteOrder.Uint64(scratch[:]))

	if err := readOutpoint(io.LimitReader(r, 40), &k.outpoint); err != nil {
		return err
	}

	err := readOutpoint(io.LimitReader(r, 40), &k.originChanPoint)
	if err != nil {
		return err
	}

	if _, err := r.Read(scratch[:4]); err != nil {
		return err
	}
	k.blocksToMaturity = byteOrder.Uint32(scratch[:4])

	if _, err := r.Read(scratch[:4]); err != nil {
		return err
	}
	k.confHeight = byteOrder.Uint32(scratch[:4])

	if _, err := r.Read(scratch[:2]); err != nil {
		return err
	}
	k.witnessType = lnwallet.WitnessType(byteOrder.Uint16(scratch[:2]))

	return lnwallet.ReadSignDescriptor(r, &k.signDesc)
}

// TODO(bvu): copied from channeldb, remove repetition
func writeOutpoint(w io.Writer, o *wire.OutPoint) error {
	// TODO(roasbeef): make all scratch buffers on the stack
	scratch := make([]byte, 4)

	// TODO(roasbeef): write raw 32 bytes instead of wasting the extra
	// byte.
	if err := wire.WriteVarBytes(w, 0, o.Hash[:]); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch, o.Index)
	_, err := w.Write(scratch)
	return err
}

// TODO(bvu): copied from channeldb, remove repetition
func readOutpoint(r io.Reader, o *wire.OutPoint) error {
	scratch := make([]byte, 4)

	txid, err := wire.ReadVarBytes(r, 0, 32, "prevout")
	if err != nil {
		return err
	}
	copy(o.Hash[:], txid)

	if _, err := r.Read(scratch); err != nil {
		return err
	}
	o.Index = byteOrder.Uint32(scratch)

	return nil
}

func writeTxOut(w io.Writer, txo *wire.TxOut) error {
	scratch := make([]byte, 8)

	byteOrder.PutUint64(scratch, uint64(txo.Value))
	if _, err := w.Write(scratch); err != nil {
		return err
	}

	if err := wire.WriteVarBytes(w, 0, txo.PkScript); err != nil {
		return err
	}

	return nil
}

func readTxOut(r io.Reader, txo *wire.TxOut) error {
	scratch := make([]byte, 8)

	if _, err := r.Read(scratch); err != nil {
		return err
	}
	txo.Value = int64(byteOrder.Uint64(scratch))

	pkScript, err := wire.ReadVarBytes(r, 0, 80, "pkScript")
	if err != nil {
		return err
	}
	txo.PkScript = pkScript

	return nil
}

// Compile-time constraint to ensure kidOutput and babyOutpt implement the
// CsvSpendableOutput interface.
var _ CsvSpendableOutput = (*kidOutput)(nil)
var _ CsvSpendableOutput = (*babyOutput)(nil)
