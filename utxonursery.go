package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/boltdb/bolt"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/roasbeef/btcd/blockchain"
	"github.com/roasbeef/btcd/txscript"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcutil"
)

var (
	// preschoolBucket stores outputs from commitment transactions that
	// have been broadcast, but not yet confirmed. This set of outputs is
	// persisted in case the system is shut down between the time when the
	// commitment has been broadcast and the time the transaction has been
	// confirmed on the blockchain.
	// TODO(roasbeef): modify schema later to be:
	//  * chanPoint ->
	//               {outpoint1} -> info
	//               {outpoint2} -> info
	preschoolBucket = []byte("psc")

	// preschoolIndex is an index that maps original chanPoint that created
	// the channel to all the active time-locked outpoints for that
	// channel.
	preschoolIndex = []byte("preschool-index")

	// kindergartenBucket stores outputs from commitment transactions that
	// have received an initial confirmation, but which aren't yet
	// spendable because they require additional confirmations enforced by
	// CheckSequenceVerify. Once required additional confirmations have
	// been reported, a sweep transaction will be created to move the funds
	// out of these outputs. After a further six confirmations have been
	// reported, the outputs will be deleted from this bucket. The purpose
	// of this additional wait time is to ensure that a block
	// reorganization doesn't result in the sweep transaction getting
	// re-organized out of the chain.
	// TODO(roasbeef): modify schema later to be:
	//   * height ->
	//              {chanPoint} -> info
	kindergartenBucket = []byte("kdg")

	// contractIndex is an index that maps a contract's channel point to
	// the current information pertaining to the maturity of outputs within
	// that contract. Items are inserted into this index once they've been
	// accepted to pre-school and deleted after the output has been fully
	// swept.
	//
	// mapping: chanPoint -> graduationHeight || byte-offset-in-kindergartenBucket
	contractIndex = []byte("contract-index")

	// lastGraduatedHeightKey is used to persist the last block height that
	// has been checked for graduating outputs. When the nursery is
	// restarted, lastGraduatedHeightKey is used to determine the point
	// from which it's necessary to catch up.
	lastGraduatedHeightKey = []byte("lgh")

	byteOrder = binary.BigEndian
)

var (
	// ErrContractNotFound is returned when the nursery is unable to
	// retreive information about a queried contract.
	ErrContractNotFound = fmt.Errorf("unable to locate contract")
)

// utxoNursery is a system dedicated to incubating time-locked outputs created
// by the broadcast of a commitment transaction either by us, or the remote
// peer. The nursery accepts outputs and "incubates" them until they've reached
// maturity, then sweep the outputs into the source wallet. An output is
// considered mature after the relative time-lock within the pkScript has
// passed. As outputs reach their maturity age, they're swept in batches into
// the source wallet, returning the outputs so they can be used within future
// channels, or regular Bitcoin transactions.
type utxoNursery struct {
	mu            sync.Mutex
	currentHeight uint32
	db            *channeldb.DB

	notifier  chainntnfs.ChainNotifier
	wallet    *lnwallet.LightningWallet
	estimator lnwallet.FeeEstimator
	store     NurseryStore

	started uint32
	stopped uint32
	quit    chan struct{}
	wg      sync.WaitGroup
}

// newUtxoNursery creates a new instance of the utxoNursery from a
// ChainNotifier and LightningWallet instance.
func newUtxoNursery(db *channeldb.DB, notifier chainntnfs.ChainNotifier,
	wallet *lnwallet.LightningWallet,
	fe lnwallet.FeeEstimator, ns NurseryStore) *utxoNursery {

	return &utxoNursery{
		notifier:  notifier,
		wallet:    wallet,
		estimator: fe,
		store:     ns,
		db:        db,
		quit:      make(chan struct{}),
	}
}

// Start launches all goroutines the utxoNursery needs to properly carry out
// its duties.
func (u *utxoNursery) Start() error {
	if !atomic.CompareAndSwapUint32(&u.started, 0, 1) {
		return nil
	}

	utxnLog.Tracef("Starting UTXO nursery")

	// Query the database for the most recently processed block. We'll use
	// this to strict the search space when asking for confirmation
	// notifications, and also to scan the chain to graduate now mature
	// outputs.
	lastGraduatedHeight, err := u.store.LastAttemptedHeight()
	if err != nil {
		return err
	}

	if err := u.reloadPreschool(lastGraduatedHeight); err != nil {
		return err
	}

	// Register with the notifier to receive notifications for each newly
	// connected block. We register during startup to ensure that no blocks
	// are missed while we are handling blocks that were missed during the
	// time the UTXO nursery was unavailable.
	newBlockChan, err := u.notifier.RegisterBlockEpochNtfn()
	if err != nil {
		return err
	}
	if err := u.catchUpKindergarten(lastGraduatedHeight); err != nil {
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
	return u.store.ForEachPreschool(func(kid *kidOutput) error {
		txID := kid.OutPoint().Hash

		confChan, err := u.notifier.RegisterConfirmationsNtfn(
			&txID, 1, heightHint)
		if err != nil {
			return err
		}

		utxnLog.Infof("Preschool outpoint %v re-registered for confirmation "+
			"notification.", kid.OutPoint())

		u.wg.Add(1)
		go u.waitForPromotion(kid, confChan)

		return nil
	})
}

// catchUpKindergarten handles the graduation of kindergarten outputs from
// blocks that were missed while the UTXO Nursery was down or offline.
// graduateMissedBlocks is called during the startup of the UTXO Nursery.
func (u *utxoNursery) catchUpKindergarten(lastGraduatedHeight uint32) error {
	// Get the most recently mined block
	_, bestHeight, err := u.wallet.Cfg.ChainIO.GetBestBlock()
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
		utxnLog.Debugf("Attempting to graduate outputs at height=%v",
			graduationHeight)

		if err := u.graduateKindergarten(graduationHeight); err != nil {
			return err
		}
	}

	utxnLog.Infof("UTXO Nursery is now fully synced")

	return nil
}

// graduateKindergarten handles the steps invoked with moving funds from a
// force close commitment transaction into a user's wallet after the output
// from the commitment transaction has become spendable. graduateKindergarten
// is called both when a new block notification has been received and also at
// startup in order to process graduations from blocks missed while the UTXO
// nursery was offline.
// TODO(roasbeef): single db transaction for the below
func (u *utxoNursery) graduateKindergarten(blockHeight uint32) error {

	// Immediately mark that we are attempting to process this block height.
	// During restarts, this will allow the utxo nursery to re-initialize
	// any tasks that are required to completely finish processing the
	// outputs at this height.
	if err := u.store.BeginCeremony(blockHeight); err != nil {
		return err
	}

	// First fetch the set of outputs that we can "graduate" at this
	// particular block height. We can graduate an output once we've
	// reached its height maturity.
	kgtnOutputs, err := u.store.FetchGraduatingOutputs(blockHeight)
	if err != nil {
		return err
	}

	// If we're able to graduate any outputs, then create a single
	// transaction which sweeps them all into the wallet.
	if len(kgtnOutputs) > 0 {
		err := u.sweepGraduatingOutputs(kgtnOutputs)
		if err != nil {
			return err
		}
	}

	wombOutputs, err := u.store.FetchBirthingOutputs(blockHeight)
	if err != nil {
		return err
	}

	for i := range wombOutputs {
		output := &wombOutputs[i]

		// Broadcast transaction, canceling notification if we fail
		err = u.wallet.PublishTransaction(output.timeoutTx)
		if err != nil {
			utxnLog.Errorf("unable to broadcast womb tx: "+
				"%v, %v", err,
				spew.Sdump(output.timeoutTx))
			return err
		}

		birthTxID := output.OutPoint().Hash

		// Register for the confirmation of womb tx
		confChan, err := u.notifier.RegisterConfirmationsNtfn(
			&birthTxID, 1, blockHeight,
		)
		if err != nil {
			return err
		}

		utxnLog.Infof("Womb output %v registered for promotion "+
			"notification.", output.OutPoint())

		u.wg.Add(1)
		go u.waitForBirth(output, confChan)

	}

	return nil
}

// sweepGraduatingOutputs generates and broadcasts the transaction that
// transfers control of funds from a channel commitment transaction to the
// user's wallet.
func (u *utxoNursery) sweepGraduatingOutputs(kgtnOutputs []kidOutput) error {
	// Create a transaction which sweeps all the newly mature outputs into
	// a output controlled by the wallet.
	// TODO(roasbeef): can be more intelligent about buffering outputs to
	// be more efficient on-chain.
	inputs := make([]TLockSpendableOutput, 0, len(kgtnOutputs))
	for i := range kgtnOutputs {
		inputs = append(inputs, &kgtnOutputs[i])
	}

	sweepTx, err := u.createSweepTx(inputs)
	if err != nil {
		// TODO(roasbeef): retry logic?
		utxnLog.Errorf("unable to create sweep tx: %v", err)
		return err
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
	if err := u.wallet.PublishTransaction(sweepTx); err != nil {
		utxnLog.Errorf("unable to broadcast sweep tx: %v, %v",
			err, spew.Sdump(sweepTx))
		return err
	}

	sweepTxID := sweepTx.TxHash()

	confChan, err := u.notifier.RegisterConfirmationsNtfn(
		&sweepTxID, 1, u.currentHeight)
	if err != nil {
		utxnLog.Errorf("unable to register notification for "+
			"sweep confirmation: %v", sweepTxID)
		return err
	}

	u.wg.Add(1)
	go u.waitForGraduation(kgtnOutputs, confChan)

	return nil
}

// createSweepTx creates a final sweeping transaction with all witnesses in
// place for all inputs. The created transaction has a single output sending
// all the funds back to the source wallet.
func (u *utxoNursery) createSweepTx(
	inputs []TLockSpendableOutput) (*wire.MsgTx, error) {

	pkScript, err := newSweepPkScript(u.wallet)
	if err != nil {
		return nil, err
	}

	var totalSum btcutil.Amount
	for _, o := range inputs {
		totalSum += o.Amount()
	}

	sweepTx := wire.NewMsgTx(2)
	sweepTx.AddTxOut(&wire.TxOut{
		PkScript: pkScript,
		Value:    int64(totalSum - 5000),
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

	// TODO(roasbeef): insert fee calculation
	//  * remove hardcoded fee above

	hashCache := txscript.NewTxSigHashes(sweepTx)

	// With all the inputs in place, use each output's unique witness
	// function to generate the final witness required for spending.

	addWitness := func(idx int, tso TLockSpendableOutput) error {
		witness, err := tso.BuildWitness(u.wallet.Cfg.Signer, sweepTx,
			hashCache, idx)
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

// incubateOutputs sends a request to utxoNursery to incubate the outputs
// defined within the summary of a closed channel. Individually, as all outputs
// reach maturity they'll be swept back into the wallet.
func (u *utxoNursery) IncubateOutputs(closeSummary *lnwallet.ForceCloseSummary) {
	u.mu.Lock()
	defer u.mu.Unlock()

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
		if selfOutput.Amount() == 0 {
			goto wombOutputs
		}

		sourceTxid := selfOutput.OutPoint().Hash

		err := u.store.EnterPreschool(&selfOutput)
		if err != nil {
			utxnLog.Errorf("unable to add kidOutput "+
				"to new preschool: %v, %v", selfOutput, err)
			goto wombOutputs
		}

		// Register for a notification that will trigger graduation from
		// preschool to kindergarten when the channel close transaction
		// has been confirmed.
		confChan, err := u.notifier.RegisterConfirmationsNtfn(
			&sourceTxid, 1, u.currentHeight)
		if err != nil {
			utxnLog.Errorf("unable to register output for "+
				"confirmation: %v", sourceTxid)
			goto wombOutputs
		}

		utxnLog.Infof("Added kid output to pscl: %v",
			selfOutput.OutPoint())

		// Launch a dedicated goroutine that will move the output from
		// the preschool bucket to the kindergarten bucket once the
		// channel close transaction has been confirmed.
		u.wg.Add(1)
		go u.waitForPromotion(&selfOutput, confChan)
	}

wombOutputs:
	for i := range closeSummary.HtlcResolutions {
		htlcRes := closeSummary.HtlcResolutions[i]

		htlcOutpoint := &wire.OutPoint{
			Hash:  htlcRes.SignedTimeoutTx.TxHash(),
			Index: 0,
		}

		utxnLog.Infof("htlc resolution with expiry: %v",
			htlcRes.Expiry)

		htlcOutput := makeWombOutput(
			htlcOutpoint,
			&closeSummary.ChanPoint,
			closeSummary.SelfOutputMaturity,
			lnwallet.HtlcOfferedTimeLock,
			&htlcRes,
		)

		if htlcOutput.Amount() == 0 {
			continue
		}

		err := u.store.ConceiveOutput(&htlcOutput)
		if err != nil {
			continue
		}

		utxnLog.Infof("Added htlc output to womb: %v",
			htlcOutput.OutPoint())
	}
}

// incubator is tasked with watching over all outputs from channel closes as
// they transition from being broadcast (at which point they move into the
// "preschool state"), then confirmed and waiting for the necessary number of
// blocks to be confirmed (as specified as kidOutput.blocksToMaturity and
// enforced by CheckSequenceVerify). When the necessary block height has been
// reached, the output has "matured" and the waitForGraduation function will
// generate a sweep transaction to move funds from the commitment transaction
// into the user's wallet.
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
			u.currentHeight = height
			if err := u.graduateKindergarten(height); err != nil {
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

	var report *contractMaturityReport
	if err := u.store.ForChanOutputs(chanPoint, func(k, v []byte) error {
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
		case string(wombPrefix):
		default:
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return report, nil
}

func (u *utxoNursery) waitForBirth(womb *wombOutput,
	confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case txConfirmation, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan "+
				"closed, can't advance womb output %v",
				womb.OutPoint())
			return
		}

		womb.SetConfHeight(txConfirmation.BlockHeight)

	case <-u.quit:
		return
	}

	// TODO(conner): add retry logic?

	u.mu.Lock()
	defer u.mu.Unlock()

	err := u.store.BirthToKinder(womb)
	if err != nil {
		utxnLog.Errorf("Unable to move htlc output from "+
			"baby to kindergarten bucket: %v", err)
	}

	utxnLog.Infof("Htlc output %v promoted to "+
		"kindergarten", womb.OutPoint())
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

	err := u.store.PreschoolToKinder(kid)
	if err != nil {
		utxnLog.Errorf("Unable to move kid output "+
			"from preschool to kindergarten bucket: %v",
			err)
		return
	}

	utxnLog.Infof("Preschool output %v promoted to "+
		"kindergarten", kid.OutPoint())
}

func (u *utxoNursery) waitForGraduation(kgtnOutputs []kidOutput,
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

	for i := range kgtnOutputs {
		kid := &kgtnOutputs[i]

		// Now that the sweeping transaction has been broadcast, for
		// each of the immature outputs, we'll mark them as being fully
		// closed within the database.
		err := u.db.MarkChanFullyClosed(kid.OriginChanPoint())
		if err != nil {
			utxnLog.Errorf("Unable to mark channel %v as fully "+
				"closed: %v", kid.OriginChanPoint(), err)
			continue
		}

		utxnLog.Infof("Successfully marked channel %v as fully closed",
			kid.OriginChanPoint())
	}

	if err := u.store.AwardDiplomas(kgtnOutputs); err != nil {
		utxnLog.Errorf("Unable to award diplomas to %v"+
			"graduating output %v", len(kgtnOutputs))
		return
	}

	utxnLog.Infof("Awarded diplomas to %v graduating outputs",
		len(kgtnOutputs))
}

// deleteGraduatedOutputs removes outputs from the kindergarten database bucket
// when six blockchain confirmations have passed since the outputs were swept.
// We wait for six confirmations to ensure that the outputs will be swept if a
// chain reorganization occurs. This is the final step in the output incubation
// process.
func deleteGraduatedOutputs(db *channeldb.DB, deleteHeight uint32) error {
	return db.Update(func(tx *bolt.Tx) error {
		kgtnBucket := tx.Bucket(kindergartenBucket)
		if kgtnBucket == nil {
			return nil
		}

		heightBytes := make([]byte, 4)
		byteOrder.PutUint32(heightBytes, deleteHeight)
		results := kgtnBucket.Get(heightBytes)
		if results == nil {
			return nil
		}

		// Delete the row for this height within the kindergarten bucket.k
		if err := kgtnBucket.Delete(heightBytes); err != nil {
			return err
		}

		sweptOutputs, err := deserializeKidList(bytes.NewBuffer(results))
		if err != nil {
			return err
		}
		utxnLog.Infof("Deleting %v swept outputs from kindergarten bucket "+
			"at block height: %v", len(sweptOutputs), deleteHeight)

		// Additionally, for each output that has now been fully swept,
		// we'll also remove the index entry for that output.
		indexBucket := tx.Bucket(contractIndex)
		if indexBucket == nil {
			return nil
		}
		for _, sweptOutput := range sweptOutputs {
			var chanPoint bytes.Buffer
			err := writeOutpoint(&chanPoint, sweptOutput.OriginChanPoint())
			if err != nil {
				return err
			}

			if err := indexBucket.Delete(chanPoint.Bytes()); err != nil {
				return err
			}
		}

		return nil
	})
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

// deserializedKidList takes a sequence of serialized kid outputs and returns a
// slice of kidOutput structs.
func deserializeKidList(r io.Reader) ([]*kidOutput, error) {
	var kidOutputs []*kidOutput

	for {
		var kid = &kidOutput{}
		if err := kid.Decode(r); err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		kidOutputs = append(kidOutputs, kid)
	}

	return kidOutputs, nil
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
	kidOutput

	expiry    uint32
	timeoutTx *wire.MsgTx
}

// makeBabyOutput constructs baby output the wraps a future kidOutput. The
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

// Decode reconstructs a baby output using the provide io.Reader.
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
