package main

import (
	"bytes"
	"errors"

	"github.com/boltdb/bolt"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcd/wire"
)

//            NURSERY OUTPUT STATE TRANSITIONS
//
//   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─┐         ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─┐
//   | EnterPreschool() │         | CallBabysitter() │
//   └ ─ ─ ─ ─ ─ ─ ─ ─ ─┘         └ ─ ─ ─ ─ ─ ─ ─ ─ ─┘
//             │                            │
//             │                            │
//             ▼                            ▼
//   ┏━━━━━━━━━━━━━━━━━━┓         ┏━━━━━━━━━━━━━━━━━━┓
//   ┃ Preschool Bucket ┃         ┃    Baby Bucket   ┃
//   ┗━━━━━━━━━━━━━━━━━━┛         ┗━━━━━━━━━━━━━━━━━━┛
//             │                            │
//             │                            |
//             |                            ▼
//             |                           ╱ ╲
//             |                          ▕   ▏ Wait CLTV + PublishTxn
//             |                           ╲ ╱
//             │                            |
//             │                            │
//             ▼                            ▼
//            .─.                          .─.
//           (   )  waitForPromotion      (   )  waitForEnrollment
//            `─'                          `─'
//             │                            │
//             │                            │
//             ▼                            ▼
//   ┌─ ─ ─ ─ ─ ─ ─ ─ ─ ┐         ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─┐
//   | PromoteKinder()  |         |  EnrollKinder()  |
//   └─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         └ ─ ─ ─ ─ ─ ─ ─ ─ ─┘
//             │                            │
//             │                            │
//             │                            │
//             │                            │
//             │   ┏━━━━━━━━━━━━━━━━━━━━┓   │
//             └──▶┃   Kindrgtn Bucket  ┃◁──┘
//                 ┗━━━━━━━━━━━━━━━━━━━━┛
//                            │
//                            |
//                            ▼
//                           ╱ ╲
//                          ▕   ▏ Wait CSV + PublishTxn
//                           ╲ ╱
//                            │
//                            │
//                            ▼
//                           .─.
//                          (   )  waitForGraduation
//                           `─'
//                            │
//                            │
//                            ▼
//                  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
//                  |  AwardDiplomas()  |
//                  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘

// ; CHAIN SEGMENTATION
// ;
// ; The root directory for a nursery store is first bucketed by the chain hash
// ; with the 'utxon-' prefix. This allows multiple utxo nurseries for distinct
// ; chains to simultaneously use the same database. This is critical for using
// ; lnd in the multi-chain setting and to provide replay protection, since it
// ; is otherwise possible to have the same transaction id confirmed on
// ; different chains.
//
// utxon-<chain-hash>/
// |
// |   ; NURSERY-WIDE STATE VARIABLES
// |   ;
// |   ; Each nursery store tracks four distinct heights for both performance
// |   ; and reliable fault tolerance. The oldest and youngest height keys allow
// |   ; for efficient prefix scans of the height index. The last-attempted and
// |   ; last-finalized heights dictate a range of heights for which outstanding
// |   ; transactions have not been confirmed. During restarts, the utxo
// |   ; nursery should resume notifications for any outputs remaining within
// |   ; the height interval.
// |
// ├── oldest-height-key: <oldest-height>
// ├── youngest-height-key: <youngest-height>
// ├── last-attempted-height-key: <last-attempted-height>
// ├── last-finalized-height-key: <last-finalized-height>
// |
// |   ; CHANNEL INDEX
// |   ;
// |   ; The channel index contains a directory for each channel that has a
// |   ; non-zero number of outputs being tracked by the nursery store.
// |   ; Inside each channel directory are files contains serialized spendable
// |   ; outputs that are awaiting some state transition. The name of each file
// |   ; contains the outpoint of the spendable output in the file, and is
// |   ; prefixed with 4-byte state prefix, indicating whether the spendable
// |   ; output is a baby, preschool, or kindergarten output. Using state
// |   ; prefixes allows for more efficient queries about outputs in a
// |   ; particular state.
// |
// ├── channel-index-key/
// │   ├── <chain-point-1>/
// |   |   ├── <prefix>-<outpoint-1>: <spendable-output>
// |   |   └── <prefix>-<outpoint-2>: <spendable-output>
// │   ├── <chain-point-2>/
// |   |   └── <prefix>-<outpoint-3>: <spendable-output>
// │   └── <chain-point-3>/
// |       ├── <prefix>-<outpoint-4>: <spendable-output>
// |       └── <prefix>-<outpoint-5>: <spendable-output>
// |
// |   ; HEIGHT INDEX
// |   ;
// |   ; The height index contains a directory for each height at which it still
// |   ; has uncompleted actions. If an output is a baby or kindergarten output,
// |   ; it will have an associated entry in the height index. Inside a
// |   ; particular height directory, the structure is similar to that of the
// |   ; contract index, containing multiple channel directories, each
// |   ; containing files named with an outpoint belonging to the channel.
// |   ; Instead, however, the outpoint files contain the outpoint's state
// |   ; prefix, which is then concatenated,
// |   ;   e.g. <chan-point-3>/<prefix>-<outpoint-2>,
// |   ; to yield the relative file paths of each spendable output requiring
// |   ; attention the given height.
// |
// └── height-index-key/
//     ├── <height-1>/
//     |   └── <chan-point-3>/
//     |   |    ├── <prefix>-<outpoint-4>
//     |   |    └── <prefix>-<outpoint-5>
//     |   └── <chan-point-2>/
//     |        └── <prefix>-<outpoint-3>
//     └── <height-2>/
//         └── <chan-point-1>/
//              └── <prefix>-<outpoint-1>
//              └── <prefix>-<outpoint-2>

var (
	ErrBucketDoesNotExist = errors.New("bucket does not exist")
	ErrBucketNotEmpty     = errors.New("bucket is not empty, cannot be pruned")
)

var (
	youngestHeightKey      = []byte("youngest-height-key")
	oldestHeightKey        = []byte("oldest-height-key")
	lastAttemptedHeightKey = []byte("last-attempted-height-key")
	lastFinalizedHeightKey = []byte("last-finalized-height-key")

	babyPrefix = []byte("bby-")
	psclPrefix = []byte("pre-")
	kndrPrefix = []byte("kin-")

	// heightIndex maintains a persistent, 2-layer map to the baby or kid
	// outputs, grouped by channel point, that can be broadcast at a
	// particular height.
	// height -> chanPoint1 -> kidOutpoint
	//                      -> babyOutpoint
	//        -> chanPoint2 -> kidOutpoint
	//
	heightIndex = []byte("utxon-height-index")

	// channelIndex maintains a
	channelIndex = []byte("utxo-channel-index")
)

// NurseryStore facilitates the persistent data store for the utxo nursery.
// The nursery store is organized into a directory like structure with two
// primary components, namely the channel index and the height index.
type NurseryStore interface {
	LastFinalizedHeight() (uint32, error)
	LastAttemptedHeight() (uint32, error)
	//YoungestHeight() (uint32, error)
	//OldestHeight() (uint32, error)

	ConceiveOutput(*babyOutput) error
	FetchBirthingOutputs(height uint32) ([]babyOutput, error)
	BirthToKinder(*babyOutput) error

	EnterPreschool(*kidOutput) error
	ForEachPreschool(func(*kidOutput) error) error
	PreschoolToKinder(*kidOutput) error

	FetchGraduatingOutputs(height uint32) ([]kidOutput, error)
	AwardDiplomas([]kidOutput) error

	BeginCeremony(height uint32) error
	FinalizeCeremony(height uint32) error

	ForChanOutputs(*wire.OutPoint, func([]byte, []byte) error) error
}

type nurseryStore struct {
	chainHash chainhash.Hash
	db        *channeldb.DB
}

func newNurseryStore(db *channeldb.DB, chainHash *chainhash.Hash) *nurseryStore {
	return &nurseryStore{
		db:        db,
		chainHash: *chainHash,
	}
}

func (ns *nurseryStore) ConceiveOutput(baby *babyOutput) error {
	return ns.db.Update(func(tx *bolt.Tx) error {

		chanPoint := baby.OriginChanPoint()
		chanBucket, err := ns.createChannelBucket(tx, chanPoint)
		if err != nil {
			return err
		}

		hghtChanBucket, err := ns.createHeightChanBucket(tx,
			baby.expiry, chanPoint)
		if err != nil {
			return err
		}

		pfxOutputKey, err := prefixOutputKey(babyPrefix, baby.OutPoint())
		if err != nil {
			return err
		}

		var babyBuffer bytes.Buffer
		if err := baby.Encode(&babyBuffer); err != nil {
			return err
		}
		babyBytes := babyBuffer.Bytes()

		if err := chanBucket.Put(pfxOutputKey, babyBytes); err != nil {
			return err
		}

		_, err = hghtChanBucket.CreateBucketIfNotExists(pfxOutputKey)

		return err
	})
}

func (ns *nurseryStore) FetchBirthingOutputs(height uint32) ([]babyOutput, error) {
	var babies []babyOutput
	if err := ns.db.View(func(tx *bolt.Tx) error {

		hghtBucket := ns.getHeightBucket(tx, height)
		if hghtBucket == nil {
			return nil
		}

		var channelBuckets [][]byte
		if err := hghtBucket.ForEach(func(chanBytes, valBytes []byte) error {
			channelBuckets = append(channelBuckets, chanBytes)
			return nil
		}); err != nil {
			return err
		}

		chainBucket := tx.Bucket(ns.chainHash[:])
		if chainBucket == nil {
			return nil
		}

		chanIndex := chainBucket.Bucket(channelIndex)
		if chanIndex == nil {
			return nil
		}

		for _, chanBytes := range channelBuckets {
			hghtChanBucket := hghtBucket.Bucket(chanBytes)
			if hghtChanBucket == nil {
				continue
			}

			chanBucket := chanIndex.Bucket(chanBytes)
			if chanBucket == nil {
				continue
			}

			c := hghtChanBucket.Cursor()

			for k, _ := c.Seek(babyPrefix); k != nil &&
				bytes.HasPrefix(k, babyPrefix); k, _ = c.Next() {

				pfxOutputKey := k

				babyBytes := chanBucket.Get(pfxOutputKey)
				if babyBytes == nil {
					continue
				}

				var baby babyOutput
				err := baby.Decode(bytes.NewBuffer(babyBytes))
				if err != nil {
					return err
				}

				babies = append(babies, baby)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return babies, nil
}

// Graduate persists the most recently processed blockheight to the database.
// This blockheight is used during restarts to determine if blocks were missed
// while the UTXO Nursery was offline.
func (ns *nurseryStore) BeginCeremony(height uint32) error {
	return ns.db.Update(func(tx *bolt.Tx) error {
		return ns.putLastAttemptedHeight(tx, height)
	})
}
func (ns *nurseryStore) FinalizeCeremony(height uint32) error {
	return ns.db.Update(func(tx *bolt.Tx) error {
		return ns.putLastFinalizedHeight(tx, height)
	})
}

func (ns *nurseryStore) LastAttemptedHeight() (uint32, error) {
	var lastAttemptedHeight uint32
	err := ns.db.View(func(tx *bolt.Tx) error {
		lastHeight, err := ns.getLastAttemptedHeight(tx)
		if err != nil {
			return err
		}

		lastAttemptedHeight = lastHeight

		return nil
	})
	return lastAttemptedHeight, err
}

func (ns *nurseryStore) getLastAttemptedHeight(tx *bolt.Tx) (uint32, error) {
	chainBucket := tx.Bucket(ns.chainHash[:])
	if chainBucket == nil {
		return 0, nil
	}

	heightBytes := chainBucket.Get(lastAttemptedHeightKey)
	if len(heightBytes) != 4 {
		return 0, nil
	}

	return byteOrder.Uint32(heightBytes), nil
}

func (ns *nurseryStore) putLastAttemptedHeight(tx *bolt.Tx, height uint32) error {
	chainBucket, err := tx.CreateBucketIfNotExists(ns.chainHash[:])
	if err != nil {
		return err
	}

	var lastHeightBytes [4]byte
	byteOrder.PutUint32(lastHeightBytes[:], height)

	return chainBucket.Put(lastAttemptedHeightKey, lastHeightBytes[:])
}

func (ns *nurseryStore) LastFinalizedHeight() (uint32, error) {
	var lastFinalizedHeight uint32
	err := ns.db.View(func(tx *bolt.Tx) error {
		lastHeight, err := ns.getLastFinalizedHeight(tx)
		if err != nil {
			return err
		}

		lastFinalizedHeight = lastHeight

		return nil
	})
	return lastFinalizedHeight, err
}

func (ns *nurseryStore) getLastFinalizedHeight(tx *bolt.Tx) (uint32, error) {
	chainBucket := tx.Bucket(ns.chainHash[:])
	if chainBucket == nil {
		return 0, nil
	}

	heightBytes := chainBucket.Get(lastFinalizedHeightKey)
	if len(heightBytes) != 4 {
		return 0, nil
	}

	return byteOrder.Uint32(heightBytes), nil
}

func (ns *nurseryStore) putLastFinalizedHeight(tx *bolt.Tx, height uint32) error {
	chainBucket, err := tx.CreateBucketIfNotExists(ns.chainHash[:])
	if err != nil {
		return err
	}

	var lastHeightBytes [4]byte
	byteOrder.PutUint32(lastHeightBytes[:], height)

	return chainBucket.Put(lastFinalizedHeightKey, lastHeightBytes[:])
}

func (ns *nurseryStore) BirthToKinder(baby *babyOutput) error {
	return ns.db.Update(func(tx *bolt.Tx) error {

		chanPoint := baby.OriginChanPoint()
		chanBucket, err := ns.createChannelBucket(tx, chanPoint)
		if err != nil {
			return err
		}

		pfxOutputKey, err := prefixOutputKey(babyPrefix, baby.OutPoint())
		if err != nil {
			return err
		}

		if err := chanBucket.Delete(pfxOutputKey); err != nil {
			return err
		}

		hghtChanBucketOld, err := ns.createHeightChanBucket(tx,
			baby.expiry, chanPoint)
		if err != nil {
			return err
		}

		err = hghtChanBucketOld.DeleteBucket(pfxOutputKey)
		if err != nil {
			return err
		}

		copy(pfxOutputKey, kndrPrefix)

		var kidBuffer bytes.Buffer
		if err := baby.kidOutput.Encode(&kidBuffer); err != nil {
			return err
		}
		kidBytes := kidBuffer.Bytes()

		if err := chanBucket.Put(pfxOutputKey, kidBytes); err != nil {
			return err
		}

		maturityHeight := baby.ConfHeight() + baby.BlocksToMaturity()
		hghtChanBucketNew, err := ns.createHeightChanBucket(tx,
			maturityHeight, chanPoint)
		if err != nil {
			return err
		}

		_, err = hghtChanBucketNew.CreateBucketIfNotExists(pfxOutputKey)
		if err != nil {
			return err
		}

		err = ns.pruneHeight(tx, baby.expiry)
		switch err {
		case nil, ErrBucketDoesNotExist:
			return nil
		case ErrBucketNotEmpty:
			return nil
		default:
			return err
		}
	})
}

// EnterPreschool is the first stage in the process of transferring funds from
// a force closed channel into the user's wallet. When an output is in the
// "preschool" stage, the daemon is waiting for the initial confirmation of the
// commitment transaction.
func (ns *nurseryStore) EnterPreschool(kid *kidOutput) error {
	return ns.db.Update(func(tx *bolt.Tx) error {

		chanPoint := kid.OriginChanPoint()
		chanBucket, err := ns.createChannelBucket(tx, chanPoint)
		if err != nil {
			return err
		}

		pfxOutputKey, err := prefixOutputKey(psclPrefix, kid.OutPoint())
		if err != nil {
			return err
		}

		var kidBuffer bytes.Buffer
		if err := kid.Encode(&kidBuffer); err != nil {
			return err
		}
		kidBytes := kidBuffer.Bytes()

		return chanBucket.Put(pfxOutputKey, kidBytes)
	})
}

func (ns *nurseryStore) PreschoolToKinder(kid *kidOutput) error {
	return ns.db.Update(func(tx *bolt.Tx) error {

		chanPoint := kid.OriginChanPoint()
		chanBucket, err := ns.createChannelBucket(tx, chanPoint)
		if err != nil {
			return err
		}

		pfxOutputKey, err := prefixOutputKey(psclPrefix, kid.OutPoint())
		if err != nil {
			return err
		}

		if err := chanBucket.Delete(pfxOutputKey); err != nil {
			return err
		}

		copy(pfxOutputKey, kndrPrefix)

		var kidBuffer bytes.Buffer
		if err := kid.Encode(&kidBuffer); err != nil {
			return err
		}
		kidBytes := kidBuffer.Bytes()

		if err := chanBucket.Put(pfxOutputKey, kidBytes); err != nil {
			return err
		}

		maturityHeight := kid.ConfHeight() + kid.BlocksToMaturity()
		hghtChanBucket, err := ns.createHeightChanBucket(tx,
			maturityHeight, chanPoint)
		if err != nil {
			return err
		}

		_, err = hghtChanBucket.CreateBucketIfNotExists(pfxOutputKey)

		return err
	})
}

// FetchGraduatingOutputs checks the "kindergarten" database bucket whenever a
// new block is received in order to determine if commitment transaction
// outputs have become newly spendable. If fetchGraduatingOutputs finds outputs
// that are ready for "graduation," it passes them on to be swept.  This is the
// third step in the output incubation process.
func (ns *nurseryStore) FetchGraduatingOutputs(height uint32) ([]kidOutput, error) {
	var kids []kidOutput
	if err := ns.db.View(func(tx *bolt.Tx) error {

		hghtBucket := ns.getHeightBucket(tx, height)
		if hghtBucket == nil {
			return nil
		}

		var channelBuckets [][]byte
		if err := hghtBucket.ForEach(func(chanBytes, valBytes []byte) error {
			channelBuckets = append(channelBuckets, chanBytes)
			return nil
		}); err != nil {
			return err
		}

		chainBucket := tx.Bucket(ns.chainHash[:])
		if chainBucket == nil {
			return nil
		}

		chanIndex := chainBucket.Bucket(channelIndex)
		if chanIndex == nil {
			return nil
		}

		for _, chanBytes := range channelBuckets {
			hghtChanBucket := hghtBucket.Bucket(chanBytes)
			if hghtChanBucket == nil {
				continue
			}

			chanBucket := chanIndex.Bucket(chanBytes)
			if chanBucket == nil {
				continue
			}

			c := hghtChanBucket.Cursor()

			for k, _ := c.Seek(kndrPrefix); k != nil &&
				bytes.HasPrefix(k, kndrPrefix); k, _ = c.Next() {

				pfxOutputKey := k

				kidBytes := chanBucket.Get(pfxOutputKey)
				if kidBytes == nil {
					continue
				}

				var kid kidOutput
				err := kid.Decode(bytes.NewBuffer(kidBytes))
				if err != nil {
					return err
				}

				kids = append(kids, kid)
			}
		}

		return nil

	}); err != nil {
		return nil, err
	}

	return kids, nil
}

func (ns *nurseryStore) AwardDiplomas(kids []kidOutput) error {
	return ns.db.Update(func(tx *bolt.Tx) error {
		for _, kid := range kids {
			err := ns.pruneHeight(tx, kid.ConfHeight())
			switch err {
			case nil, ErrBucketDoesNotExist, ErrBucketNotEmpty:
				continue
			default:
				return err
			}

			err = ns.pruneChannel(tx, kid.OriginChanPoint())
			switch err {
			case nil, ErrBucketDoesNotExist, ErrBucketNotEmpty:
				continue
			default:
				return err
			}
		}

		return nil
	})
}

func (ns *nurseryStore) ForChanOutputs(chanPoint *wire.OutPoint,
	cb func([]byte, []byte) error) error {

	return ns.db.View(func(tx *bolt.Tx) error {
		chanBucket := ns.getChannelBucket(tx, chanPoint)
		if chanBucket == nil {
			return ErrContractNotFound
		}

		return chanBucket.ForEach(cb)
	})
}

func (ns *nurseryStore) ForEachPreschool(cb func(kid *kidOutput) error) error {
	return ns.db.View(func(tx *bolt.Tx) error {
		chainBucket := tx.Bucket(ns.chainHash[:])
		if chainBucket == nil {
			return nil
		}

		chanIndex := chainBucket.Bucket(channelIndex)
		if chanIndex == nil {
			return nil
		}

		var channelBuckets [][]byte
		if err := chanIndex.ForEach(func(chanBytes, valBytes []byte) error {
			channelBuckets = append(channelBuckets, chanBytes)
			return nil
		}); err != nil {
			return err
		}

		for _, chanBytes := range channelBuckets {
			chanBucket := chanIndex.Bucket(chanBytes)
			if chanBucket == nil {
				continue
			}

			c := chanBucket.Cursor()

			for k, v := c.Seek(psclPrefix); k != nil &&
				bytes.HasPrefix(k, psclPrefix); k, v = c.Next() {

				var psclOutput kidOutput
				psclReader := bytes.NewReader(v)
				err := psclOutput.Decode(psclReader)
				if err != nil {
					return err
				}

				if err := cb(&psclOutput); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (ns *nurseryStore) createChannelBucket(tx *bolt.Tx,
	chanPoint *wire.OutPoint) (*bolt.Bucket, error) {

	chainBucket, err := tx.CreateBucketIfNotExists(ns.chainHash[:])
	if err != nil {
		return nil, err
	}

	chanIndex, err := chainBucket.CreateBucketIfNotExists(channelIndex)
	if err != nil {
		return nil, err
	}

	var chanBuffer bytes.Buffer
	if err := writeOutpoint(&chanBuffer, chanPoint); err != nil {
		return nil, err
	}

	return chanIndex.CreateBucketIfNotExists(chanBuffer.Bytes())
}

func (ns *nurseryStore) getChannelBucket(tx *bolt.Tx,
	chanPoint *wire.OutPoint) *bolt.Bucket {

	chainBucket := tx.Bucket(ns.chainHash[:])
	if chainBucket == nil {
		return nil
	}

	chanIndex := chainBucket.Bucket(channelIndex)
	if chanIndex == nil {
		return nil
	}

	var chanBuffer bytes.Buffer
	if err := writeOutpoint(&chanBuffer, chanPoint); err != nil {
		return nil
	}

	return chanIndex.Bucket(chanBuffer.Bytes())
}

func (ns *nurseryStore) pruneChannel(tx *bolt.Tx,
	chanPoint *wire.OutPoint) error {

	chainBucket := tx.Bucket(ns.chainHash[:])
	if chainBucket == nil {
		return nil
	}

	chanIndex := chainBucket.Bucket(channelIndex)
	if chanIndex == nil {
		return nil
	}

	var chanBuffer bytes.Buffer
	if err := writeOutpoint(&chanBuffer, chanPoint); err != nil {
		return err
	}

	return ns.removeBucketIfEmpty(chanIndex, chanBuffer.Bytes())
}

func (ns *nurseryStore) createHeightBucket(tx *bolt.Tx,
	height uint32) (*bolt.Bucket, error) {

	chainBucket, err := tx.CreateBucketIfNotExists(ns.chainHash[:])
	if err != nil {
		return nil, err
	}

	hghtIndex, err := chainBucket.CreateBucketIfNotExists(heightIndex)
	if err != nil {
		return nil, err
	}

	var heightBytes [4]byte
	byteOrder.PutUint32(heightBytes[:], height)

	return hghtIndex.CreateBucketIfNotExists(heightBytes[:])
}

func (ns *nurseryStore) getHeightBucket(tx *bolt.Tx,
	height uint32) *bolt.Bucket {

	chainBucket := tx.Bucket(ns.chainHash[:])
	if chainBucket == nil {
		return nil
	}

	hghtIndex := chainBucket.Bucket(heightIndex)
	if hghtIndex == nil {
		return nil
	}

	var heightBytes [4]byte
	byteOrder.PutUint32(heightBytes[:], height)

	return hghtIndex.Bucket(heightBytes[:])
}

func (ns *nurseryStore) createHeightChanBucket(tx *bolt.Tx,
	height uint32, chanPoint *wire.OutPoint) (*bolt.Bucket, error) {

	hghtBucket, err := ns.createHeightBucket(tx, height)
	if err != nil {
		return nil, err
	}

	var chanBuffer bytes.Buffer
	if err := writeOutpoint(&chanBuffer, chanPoint); err != nil {
		return nil, err
	}
	chanBytes := chanBuffer.Bytes()

	return hghtBucket.CreateBucketIfNotExists(chanBytes)
}

func (ns *nurseryStore) pruneHeight(tx *bolt.Tx, height uint32) error {

	chainBucket := tx.Bucket(ns.chainHash[:])
	if chainBucket == nil {
		return nil
	}

	hghtIndex := chainBucket.Bucket(heightIndex)
	if hghtIndex == nil {
		return nil
	}

	var heightBytes [4]byte
	byteOrder.PutUint32(heightBytes[:], height)

	hghtBucket := hghtIndex.Bucket(heightBytes[:])
	if hghtBucket == nil {
		return ErrBucketDoesNotExist
	}

	var nActiveBuckets int
	if err := hghtBucket.ForEach(func(chanBytes, _ []byte) error {
		err := ns.removeBucketIfEmpty(hghtBucket, chanBytes)
		switch err {
		case nil, ErrBucketDoesNotExist:
			return nil
		case ErrBucketNotEmpty:
			nActiveBuckets++
			return nil
		default:
			return err
		}
	}); err != nil {
		return err
	}

	if nActiveBuckets > 0 {
		return ErrBucketNotEmpty
	}

	return ns.removeBucketIfEmpty(hghtIndex, heightBytes[:])
}

func (ns *nurseryStore) removeBucketIfEmpty(parent *bolt.Bucket,
	bktName []byte) error {

	bkt := parent.Bucket(bktName)
	if bkt == nil {
		return ErrBucketDoesNotExist
	}

	nChildren, err := ns.numChildrenInBucket(bkt)
	if err != nil {
		return err
	}

	if nChildren > 0 {
		return ErrBucketNotEmpty
	}

	return parent.DeleteBucket(bktName)
}

func (ns *nurseryStore) numChildrenInBucket(parent *bolt.Bucket) (int, error) {
	var nChildren int
	if err := parent.ForEach(func(_, _ []byte) error {
		nChildren++
		return nil
	}); err != nil {
		return 0, err
	}

	return nChildren, nil
}

func prefixOutputKey(prefix []byte, outpoint *wire.OutPoint) ([]byte, error) {
	var pfxdOutputBuffer bytes.Buffer
	if _, err := pfxdOutputBuffer.Write(prefix); err != nil {
		return nil, err
	}

	err := writeOutpoint(&pfxdOutputBuffer, outpoint)
	if err != nil {
		return nil, err
	}

	return pfxdOutputBuffer.Bytes(), nil
}
