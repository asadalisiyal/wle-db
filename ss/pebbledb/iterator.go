package pebbledb

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/sei-protocol/sei-db/ss/types"
	"golang.org/x/exp/slices"
)

var _ types.DBIterator = (*iterator)(nil)

// iterator implements the Iterator interface. It wraps a PebbleDB iterator
// with added MVCC key handling logic. The iterator will iterate over the key space
// in the provided domain for a given version. If a key has been written at the
// provided version, that key/value pair will be iterated over. Otherwise, the
// latest version for that key/value pair will be iterated over s.t. it's less
// than the provided version. Note:
//
// - The start key must not be empty.
// - Currently, reverse iteration is NOT supported.
type iterator struct {
	source             *pebble.Iterator
	prefix, start, end []byte
	version            int64
	valid              bool
	reverse            bool
}

func newPebbleDBIterator(src *pebble.Iterator, prefix, mvccStart, mvccEnd []byte, version int64, earliestVersion int64, reverse bool) *iterator {
	// Return invalid iterator if requested iterator height is lower than earliest version after pruning
	if version < earliestVersion {
		return &iterator{
			source:  src,
			prefix:  prefix,
			start:   mvccStart,
			end:     mvccEnd,
			version: version,
			valid:   false,
			reverse: reverse,
		}
	}

	fmt.Printf("DEBUG - newPebbleDBIterator - prefix %s mvccStart %X mvccEnd %s mvccStartString %s mvccEndString %s version %d reverse %+v\n", string(prefix), mvccStart, mvccEnd, string(mvccStart), string(mvccEnd), version, reverse)
	pc := make([]uintptr, 10)   // at most 10 callers
	n := runtime.Callers(2, pc) // skip Callers and newPebbleDBIterator itself
	if n == 0 {
		fmt.Println("DEBUG - No callers found")
	} else {
		frames := runtime.CallersFrames(pc[:n])
		for {
			frame, more := frames.Next()
			fmt.Printf("DEBUG - newPebbleDBIterator - Called from %s, function %s, line %d\n", frame.File, frame.Function, frame.Line)
			if !more {
				break
			}
		}
	}

	// move the underlying PebbleDB iterator to the first key
	var valid bool
	if reverse {
		valid = src.Last()
	} else {
		valid = src.First()
	}

	itr := &iterator{
		source:  src,
		prefix:  prefix,
		start:   mvccStart,
		end:     mvccEnd,
		version: version,
		valid:   valid,
		reverse: reverse,
	}

	if valid {
		_, currKeyVersion, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC value.
			panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Key()))
		}

		curKeyVersionDecoded, err := decodeUint64Ascending(currKeyVersion)
		if err != nil {
			panic(err)
		}

		// Edge case: the first key has multiple versions so we need to do a seek
		if curKeyVersionDecoded > itr.version {
			itr.Next()
		}
	}

	return itr
}

// Domain returns the domain of the iterator. The caller must not modify the
// return values.
func (itr *iterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *iterator) Key() []byte {
	itr.assertIsValid()

	key, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC key.
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	keyCopy := slices.Clone(key)
	return keyCopy[len(itr.prefix):]
}

func (itr *iterator) Value() []byte {
	itr.assertIsValid()

	val, _, ok := SplitMVCCKey(itr.source.Value())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC value.
		panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Key()))
	}

	return slices.Clone(val)
}

func (itr *iterator) NextForward() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC key.
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	fmt.Printf("currKey %s\n", string(currKey))

	next := itr.source.NextPrefix()

	// First move the iterator to the next prefix, which may not correspond to the
	// desired version for that key, e.g. if the key was written at a later version,
	// so we seek back to the latest desired version, s.t. the version is <= itr.version.
	if next {
		nextKey, _, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}
		if !bytes.HasPrefix(nextKey, itr.prefix) {
			// the next key must have itr.prefix as the prefix
			itr.valid = false
			return
		}

		fmt.Printf("nextKey %s\n", string(nextKey))

		// Move the iterator to the closest version to the desired version, so we
		// append the current iterator key to the prefix and seek to that key.
		itr.valid = itr.source.SeekLT(MVCCEncode(nextKey, itr.version+1))

		tmpKey, tmpKeyVersion, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}

		fmt.Printf("tmpKey %s\n", string(tmpKey))

		// There exists cases where the SeekLT() call moved us back to the same key
		// we started at, so we must move to next key, i.e. two keys forward.
		if bytes.Equal(tmpKey, currKey) {
			fmt.Printf("recursive loop back")
			if itr.source.NextPrefix() {
				itr.NextForward()

				tmpKey, tmpKeyVersion, ok = SplitMVCCKey(itr.source.Key())
				if !ok {
					// XXX: This should not happen as that would indicate we have a malformed
					// MVCC key.
					itr.valid = false
					return
				}

			} else {
				itr.valid = false
				return
			}
		}

		tmpKeyVersionDecoded, err := decodeUint64Ascending(tmpKeyVersion)
		if err != nil {
			panic(err)
		}

		fmt.Printf("after - tmpKey %s tmpKeyVersionDecoded %d\n", string(tmpKey), tmpKeyVersionDecoded)

		if tmpKeyVersionDecoded > itr.version {
			fmt.Printf("recursive greater version")
			itr.NextForward()
		}

		// The cursor might now be pointing at a key/value pair that is tombstoned.
		// If so, we must move the cursor.
		if itr.valid && itr.cursorTombstoned() {
			itr.NextForward()
		}

		return
	}

	itr.valid = false
}

func (itr *iterator) NextReverse() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC key.
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	next := itr.source.SeekLT(MVCCEncode(currKey, 0))

	// First move the iterator to the next prefix, which may not correspond to the
	// desired version for that key, e.g. if the key was written at a later version,
	// so we seek back to the latest desired version, s.t. the version is <= itr.version.
	if next {
		nextKey, _, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}
		if !bytes.HasPrefix(nextKey, itr.prefix) {
			// the next key must have itr.prefix as the prefix
			itr.valid = false
			return
		}

		// Move the iterator to the closest version to the desired version, so we
		// append the current iterator key to the prefix and seek to that key.
		itr.valid = itr.source.SeekLT(MVCCEncode(nextKey, itr.version+1))

		_, tmpKeyVersion, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}

		tmpKeyVersionDecoded, err := decodeUint64Ascending(tmpKeyVersion)
		if err != nil {
			panic(err)
		}

		if tmpKeyVersionDecoded > itr.version {
			itr.NextReverse()
		}

		// The cursor might now be pointing at a key/value pair that is tombstoned.
		// If so, we must move the cursor.
		if itr.valid && itr.cursorTombstoned() {
			itr.NextReverse()
		}

		return
	}

	itr.valid = false
}

func (itr *iterator) Next() {
	if itr.reverse {
		itr.NextReverse()
	} else {
		itr.NextForward()
	}
}

func (itr *iterator) Valid() bool {
	// once invalid, forever invalid
	if !itr.valid || !itr.source.Valid() {
		itr.valid = false
		return itr.valid
	}

	// if source has error, consider it invalid
	if err := itr.source.Error(); err != nil {
		itr.valid = false
		return itr.valid
	}

	// if key is at the end or past it, consider it invalid
	if end := itr.end; end != nil {
		if bytes.Compare(end, itr.Key()) <= 0 {
			itr.valid = false
			return itr.valid
		}
	}

	return true
}

func (itr *iterator) Error() error {
	return itr.source.Error()
}

func (itr *iterator) Close() error {
	_ = itr.source.Close()
	itr.source = nil
	itr.valid = false
	return nil
}

func (itr *iterator) assertIsValid() {
	if !itr.valid {
		panic("iterator is invalid")
	}
}

// cursorTombstoned checks if the current cursor is pointing at a key/value pair
// that is tombstoned. If the cursor is tombstoned, <true> is returned, otherwise
// <false> is returned. In the case where the iterator is valid but the key/value
// pair is tombstoned, the caller should call Next(). Note, this method assumes
// the caller assures the iterator is valid first!
func (itr *iterator) cursorTombstoned() bool {
	_, tombBz, ok := SplitMVCCKey(itr.source.Value())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC value.
		panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Key()))
	}

	// If the tombstone suffix is empty, we consider this a zero value and thus it
	// is not tombstoned.
	if len(tombBz) == 0 {
		return false
	}

	// If the tombstone suffix is non-empty and greater than the target version,
	// the value is not tombstoned.
	tombstone, err := decodeUint64Ascending(tombBz)
	if err != nil {
		panic(fmt.Errorf("failed to decode value tombstone: %w", err))
	}
	if tombstone > itr.version {
		return false
	}

	return true
}

func (itr *iterator) DebugRawIterate() {
	valid := itr.source.Valid()
	if valid {
		// The first key may not represent the desired target version, so move the
		// cursor to the correct location.
		firstKey, _, _ := SplitMVCCKey(itr.source.Key())
		valid = itr.source.SeekLT(MVCCEncode(firstKey, itr.version+1))
	}

	for valid {
		key, vBz, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
		}

		version, err := decodeUint64Ascending(vBz)
		if err != nil {
			panic(fmt.Errorf("failed to decode key version: %w", err))
		}

		val, tombBz, ok := SplitMVCCKey(itr.source.Value())
		if !ok {
			panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Value()))
		}

		var tombstone int64
		if len(tombBz) > 0 {
			tombstone, err = decodeUint64Ascending(vBz)
			if err != nil {
				panic(fmt.Errorf("failed to decode value tombstone: %w", err))
			}
		}

		fmt.Printf("KEY: %s, VALUE: %s, VERSION: %d, TOMBSTONE: %d\n", key, val, version, tombstone)

		var next bool
		if itr.reverse {
			next = itr.source.SeekLT(MVCCEncode(key, 0))
		} else {
			next = itr.source.NextPrefix()
		}

		if next {
			nextKey, _, ok := SplitMVCCKey(itr.source.Key())
			if !ok {
				panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
			}

			// the next key must have itr.prefix as the prefix
			if !bytes.HasPrefix(nextKey, itr.prefix) {
				valid = false
			} else {
				valid = itr.source.SeekLT(MVCCEncode(nextKey, itr.version+1))
			}
		} else {
			valid = false
		}
	}
}
