package memiavl

import (
	"context"
	"errors"
	"testing"

	"bytes"
	"os"
	"path/filepath"

	errorutils "github.com/sei-protocol/sei-db/common/errors"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/sc/types"
	"github.com/stretchr/testify/require"
)

func TestSnapshotEncodingRoundTrip(t *testing.T) {
	// setup test tree with compression enabled
	dbDir := t.TempDir()
	db, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:                 dbDir,
		CreateIfMissing:     true,
		InitialStores:       []string{"test"},
		SnapshotCompression: true,
	})
	require.NoError(t, err)
	tree := db.TreeByName("test")
	for _, changes := range ChangeSets[:len(ChangeSets)-1] {
		tree.ApplyChangeSet(changes)
		_, _, err := tree.SaveVersion(true)
		require.NoError(t, err)
	}

	snapshotDir := t.TempDir()
	require.NoError(t, tree.WriteSnapshot(context.Background(), snapshotDir, true))

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	require.Equal(t, uint8(1), snapshot.compression) // verify compression is enabled

	tree2 := NewFromSnapshot(snapshot, true, 0)

	require.Equal(t, tree.Version(), tree2.Version())
	require.Equal(t, tree.RootHash(), tree2.RootHash())

	// verify all the node hashes in snapshot
	for i := 0; i < snapshot.nodesLen(); i++ {
		node := snapshot.Node(uint32(i))
		require.Equal(t, node.Hash(), HashNode(node))
	}

	require.NoError(t, snapshot.Close())

	// test modify tree loaded from snapshot
	snapshot, err = OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	tree3 := NewFromSnapshot(snapshot, true, 0)
	tree3.ApplyChangeSet(ChangeSets[len(ChangeSets)-1])
	hash, v, err := tree3.SaveVersion(true)
	require.NoError(t, err)
	require.Equal(t, RefHashes[len(ChangeSets)-1], hash)
	require.Equal(t, len(ChangeSets), int(v))
	require.NoError(t, snapshot.Close())
}

func TestSnapshotExport(t *testing.T) {
	expNodes := []*types.SnapshotNode{
		{Key: []byte("hello"), Value: []byte("world1"), Version: 2, Height: 0},
		{Key: []byte("hello1"), Value: []byte("world1"), Version: 2, Height: 0},
		{Key: []byte("hello1"), Value: nil, Version: 3, Height: 1},
		{Key: []byte("hello2"), Value: []byte("world1"), Version: 3, Height: 0},
		{Key: []byte("hello3"), Value: []byte("world1"), Version: 3, Height: 0},
		{Key: []byte("hello3"), Value: nil, Version: 3, Height: 1},
		{Key: []byte("hello2"), Value: nil, Version: 3, Height: 2},
	}

	// setup test tree
	tree := New(0)
	for _, changes := range ChangeSets[:3] {
		tree.ApplyChangeSet(changes)
		_, _, err := tree.SaveVersion(true)
		require.NoError(t, err)
	}

	snapshotDir := t.TempDir()
	require.NoError(t, tree.WriteSnapshot(context.Background(), snapshotDir, false))

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)

	var nodes []*types.SnapshotNode
	exporter := snapshot.Export()
	for {
		node, err := exporter.Next()
		if errors.Is(err, errorutils.ErrorExportDone) {
			break
		}
		require.NoError(t, err)
		nodes = append(nodes, node)
	}

	require.Equal(t, expNodes, nodes)
}

func TestSnapshotImportExport(t *testing.T) {
	// setup test tree
	tree := New(0)
	for _, changes := range ChangeSets {
		tree.ApplyChangeSet(changes)
		_, _, err := tree.SaveVersion(true)
		require.NoError(t, err)
	}

	snapshotDir := t.TempDir()
	require.NoError(t, tree.WriteSnapshot(context.Background(), snapshotDir, false))
	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)

	ch := make(chan *types.SnapshotNode)

	go func() {
		defer close(ch)

		exporter := snapshot.Export()
		for {
			node, err := exporter.Next()
			if err == errorutils.ErrorExportDone {
				break
			}
			require.NoError(t, err)
			ch <- node
		}
	}()

	snapshotDir2 := t.TempDir()
	err = doImport(snapshotDir2, tree.Version(), ch)
	require.NoError(t, err)

	snapshot2, err := OpenSnapshot(snapshotDir2)
	require.NoError(t, err)
	require.Equal(t, snapshot.RootNode().Hash(), snapshot2.RootNode().Hash())

	// verify all the node hashes in snapshot
	for i := 0; i < snapshot2.nodesLen(); i++ {
		node := snapshot2.Node(uint32(i))
		require.Equal(t, node.Hash(), HashNode(node))
	}
}

func TestDBSnapshotRestore(t *testing.T) {
	db, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:               t.TempDir(),
		CreateIfMissing:   true,
		InitialStores:     []string{"test", "test2"},
		AsyncCommitBuffer: -1,
	})
	require.NoError(t, err)

	for _, changes := range ChangeSets {
		cs := []*proto.NamedChangeSet{
			{
				Name:      "test",
				Changeset: changes,
			},
			{
				Name:      "test2",
				Changeset: changes,
			},
		}
		require.NoError(t, db.ApplyChangeSets(cs))
		_, err := db.Commit()
		require.NoError(t, err)
		testSnapshotRoundTrip(t, db)
	}

	require.NoError(t, db.RewriteSnapshot(context.Background()))
	require.NoError(t, db.Reload())
	require.Equal(t, len(ChangeSets), int(db.metadata.CommitInfo.Version))
	testSnapshotRoundTrip(t, db)
}

func testSnapshotRoundTrip(t *testing.T, db *DB) {
	exporter, err := NewMultiTreeExporter(db.dir, uint32(db.Version()), false)
	require.NoError(t, err)

	restoreDir := t.TempDir()
	importer, err := NewMultiTreeImporter(restoreDir, uint64(db.Version()))
	require.NoError(t, err)

	for {
		item, err := exporter.Next()
		if err == errorutils.ErrorExportDone {
			break
		}
		require.NoError(t, err)
		require.NoError(t, importer.Add(item))
	}

	require.NoError(t, importer.Close())
	require.NoError(t, exporter.Close())

	db2, err := OpenDB(logger.NewNopLogger(), 0, Options{Dir: restoreDir})
	require.NoError(t, err)
	require.Equal(t, db.LastCommitInfo(), db2.LastCommitInfo())

	// the imported db function normally
	_, err = db2.Commit()
	require.NoError(t, err)
}

func TestSnapshotGzipCompression(t *testing.T) {
	tree := New(0)
	// Add some compressible and incompressible data
	compressible := bytes.Repeat([]byte("A"), 1024)
	incompressible := make([]byte, 1024)
	for i := range incompressible {
		incompressible[i] = byte(i % 256)
	}
	testData := []struct {
		key, value []byte
	}{
		{[]byte("foo"), []byte("bar")},
		{[]byte("hello"), []byte("world")},
		{[]byte("compressible"), compressible},
		{[]byte("incompressible"), incompressible},
	}
	for _, kv := range testData {
		tree.Set(kv.key, kv.value)
	}
	_, _, err := tree.SaveVersion(true)
	require.NoError(t, err)

	snapshotDir := t.TempDir()
	require.NoError(t, tree.WriteSnapshot(context.Background(), snapshotDir, true))

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	defer snapshot.Close()

	// Check that compression flag is set to gzip
	require.Equal(t, uint8(1), snapshot.compression)

	// Check that all key/value pairs are restored correctly
	for _, kv := range testData {
		val, _ := snapshot.RootNode().Get(kv.key)
		require.NotNil(t, val, "key %s not found", kv.key)
		require.Equal(t, kv.value, val, "value mismatch for key %s", kv.key)
	}
}

func TestOpenOldSnapshotWithoutCompressionFlag(t *testing.T) {
	tree := New(0)
	tree.Set([]byte("foo"), []byte("bar"))
	_, _, err := tree.SaveVersion(true)
	require.NoError(t, err)

	snapshotDir := t.TempDir()
	require.NoError(t, tree.WriteSnapshot(context.Background(), snapshotDir, false))

	// Overwrite metadata file to simulate old snapshot (12 bytes, no compression flag)
	metadataFile := filepath.Join(snapshotDir, "metadata")
	metadata, err := os.ReadFile(metadataFile)
	require.NoError(t, err)
	require.Equal(t, 13, len(metadata))
	metadata = metadata[:12] // truncate to 12 bytes
	require.NoError(t, os.WriteFile(metadataFile, metadata, 0o600))

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	require.Equal(t, uint8(0), snapshot.compression) // should be uncompressed
	_, val := snapshot.RootNode().Get([]byte("foo"))
	require.Equal(t, []byte("bar"), val)
	require.NoError(t, snapshot.Close())
}
