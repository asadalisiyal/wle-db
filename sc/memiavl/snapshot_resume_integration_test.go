package memiavl

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/stretchr/testify/require"
)

// MockSnapshotWriter simulates a snapshot writer that can be interrupted
type MockSnapshotWriter struct {
	*snapshotWriter
	interruptAfter  int // Number of nodes to write before interrupting
	nodesWritten    int
	shouldInterrupt bool
}

func newMockSnapshotWriter(ctx context.Context, nodesWriter, leavesWriter, kvsWriter interface{}, interruptAfter int) *MockSnapshotWriter {
	return &MockSnapshotWriter{
		snapshotWriter:  newSnapshotWriter(ctx, nodesWriter.(*bufio.Writer), leavesWriter.(*bufio.Writer), kvsWriter.(*bufio.Writer)),
		interruptAfter:  interruptAfter,
		shouldInterrupt: true,
	}
}

func (w *MockSnapshotWriter) writeRecursive(node Node) error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	if node.IsLeaf() {
		// Check if we should interrupt before writing leaf
		if w.shouldInterrupt && w.nodesWritten >= w.interruptAfter {
			return fmt.Errorf("simulated interruption at node %d", w.nodesWritten)
		}
		w.nodesWritten++
		return w.writeLeaf(node.Version(), node.Key(), node.Value(), node.Hash())
	}

	// record the number of pending subtrees before the current one,
	// it's always positive and won't exceed the tree height, so we can use an uint8 to store it.
	preTrees := uint8(w.leafCounter - w.branchCounter)

	if err := w.writeRecursive(node.Left()); err != nil {
		return err
	}
	keyLeaf := w.leafCounter
	if err := w.writeRecursive(node.Right()); err != nil {
		return err
	}

	// Check if we should interrupt before writing branch
	if w.shouldInterrupt && w.nodesWritten >= w.interruptAfter {
		return fmt.Errorf("simulated interruption at node %d", w.nodesWritten)
	}
	w.nodesWritten++
	return w.writeBranch(node.Version(), uint32(node.Size()), node.Height(), preTrees, keyLeaf, node.Hash())
}

// writeSnapshotWithInterruption writes a snapshot that can be interrupted
func writeSnapshotWithInterruption(
	ctx context.Context,
	dir string, version uint32,
	interruptAfter int,
	doWrite func(*MockSnapshotWriter) (uint32, error),
) (returnErr error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	nodesFile := filepath.Join(dir, FileNameNodes)
	leavesFile := filepath.Join(dir, FileNameLeaves)
	kvsFile := filepath.Join(dir, FileNameKVs)

	fpNodes, err := createFile(nodesFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpNodes.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	fpLeaves, err := createFile(leavesFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpLeaves.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	fpKVs, err := createFile(kvsFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpKVs.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	nodesWriter := bufio.NewWriterSize(fpNodes, bufIOSize)
	leavesWriter := bufio.NewWriterSize(fpLeaves, bufIOSize)
	kvsWriter := bufio.NewWriterSize(fpKVs, bufIOSize)

	w := newMockSnapshotWriter(ctx, nodesWriter, leavesWriter, kvsWriter, interruptAfter)
	_, err = doWrite(w)

	// Always flush and sync, even on error, to simulate partial writes
	// This ensures that partial data is written to disk even when interrupted
	if err := nodesWriter.Flush(); err != nil {
		return err
	}
	if err := leavesWriter.Flush(); err != nil {
		return err
	}
	if err := kvsWriter.Flush(); err != nil {
		return err
	}

	if err := fpKVs.Sync(); err != nil {
		return err
	}
	if err := fpLeaves.Sync(); err != nil {
		return err
	}
	if err := fpNodes.Sync(); err != nil {
		return err
	}

	// Return the error after flushing (to simulate interruption after partial write)
	if err != nil {
		return err
	}

	// Write metadata
	var metadataBuf [SizeMetadata]byte
	binary.LittleEndian.PutUint32(metadataBuf[:], SnapshotFileMagic)
	binary.LittleEndian.PutUint32(metadataBuf[4:], SnapshotFormat)
	binary.LittleEndian.PutUint32(metadataBuf[8:], version)

	metadataFile := filepath.Join(dir, FileNameMetadata)
	fpMetadata, err := createFile(metadataFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpMetadata.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	if _, err := fpMetadata.Write(metadataBuf[:]); err != nil {
		return err
	}

	return fpMetadata.Sync()
}

func TestResumableSnapshotIntegration(t *testing.T) {
	dbDir := t.TempDir()
	defer os.RemoveAll(dbDir)

	// Step 1: Create DB with 2 trees
	db, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:             dbDir,
		CreateIfMissing: true,
		InitialStores:   []string{"bank", "staking"},
	})
	require.NoError(t, err)

	// Step 2: Fill up data with 10 versions using ApplyChangeSets
	for version := 1; version <= 10; version++ {
		// Create changesets for both trees using iavl.ChangeSet
		bankPairs := make([]*iavl.KVPair, 0)
		stakingPairs := make([]*iavl.KVPair, 0)

		// Add some data to each tree
		for i := 0; i < 5; i++ {
			key := []byte(fmt.Sprintf("bank-key-%d-v%d", i, version))
			value := []byte(fmt.Sprintf("bank-value-%d-v%d", i, version))
			bankPairs = append(bankPairs, &iavl.KVPair{
				Key:   key,
				Value: value,
			})

			key = []byte(fmt.Sprintf("staking-key-%d-v%d", i, version))
			value = []byte(fmt.Sprintf("staking-value-%d-v%d", i, version))
			stakingPairs = append(stakingPairs, &iavl.KVPair{
				Key:   key,
				Value: value,
			})
		}

		changesets := []*proto.NamedChangeSet{
			{
				Name:      "bank",
				Changeset: iavl.ChangeSet{Pairs: bankPairs},
			},
			{
				Name:      "staking",
				Changeset: iavl.ChangeSet{Pairs: stakingPairs},
			},
		}

		err := db.ApplyChangeSets(changesets)
		require.NoError(t, err)

		_, err = db.Commit()
		require.NoError(t, err)
	}

	// Verify we have 10 versions
	require.Equal(t, int64(10), db.Version())

	// Step 3: Simulate snapshot creation for version 5 that gets interrupted
	// First, let's manually create a partial snapshot to simulate interruption
	targetVersion := int64(5)
	snapshotDir := snapshotName(targetVersion) + "-tmp"
	tmpPath := filepath.Join(dbDir, snapshotDir)

	// Create the snapshot directory
	require.NoError(t, os.MkdirAll(tmpPath, 0755))

	// Create subdirectories for each tree
	bankDir := filepath.Join(tmpPath, "bank")
	stakingDir := filepath.Join(tmpPath, "staking")
	require.NoError(t, os.MkdirAll(bankDir, 0755))
	require.NoError(t, os.MkdirAll(stakingDir, 0755))

	// Simulate partial snapshot creation for bank tree (interrupted after 3 nodes)
	bankTree := db.TreeByName("bank")
	require.NotNil(t, bankTree)

	// Create a partial snapshot by writing some data and then stopping
	t.Logf("Bank tree root: %v, size: %d", bankTree.root != nil, bankTree.root.Size())
	err = writeSnapshotWithInterruption(context.Background(), bankDir, uint32(targetVersion), 5, func(w *MockSnapshotWriter) (uint32, error) {
		if bankTree.root == nil {
			t.Log("Bank tree root is nil")
			return 0, nil
		}
		t.Logf("Starting to write bank tree, root size: %d", bankTree.root.Size())
		if err := w.writeRecursive(bankTree.root); err != nil {
			t.Logf("Bank tree write error: %v", err)
			return 0, err
		}
		t.Logf("Bank tree write completed, leafCounter: %d", w.leafCounter)
		return w.leafCounter, nil
	})
	require.Error(t, err) // Should error due to interruption
	require.Contains(t, err.Error(), "simulated interruption")

	// Do the same for staking tree (interrupted after 2 nodes)
	stakingTree := db.TreeByName("staking")
	require.NotNil(t, stakingTree)

	err = writeSnapshotWithInterruption(context.Background(), stakingDir, uint32(targetVersion), 3, func(w *MockSnapshotWriter) (uint32, error) {
		if stakingTree.root == nil {
			return 0, nil
		}
		if err := w.writeRecursive(stakingTree.root); err != nil {
			return 0, err
		}
		return w.leafCounter, nil
	})
	require.Error(t, err) // Should error due to interruption
	require.Contains(t, err.Error(), "simulated interruption")

	// Step 4: Verify partial snapshots exist and are resumable
	require.True(t, IsResumableSnapshot(bankDir), "Bank snapshot should be resumable")
	require.True(t, IsResumableSnapshot(stakingDir), "Staking snapshot should be resumable")

	// Analyze the partial snapshots
	bankResumeInfo, err := AnalyzePartialSnapshot(bankDir)
	require.NoError(t, err)
	t.Logf("Bank partial snapshot: %d nodes, %d leaves", bankResumeInfo.WrittenNodes, bankResumeInfo.WrittenLeaves)
	require.Greater(t, bankResumeInfo.WrittenNodes, uint32(0), "Bank should have written some nodes")
	require.Greater(t, bankResumeInfo.WrittenLeaves, uint32(0), "Bank should have written some leaves")

	stakingResumeInfo, err := AnalyzePartialSnapshot(stakingDir)
	require.NoError(t, err)
	t.Logf("Staking partial snapshot: %d nodes, %d leaves", stakingResumeInfo.WrittenNodes, stakingResumeInfo.WrittenLeaves)
	require.Greater(t, stakingResumeInfo.WrittenNodes, uint32(0), "Staking should have written some nodes")
	require.Greater(t, stakingResumeInfo.WrittenLeaves, uint32(0), "Staking should have written some leaves")

	// Step 5: Simulate restart - create a new DB instance
	require.NoError(t, db.Close())

	// Step 6: Open DB again and verify it detects resumable snapshots
	// Load the DB at version 5 to match our target snapshot version
	db2, err := OpenDB(logger.NewNopLogger(), targetVersion, Options{
		Dir: dbDir,
	})
	require.NoError(t, err)
	defer db2.Close()

	// Step 7: Test resuming the snapshot creation by calling RewriteSnapshot
	// This should automatically detect the resumable snapshot and complete it
	err = db2.RewriteSnapshot(context.Background())
	require.NoError(t, err)

	// Step 7b: Reload to switch to the new snapshot and apply pending changes
	err = db2.Reload()
	require.NoError(t, err)

	// Step 8: Verify the DB has switched to the new snapshot
	// The DB should now be using the completed snapshot
	require.Equal(t, targetVersion, db2.Version(), "DB should be at target version after resume")

	// Step 9: Verify the temporary snapshot directory is gone (moved to final location)
	finalSnapshotDir := filepath.Join(dbDir, snapshotName(targetVersion))
	_, err = os.Stat(finalSnapshotDir)
	require.NoError(t, err, "Final snapshot directory should exist")

	_, err = os.Stat(tmpPath)
	require.True(t, os.IsNotExist(err), "Temporary snapshot directory should be gone")

	// Step 10: Verify we can read data from the current DB (which should be using the new snapshot)
	bankTree = db2.TreeByName("bank")
	require.NotNil(t, bankTree)

	// Check that we can read some of the data that was written
	value := bankTree.Get([]byte("bank-key-0-v5"))
	require.Equal(t, []byte("bank-value-0-v5"), value)

	stakingTree = db2.TreeByName("staking")
	require.NotNil(t, stakingTree)

	stakingValue := stakingTree.Get([]byte("staking-key-0-v5"))
	require.Equal(t, []byte("staking-value-0-v5"), stakingValue)

	// Step 11: Verify the snapshot files are complete
	bankSnapshotDir := filepath.Join(finalSnapshotDir, "bank")
	stakingSnapshotDir := filepath.Join(finalSnapshotDir, "staking")

	_, err = os.Stat(filepath.Join(bankSnapshotDir, FileNameMetadata))
	require.NoError(t, err, "Bank metadata should exist after resume")

	_, err = os.Stat(filepath.Join(stakingSnapshotDir, FileNameMetadata))
	require.NoError(t, err, "Staking metadata should exist after resume")

	t.Logf("Successfully resumed snapshot creation:")
	t.Logf("  Bank: %d nodes, %d leaves written", bankResumeInfo.WrittenNodes, bankResumeInfo.WrittenLeaves)
	t.Logf("  Staking: %d nodes, %d leaves written", stakingResumeInfo.WrittenNodes, stakingResumeInfo.WrittenLeaves)
}

func TestResumableSnapshotWithEmptyTrees(t *testing.T) {
	dbDir := t.TempDir()
	defer os.RemoveAll(dbDir)

	// Create DB with empty trees
	db, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:             dbDir,
		CreateIfMissing: true,
		InitialStores:   []string{"empty1", "empty2"},
	})
	require.NoError(t, err)
	defer db.Close()

	// Commit to create version 1
	_, err = db.Commit()
	require.NoError(t, err)

	// Create partial snapshot directories
	snapshotDir := "snapshot-00000000000000000001-tmp"
	tmpPath := filepath.Join(dbDir, snapshotDir)
	require.NoError(t, os.MkdirAll(tmpPath, 0755))

	empty1Dir := filepath.Join(tmpPath, "empty1")
	empty2Dir := filepath.Join(tmpPath, "empty2")
	require.NoError(t, os.MkdirAll(empty1Dir, 0755))
	require.NoError(t, os.MkdirAll(empty2Dir, 0755))

	// Create empty files to simulate partial snapshot
	require.NoError(t, os.WriteFile(filepath.Join(empty1Dir, FileNameNodes), []byte{}, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(empty1Dir, FileNameLeaves), []byte{}, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(empty1Dir, FileNameKVs), []byte{}, 0644))

	require.NoError(t, os.WriteFile(filepath.Join(empty2Dir, FileNameNodes), []byte{}, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(empty2Dir, FileNameLeaves), []byte{}, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(empty2Dir, FileNameKVs), []byte{}, 0644))

	// Test resuming empty snapshots
	err = db.TreeByName("empty1").WriteSnapshotResumableFromFiles(context.Background(), empty1Dir)
	require.NoError(t, err)

	err = db.TreeByName("empty2").WriteSnapshotResumableFromFiles(context.Background(), empty2Dir)
	require.NoError(t, err)

	// Verify snapshots are complete
	_, err = os.Stat(filepath.Join(empty1Dir, FileNameMetadata))
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(empty2Dir, FileNameMetadata))
	require.NoError(t, err)
}
