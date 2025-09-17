package memiavl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/stretchr/testify/require"
)

func TestSnapshotResumeFromFiles(t *testing.T) {
	dbDir := t.TempDir()
	defer os.RemoveAll(dbDir)

	// Create a test database
	db, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:             dbDir,
		CreateIfMissing: true,
		InitialStores:   []string{"test"},
	})
	require.NoError(t, err)

	// Add some test data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		db.TreeByName("test").Set(key, value)
	}

	// Commit the changes
	_, err = db.Commit()
	require.NoError(t, err)

	// Test 1: Create a partial snapshot by manually creating files
	snapshotDir := filepath.Join(dbDir, "snapshot-test-tmp")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	// Create partial files to simulate interrupted snapshot
	nodesFile := filepath.Join(snapshotDir, "nodes")
	leavesFile := filepath.Join(snapshotDir, "leaves")
	kvsFile := filepath.Join(snapshotDir, "kvs")

	// Write some partial data
	require.NoError(t, os.WriteFile(nodesFile, make([]byte, 5*SizeNode), 0644))
	require.NoError(t, os.WriteFile(leavesFile, make([]byte, 10*SizeLeaf), 0644))
	require.NoError(t, os.WriteFile(kvsFile, make([]byte, 1000), 0644))

	// Test 2: Analyze the partial snapshot
	resumeInfo, err := AnalyzePartialSnapshot(snapshotDir)
	require.NoError(t, err)
	require.Equal(t, uint32(5), resumeInfo.WrittenNodes)
	require.Equal(t, uint32(10), resumeInfo.WrittenLeaves)
	require.Equal(t, int64(5*SizeNode), resumeInfo.NodesFilePos)
	require.Equal(t, int64(10*SizeLeaf), resumeInfo.LeavesFilePos)

	// Test 3: Test IsResumableSnapshot
	require.True(t, IsResumableSnapshot(snapshotDir))

	// Test 4: Test resume functionality
	treeDir := filepath.Join(snapshotDir, "test")
	require.NoError(t, os.MkdirAll(treeDir, 0755))

	// Create partial tree files
	treeNodesFile := filepath.Join(treeDir, "nodes")
	treeLeavesFile := filepath.Join(treeDir, "leaves")
	treeKvsFile := filepath.Join(treeDir, "kvs")

	require.NoError(t, os.WriteFile(treeNodesFile, make([]byte, 3*SizeNode), 0644))
	require.NoError(t, os.WriteFile(treeLeavesFile, make([]byte, 7*SizeLeaf), 0644))
	require.NoError(t, os.WriteFile(treeKvsFile, make([]byte, 500), 0644))

	// Test resume
	err = db.TreeByName("test").WriteSnapshotResumableFromFiles(context.Background(), treeDir)
	require.NoError(t, err)

	// Verify the snapshot was completed
	_, err = os.Stat(filepath.Join(treeDir, "metadata"))
	require.NoError(t, err)

	require.NoError(t, db.Close())
}

func TestAnalyzePartialSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Test with non-existent directory
	_, err := AnalyzePartialSnapshot(filepath.Join(tmpDir, "nonexistent"))
	require.Error(t, err)

	// Test with missing files
	snapshotDir := filepath.Join(tmpDir, "incomplete")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	_, err = AnalyzePartialSnapshot(snapshotDir)
	require.Error(t, err)

	// Test with invalid file sizes
	nodesFile := filepath.Join(snapshotDir, "nodes")
	leavesFile := filepath.Join(snapshotDir, "leaves")
	kvsFile := filepath.Join(snapshotDir, "kvs")

	// Create files with invalid sizes (not multiples of node/leaf sizes)
	require.NoError(t, os.WriteFile(nodesFile, make([]byte, 100), 0644))
	require.NoError(t, os.WriteFile(leavesFile, make([]byte, 200), 0644))
	require.NoError(t, os.WriteFile(kvsFile, make([]byte, 300), 0644))

	_, err = AnalyzePartialSnapshot(snapshotDir)
	require.Error(t, err)

	// Test with valid file sizes
	require.NoError(t, os.WriteFile(nodesFile, make([]byte, 5*SizeNode), 0644))
	require.NoError(t, os.WriteFile(leavesFile, make([]byte, 10*SizeLeaf), 0644))
	require.NoError(t, os.WriteFile(kvsFile, make([]byte, 1000), 0644))

	resumeInfo, err := AnalyzePartialSnapshot(snapshotDir)
	require.NoError(t, err)
	require.Equal(t, uint32(5), resumeInfo.WrittenNodes)
	require.Equal(t, uint32(10), resumeInfo.WrittenLeaves)
}
