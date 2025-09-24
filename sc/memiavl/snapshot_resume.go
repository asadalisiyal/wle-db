package memiavl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// AnalyzePartialSnapshot analyzes a partial snapshot directory to determine resume information
func AnalyzePartialSnapshot(snapshotDir string) (*SnapshotResumeInfo, error) {
	nodesFile := filepath.Join(snapshotDir, FileNameNodes)
	leavesFile := filepath.Join(snapshotDir, FileNameLeaves)
	kvsFile := filepath.Join(snapshotDir, FileNameKVs)

	// Check if all required files exist
	requiredFiles := []string{nodesFile, leavesFile, kvsFile}
	for _, file := range requiredFiles {
		if _, err := os.Stat(file); err != nil {
			return nil, fmt.Errorf("required file %s not found: %w", file, err)
		}
	}

	// Get file sizes
	nodesInfo, err := os.Stat(nodesFile)
	if err != nil {
		return nil, fmt.Errorf("failed to stat nodes file: %w", err)
	}

	leavesInfo, err := os.Stat(leavesFile)
	if err != nil {
		return nil, fmt.Errorf("failed to stat leaves file: %w", err)
	}

	kvsInfo, err := os.Stat(kvsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to stat kvs file: %w", err)
	}

	// Calculate written counts based on file sizes
	// Since post-order traversal writes leaves first, then nodes, we can determine progress
	nodesSize := nodesInfo.Size()
	leavesSize := leavesInfo.Size()
	kvsSize := kvsInfo.Size()

	// Validate file sizes are multiples of node/leaf sizes
	if nodesSize%int64(SizeNode) != 0 {
		return nil, fmt.Errorf("nodes file size %d is not a multiple of %d", nodesSize, SizeNode)
	}
	if leavesSize%int64(SizeLeaf) != 0 {
		return nil, fmt.Errorf("leaves file size %d is not a multiple of %d", leavesSize, SizeLeaf)
	}

	writtenNodes := uint32(nodesSize / int64(SizeNode))
	writtenLeaves := uint32(leavesSize / int64(SizeLeaf))

	// For KVs file, we need to determine the current offset
	// This is more complex as it depends on the actual key-value data
	// For now, we'll use the file size as an approximation
	// In a more sophisticated implementation, we could parse the last written entry
	kvsOffset := kvsSize

	return &SnapshotResumeInfo{
		NodesFilePos:  nodesSize,
		LeavesFilePos: leavesSize,
		KVsFilePos:    kvsOffset,
		WrittenNodes:  writtenNodes,
		WrittenLeaves: writtenLeaves,
	}, nil
}

// IsResumableSnapshot checks if a snapshot directory can be resumed
// It handles both single-tree and multi-tree snapshots
func IsResumableSnapshot(snapshotDir string) bool {
	// Check if there are any tree subdirectories with resumable data
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == MetadataFileName {
			continue
		}

		// Check if this tree subdirectory contains resumable snapshot data
		treeDir := filepath.Join(snapshotDir, entry.Name())
		if _, err := AnalyzePartialSnapshot(treeDir); err == nil {
			return true
		} else {
			fmt.Printf("AnalyzePartialSnapshot failed with error %v\n", err)
		}

	}

	return false
}

// WriteSnapshotResumableFromFiles writes a snapshot with resume capability based on file analysis
func (t *Tree) WriteSnapshotResumableFromFiles(ctx context.Context, treeDir string) error {
	resumeInfo, err := AnalyzePartialSnapshot(treeDir)
	if err != nil {
		// If we can't analyze the partial snapshot, start fresh
		return t.WriteSnapshot(ctx, treeDir)
	}

	opts := &SnapshotWriteOptions{
		ResumeInfo: resumeInfo,
	}

	return writeSnapshotWithOptions(ctx, treeDir, t.version, opts, func(w *snapshotWriter) (uint32, error) {
		if t.root == nil {
			return 0, nil
		}

		if err := w.writeRecursive(t.root); err != nil {
			return 0, err
		}
		return w.leafCounter, nil
	})
}
