package memiavl

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// SnapshotResumeInfo contains information needed to resume a partial snapshot
type SnapshotResumeInfo struct {
	// File positions
	NodesFilePos  int64
	LeavesFilePos int64
	KVsFilePos    int64

	// Counters
	WrittenNodes  uint32
	WrittenLeaves uint32
}

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
func IsResumableSnapshot(snapshotDir string) bool {
	_, err := AnalyzePartialSnapshot(snapshotDir)
	return err == nil
}

// WriteSnapshotResumableFromFiles writes a snapshot with resume capability based on file analysis
func (t *Tree) WriteSnapshotResumableFromFiles(ctx context.Context, snapshotDir string) error {
	resumeInfo, err := AnalyzePartialSnapshot(snapshotDir)
	if err != nil {
		// If we can't analyze the partial snapshot, start fresh
		return t.WriteSnapshot(ctx, snapshotDir)
	}

	return writeSnapshotResumableFromInfo(ctx, snapshotDir, t.version, resumeInfo, func(w *snapshotWriter) (uint32, error) {
		if t.root == nil {
			return 0, nil
		}

		if err := w.writeRecursive(t.root); err != nil {
			return 0, err
		}
		return w.leafCounter, nil
	})
}

// writeSnapshotResumableFromInfo writes a snapshot resuming from the given info
func writeSnapshotResumableFromInfo(
	ctx context.Context,
	dir string, version uint32,
	resumeInfo *SnapshotResumeInfo,
	doWrite func(*snapshotWriter) (uint32, error),
) (returnErr error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	nodesFile := filepath.Join(dir, FileNameNodes)
	leavesFile := filepath.Join(dir, FileNameLeaves)
	kvsFile := filepath.Join(dir, FileNameKVs)

	// Open files for append
	fpNodes, err := os.OpenFile(nodesFile, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpNodes.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	fpLeaves, err := os.OpenFile(leavesFile, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpLeaves.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	fpKVs, err := os.OpenFile(kvsFile, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpKVs.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	// Seek to the resume positions
	if _, err := fpNodes.Seek(resumeInfo.NodesFilePos, 0); err != nil {
		return err
	}
	if _, err := fpLeaves.Seek(resumeInfo.LeavesFilePos, 0); err != nil {
		return err
	}
	if _, err := fpKVs.Seek(resumeInfo.KVsFilePos, 0); err != nil {
		return err
	}

	nodesWriter := bufio.NewWriterSize(fpNodes, bufIOSize)
	leavesWriter := bufio.NewWriterSize(fpLeaves, bufIOSize)
	kvsWriter := bufio.NewWriterSize(fpKVs, bufIOSize)

	w := newSnapshotWriter(ctx, nodesWriter, leavesWriter, kvsWriter)

	// Set initial counters from resume info
	w.branchCounter = resumeInfo.WrittenNodes
	w.leafCounter = resumeInfo.WrittenLeaves
	w.kvsOffset = uint64(resumeInfo.KVsFilePos)

	leaves, err := doWrite(w)
	if err != nil {
		return err
	}

	if leaves > 0 {
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
