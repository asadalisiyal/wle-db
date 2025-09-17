# Resumable Snapshot Creation

This document describes the resumable snapshot creation feature that addresses the page cache eviction issue during snapshot creation.

## Problem

When creating large snapshots (e.g., 107GB on a 128GB RAM machine), the current implementation has a critical flaw:

1. **Page Cache Eviction**: During snapshot creation, the OS evicts pages from the current snapshot to make room for the new snapshot being written
2. **No Resume Capability**: If the node restarts during snapshot creation, all progress is lost and the process starts from scratch
3. **Wasted Resources**: This causes repeated page cache eviction and wastes significant I/O and time

## Solution: Resumable Snapshots

The resumable snapshot system allows partial snapshots to be resumed after a restart by analyzing the partially created files themselves. This elegant approach eliminates the need for separate progress tracking files.

### Key Features

1. **File-Based Progress Detection**: Analyzes file sizes to determine progress
2. **Resume Capability**: Can resume from the last written position after restart
3. **Smart Cleanup**: Only removes non-resumable temporary directories
4. **Page Cache Preservation**: Reduces the need to re-read data from disk
5. **No Extra Files**: No need for separate progress tracking files

### Architecture

#### Progress Detection

Since snapshot creation uses post-order traversal (leaves first, then nodes), we can determine progress by examining file sizes:

```go
type SnapshotResumeInfo struct {
    // File positions
    NodesFilePos  int64
    LeavesFilePos int64
    KVsFilePos    int64
    
    // Counters
    WrittenNodes  uint32
    WrittenLeaves uint32
}
```

#### File Structure

```
snapshot-{version}-tmp/
├── nodes                    # Branch nodes data
├── leaves                   # Leaf nodes data  
├── kvs                      # Key-value pairs
└── metadata                 # Snapshot metadata (written last)
```

### Usage

#### Automatic Resume

The system automatically detects and resumes partial snapshots on startup:

```go
// On DB startup, the system checks for resumable snapshots
if IsResumableSnapshot(snapshotPath) {
    db.logger.Info("resuming snapshot creation", "path", snapshotPath)
    if err := db.resumeSnapshotFromFiles(ctx, snapshotPath); err != nil {
        return err
    }
}
```

#### Manual Resume

You can also manually resume a snapshot:

```go
resumeInfo, err := AnalyzePartialSnapshot(snapshotDir)
if err != nil {
    return err
}

err = tree.WriteSnapshotResumableFromFiles(ctx, snapshotDir)
```

### Benefits

1. **Reduced Page Cache Pressure**: Resuming from 80% completion means only 20% of data needs to be re-read
2. **Faster Recovery**: Significantly faster restart times when snapshots are partially complete
3. **Resource Efficiency**: No wasted I/O on already-written data
4. **Better Reliability**: System can handle interruptions gracefully
5. **Simplicity**: No extra files or complex progress tracking needed

### Configuration

No additional configuration is required. The feature is automatically enabled and works with existing snapshot settings:

```toml
[state-commit]
sc-snapshot-interval = 10000  # Snapshot every 10,000 blocks
sc-keep-recent = 1            # Keep 1 old snapshot
```

### Monitoring

The system logs progress information:

```
INFO resuming snapshot creation path=/data/snapshot-1234567890-tmp
INFO resuming snapshot written_nodes=750000 written_leaves=1500000 nodes_file_pos=60000000 leaves_file_pos=120000000
```

### Implementation Details

#### Progress Detection

- Analyzes file sizes to determine written nodes and leaves
- Uses post-order traversal property: leaves written first, then nodes
- Validates file sizes are multiples of node/leaf sizes

#### Cleanup Logic

- `cleanupTmpDirs()` only removes non-resumable temporary directories
- Resumable snapshots are preserved for potential resumption
- No extra cleanup needed for progress files

#### Error Handling

- Invalid file sizes cause the snapshot to be treated as non-resumable
- Missing required files cause the snapshot to be treated as non-resumable
- Graceful fallback to full snapshot creation if resume fails

### Testing

Run the tests to verify the functionality:

```bash
go test ./sc/memiavl -run TestSnapshotResumeFromFiles
go test ./sc/memiavl -run TestAnalyzePartialSnapshot
```

### Migration

This feature is backward compatible:
- Existing snapshots continue to work normally
- No database migration required
- Can be enabled/disabled without affecting existing data

### Performance Impact

- **Zero overhead**: No additional files or tracking during normal operation
- **Memory efficient**: Only analyzes files when resuming
- **I/O efficient**: Reduces total I/O by avoiding re-reading completed data

### Future Enhancements

Potential improvements for future versions:

1. **Chunked Writing**: Write snapshots in smaller chunks to reduce memory pressure
2. **Compression**: Add compression during snapshot creation
3. **Parallel Resume**: Resume multiple trees in parallel
4. **Progress UI**: Web interface for monitoring snapshot progress
5. **Memory Pressure Detection**: Automatically adjust writing strategy based on available memory
6. **KVS Offset Tracking**: More precise tracking of KVS file position for better resume accuracy
