//go:build rocksdbBackend
// +build rocksdbBackend

package rocksdb

import (
	"testing"

	"github.com/asadalisiyal/wle-db/config"
	sstest "github.com/asadalisiyal/wle-db/ss/test"
	"github.com/asadalisiyal/wle-db/ss/types"
)

func BenchmarkDBBackend(b *testing.B) {
	s := &sstest.StorageBenchSuite{
		NewDB: func(dir string) (types.StateStore, error) {
			return New(dir, config.DefaultStateStoreConfig())
		},
		BenchBackendName: "RocksDB",
	}

	s.BenchmarkGet(b)
	s.BenchmarkApplyChangeset(b)
	s.BenchmarkIterate(b)
}
