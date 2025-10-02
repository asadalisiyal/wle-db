//go:build rocksdbBackend
// +build rocksdbBackend

package ss

import (
	"github.com/asadalisiyal/wle-db/common/utils"
	"github.com/asadalisiyal/wle-db/config"
	"github.com/asadalisiyal/wle-db/ss/rocksdb"
	"github.com/asadalisiyal/wle-db/ss/types"
)

func init() {
	initializer := func(dir string, configs config.StateStoreConfig) (types.StateStore, error) {
		dbHome := utils.GetStateStorePath(dir, configs.Backend)
		if configs.DBDirectory != "" {
			dbHome = configs.DBDirectory
		}
		return rocksdb.New(dbHome, configs)
	}
	RegisterBackend(RocksDBBackend, initializer)
}
