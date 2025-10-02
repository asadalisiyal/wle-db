package ss

import (
	"github.com/asadalisiyal/wle-db/common/utils"
	"github.com/asadalisiyal/wle-db/config"
	"github.com/asadalisiyal/wle-db/ss/pebbledb"
	"github.com/asadalisiyal/wle-db/ss/types"
)

func init() {
	initializer := func(dir string, configs config.StateStoreConfig) (types.StateStore, error) {
		dbHome := utils.GetStateStorePath(dir, configs.Backend)
		if configs.DBDirectory != "" {
			dbHome = configs.DBDirectory
		}
		return pebbledb.New(dbHome, configs)
	}
	RegisterBackend(PebbleDBBackend, initializer)
}
