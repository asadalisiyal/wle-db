//go:build sqliteBackend
// +build sqliteBackend

package ss

import (
	"github.com/asadalisiyal/wle-db/common/utils"
	"github.com/asadalisiyal/wle-db/config"
	"github.com/asadalisiyal/wle-db/ss/sqlite"
	"github.com/asadalisiyal/wle-db/ss/types"
)

func init() {
	initializer := func(dir string, configs config.StateStoreConfig) (types.StateStore, error) {
		dbHome := utils.GetStateStorePath(dir, configs.Backend)
		if configs.DBDirectory != "" {
			dbHome = configs.DBDirectory
		}
		return sqlite.New(dbHome, configs)
	}
	RegisterBackend(SQLiteBackend, initializer)
}
