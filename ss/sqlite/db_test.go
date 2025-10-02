//go:build sqliteBackend
// +build sqliteBackend

package sqlite

import (
	"testing"

	"github.com/asadalisiyal/wle-db/config"
	sstest "github.com/asadalisiyal/wle-db/ss/test"
	"github.com/asadalisiyal/wle-db/ss/types"
	"github.com/stretchr/testify/suite"
)

// TODO: Update Sqlite to latest
func TestStorageTestSuite(t *testing.T) {
	s := &sstest.StorageTestSuite{
		NewDB: func(dir string) (types.StateStore, error) {
			return New(dir, config.DefaultStateStoreConfig())
		},
		EmptyBatchSize: 0,
	}

	suite.Run(t, s)
}
