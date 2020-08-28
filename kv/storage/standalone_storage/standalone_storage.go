package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.

type StandAloneStorage struct {
	Database *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB("/tmp/badger", false)
	return &StandAloneStorage{
		Database: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// switch cf {
	// case engine_util.CfDefault:
	// 	val, err := engine_util.GetCF(s.Database, CfDefault, []byte("e"))
	// case engine_util.CfLock:
	// 	val, err := engine_util.GetCF(s.Database, CfDefault, []byte("e"))
	// case engine_util.CfWrite:
	// 	val, err := engine_util.GetCF(s.Database, CfDefault, []byte("e"))
	// }

	return &memReader{s, 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	newBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			// item := memItem{data.Key, data.Value, false}
			switch data.Cf {
			case engine_util.CfDefault:
				newBatch.SetCF(engine_util.CfDefault, []byte(data.Key), []byte(data.Value))
			case engine_util.CfLock:
				newBatch.SetCF(engine_util.CfLock, []byte(data.Key), []byte(data.Value))
			case engine_util.CfWrite:
				newBatch.SetCF(engine_util.CfWrite, []byte(data.Key), []byte(data.Value))
			}
		case storage.Delete:

			switch data.Cf {
			case engine_util.CfDefault:
				newBatch.DeleteCF(engine_util.CfDefault, []byte(data.Key))
			case engine_util.CfLock:
				newBatch.DeleteCF(engine_util.CfLock, []byte(data.Key))
			case engine_util.CfWrite:
				newBatch.DeleteCF(engine_util.CfWrite, []byte(data.Key))
			}
		}
	}

	error := newBatch.WriteToDB(s.Database)
	return error
}

type memReader struct {
	inner     *StandAloneStorage
	iterCount int
}

func (mr *memReader) GetCF(cf string, key []byte) ([]byte, error) {
	var result []byte
	var _err error
	switch cf {
	case engine_util.CfDefault:
		result, _err = engine_util.GetCF(mr.inner.Database, engine_util.CfDefault, key)
	case engine_util.CfLock:
		result, _err = engine_util.GetCF(mr.inner.Database, engine_util.CfLock, key)
	case engine_util.CfWrite:
		result, _err = engine_util.GetCF(mr.inner.Database, engine_util.CfWrite, key)
	default:
		return nil, fmt.Errorf("mem-server: bad CF %s", cf)
	}

	if result == nil {
		return nil, nil
	}

	return result, _err
}

func (mr *memReader) IterCF(cf string) engine_util.DBIterator {
	txn := mr.inner.Database.NewTransaction(false)
	defer txn.Discard()
	iter := engine_util.NewCFIterator(cf, txn)
	defer iter.Close()
	return &memIter{iter}
}

func (r *memReader) Close() {
	if r.iterCount > 0 {
		panic("Unclosed iterator")
	}
}

type memIter struct {
	item *engine_util.BadgerIterator
}

func (it *memIter) Item() engine_util.DBItem {
	return it.item.Item()
}

func (it *memIter) Valid() bool {
	return it.item.Valid()
}
func (it *memIter) Next() {
	it.item.Next()
}

func (it *memIter) Seek(key []byte) {
	it.item.Seek(key)
}

func (it *memIter) Close() {
	it.item.Close()
}
