package immudbengine

import (
	"bytes"
	"context"
	"errors"

	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/metadata"
)

// A store is an implementation of the engine.Store interface.
type store struct {
	name	string
	ctx	context.Context
	tx	*transaction
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *store) Put(k, v []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return engine.ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	r,err := s.tx.engine.client.SafeSet(s.tx.ctx, k, v)
	if err || r.Verified != true {
		return errors.New("Put not verified")
	}
	return err
}

// Get returns a value associated with the given key. If not found, returns engine.ErrKeyNotFound.
func (s *store) Get(k []byte) ([]byte, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}
	
	r,err := s.tx.engine.client.SafeGet(s.tx.ctx, k)

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, engine.ErrKeyNotFound
		}

		return nil, err
	}

	return r.Value
}

// Delete a record by key. If not found, returns engine.ErrKeyNotFound.
func (s *store) Delete(k []byte) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return engine.ErrTransactionReadOnly
	}
	return error.New("Deletion impossible on immutable database")
}

// Truncate deletes all the records of the store.
func (s *store) Truncate() error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	if !s.writable {
		return engine.ErrTransactionReadOnly
	}
	return error.New("Deletion impossible on immutable database")
}

// NextSequence returns a monotonically increasing integer.
func (s *store) NextSequence() (uint64, error) {
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}

	if !s.writable {
		return 0, engine.ErrTransactionReadOnly
	}
	r,err := s.tx.engine.client.CurrentRoot(s.tx.ctx)
	if err != nil {
		return 0, error.New("Unable to fetch root")
	}
	return r.payload.index + 1, nil
}
//-----------------------

type iterator struct {
	reverse		bool
	item		schema.Item
	stor		*store
	err		error
}

// Iterator uses a Badger iterator with default options.
// Only one iterator is allowed per read-write transaction.
func (s *store) Iterator(opts engine.IteratorOptions) engine.Iterator {
	return &iterator{
		reverse:	opts.Reverse,
		stor:		s,
		item:		schema.Item{},
		err:		nil,
	}
}

func (it *iterator) Seek(pivot []byte) {
	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
		return
	default:
	}
	opt := schema.ScanOptions{
		prefix: pivot,
		offset: 0,
		limit:  1,
		reverse: it.reverse,
		deep:    false,
	}
	it.s.tx.engine.client.Scan(opt)
}

func (it *iterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix) && it.err == nil
}

func (it *iterator) Next() {
	it.it.Next()
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Item() engine.Item {
	it.item.item = it.it.Item()

	return &it.item
}

func (it *iterator) Close() error {
	it.it.Close()
	return nil
}

type badgerItem struct {
	item   *badger.Item
	prefix []byte
}

func (i *badgerItem) Key() []byte {
	return bytes.TrimPrefix(i.item.Key(), i.prefix)
}

func (i *badgerItem) ValueCopy(buf []byte) ([]byte, error) {
	return i.item.ValueCopy(buf)
}
