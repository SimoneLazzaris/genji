package enginetest

import (
	"errors"
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/stretchr/testify/require"
)

func storeBuilder(t testing.TB, builder Builder) (engine.Store, func()) {
	ng, cleanup := builder()
	tx, err := ng.Begin(true)
	require.NoError(t, err)
	err = tx.CreateStore("test")
	require.NoError(t, err)
	st, err := tx.Store("test")
	require.NoError(t, err)
	return st, func() {
		tx.Rollback()
		cleanup()
	}
}

// TestStoreAscendGreaterOrEqual verifies AscendGreaterOrEqual behaviour.
func TestStoreAscendGreaterOrEqual(t *testing.T, builder Builder) {
	t.Run("Should not fail with no records", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		i := 0
		err := st.AscendGreaterOrEqual(nil, func(k, v []byte) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})

	t.Run("With no pivot, should iterate over all records in order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 1
		var count int
		err := st.AscendGreaterOrEqual(nil, func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i++
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, count, 10)
	})

	t.Run("With pivot, should iterate over some records in order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 4
		var count int
		err := st.AscendGreaterOrEqual([]byte{i}, func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i++
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 7, count)
	})

	t.Run("If pivot not found, should start from the next item", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte{1}, []byte{1})
		require.NoError(t, err)

		err = st.Put([]byte{3}, []byte{3})
		require.NoError(t, err)

		called := false
		err = st.AscendGreaterOrEqual([]byte{2}, func(k, v []byte) error {
			require.Equal(t, []byte{3}, k)
			require.Equal(t, []byte{3}, v)
			called = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("Should stop if fn returns an error", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i)})
			require.NoError(t, err)
		}

		i := 0
		err := st.AscendGreaterOrEqual(nil, func(k, v []byte) error {
			i++
			if i >= 5 {
				return errors.New("some error")
			}
			return nil
		})
		require.EqualError(t, err, "some error")
		require.Equal(t, 5, i)
	})
}

// TestStoreDescendLessOrEqual verifies DescendLessOrEqual behaviour.
func TestStoreDescendLessOrEqual(t *testing.T, builder Builder) {
	t.Run("Should not fail with no records", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		i := 0
		err := st.DescendLessOrEqual(nil, func(k, v []byte) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})

	t.Run("With no pivot, should iterate over all records in reverse order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 10
		var count int
		err := st.DescendLessOrEqual(nil, func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i--
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 10, count)
	})

	t.Run("With pivot, should iterate over some records in reverse order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 4
		var count int
		err := st.DescendLessOrEqual([]byte{i}, func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i--
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 4, count)
	})

	t.Run("If pivot not found, should start from the previous item", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte{1}, []byte{1})
		require.NoError(t, err)

		err = st.Put([]byte{3}, []byte{3})
		require.NoError(t, err)

		called := false
		err = st.DescendLessOrEqual([]byte{2}, func(k, v []byte) error {
			require.Equal(t, []byte{1}, k)
			require.Equal(t, []byte{1}, v)
			called = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("Should stop if fn returns an error", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i)})
			require.NoError(t, err)
		}

		i := 0
		err := st.DescendLessOrEqual(nil, func(k, v []byte) error {
			i++
			if i >= 5 {
				return errors.New("some error")
			}
			return nil
		})
		require.EqualError(t, err, "some error")
		require.Equal(t, 5, i)
	})
}

// TestStorePut verifies Put behaviour.
func TestStorePut(t *testing.T, builder Builder) {
	t.Run("Should insert data", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)
	})

	t.Run("Should replace existing key", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		err = st.Put([]byte("foo"), []byte("BAR"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("BAR"), v)
	})

	t.Run("Should fail when key is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put(nil, []byte("FOO"))
		require.Error(t, err)

		err = st.Put([]byte(""), []byte("BAR"))
		require.Error(t, err)
	})

	t.Run("Should succeed when value is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), nil)
		require.NoError(t, err)

		err = st.Put([]byte("foo"), []byte(""))
		require.NoError(t, err)
	})
}

// TestStoreGet verifies Get behaviour.
func TestStoreGet(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		r, err := st.Get([]byte("id"))
		require.Equal(t, engine.ErrKeyNotFound, err)
		require.Nil(t, r)
	})

	t.Run("Should return the right key", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)

		v, err = st.Get([]byte("bar"))
		require.NoError(t, err)
		require.Equal(t, []byte("BAR"), v)
	})
}

// TestStoreDelete verifies Delete behaviour.
func TestStoreDelete(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Delete([]byte("id"))
		require.Equal(t, engine.ErrKeyNotFound, err)
	})

	t.Run("Should delete the right record", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)

		// delete the key
		err = st.Delete([]byte("foo"))
		require.NoError(t, err)

		// try again, should fail
		err = st.Delete([]byte("foo"))
		require.Equal(t, engine.ErrKeyNotFound, err)

		// make sure it didn't also delete the other one
		v, err = st.Get([]byte("bar"))
		require.NoError(t, err)
		require.Equal(t, []byte("BAR"), v)
	})
}

// // TestTableWriterReplace verifies Replace behaviour.
// func TestTableWriterReplace(t *testing.T, builder Builder) {
// 	t.Run("Should fail if not found", func(t *testing.T) {
// 		st, cleanup := storeBuilder(t, builder)
// 		defer cleanup()

// 		err := st.Replace([]byte("id"), newRecord())
// 		require.Equal(t, table.ErrRecordNotFound, err)
// 	})

// 	t.Run("Should replace the right record", func(t *testing.T) {
// 		st, cleanup := storeBuilder(t, builder)
// 		defer cleanup()

// 		// create two different records
// 		rec1 := newRecord()
// 		rec2 := record.FieldBuffer([]field.Field{
// 			field.NewString("fielda", "c"),
// 			field.NewString("fieldb", "d"),
// 		})

// 		recordID1, err := st.Insert(rec1)
// 		require.NoError(t, err)
// 		recordID2, err := st.Insert(rec2)
// 		require.NoError(t, err)

// 		// create a third record
// 		rec3 := record.FieldBuffer([]field.Field{
// 			field.NewString("fielda", "e"),
// 			field.NewString("fieldb", "f"),
// 		})

// 		// replace rec1 with rec3
// 		err = st.Replace(recordID1, rec3)
// 		require.NoError(t, err)

// 		// make sure it replaced it correctly
// 		res, err := st.Record(recordID1)
// 		require.NoError(t, err)
// 		f, err := res.Field("fielda")
// 		require.NoError(t, err)
// 		require.Equal(t, "e", string(f.Data))

// 		// make sure it didn't also replace the other one
// 		res, err = st.Record(recordID2)
// 		require.NoError(t, err)
// 		f, err = res.Field("fielda")
// 		require.NoError(t, err)
// 		require.Equal(t, "c", string(f.Data))
// 	})
// }

// // TestTableWriterTruncate verifies Truncate behaviour.
// func TestTableWriterTruncate(t *testing.T, builder Builder) {
// 	t.Run("Should succeed if table empty", func(t *testing.T) {
// 		st, cleanup := storeBuilder(t, builder)
// 		defer cleanup()

// 		err := st.Truncate()
// 		require.NoError(t, err)
// 	})

// 	t.Run("Should truncate the table", func(t *testing.T) {
// 		st, cleanup := storeBuilder(t, builder)
// 		defer cleanup()

// 		// create two records
// 		rec1 := newRecord()
// 		rec2 := newRecord()

// 		_, err := st.Insert(rec1)
// 		require.NoError(t, err)
// 		_, err = st.Insert(rec2)
// 		require.NoError(t, err)

// 		err = st.Truncate()
// 		require.NoError(t, err)

// 		err = st.Iterate(func(_ []byte, _ record.Record) error {
// 			return errors.New("should not iterate")
// 		})

// 		require.NoError(t, err)
// 	})
// }