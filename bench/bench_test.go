package bench

import (
	"pgstore/pgstore/store"
	"strconv"
	"sync/atomic"
	"testing"
)

var expectedValue = map[string]string{
	"paperback": "5",
	"language":  "English",
	"weight":    "100 g",
}

const manyRowsForQuery = 10

func init() {
	// json store
	jsonStore, err := store.InitJsonStore()
	store.FailOnErr(err)
	defer jsonStore.Close()
	store.FailOnErr(jsonStore.Clear())
	for n := 0; n < manyRowsForQuery; n++ {
		store.FailOnErr(jsonStore.Insert("path", strconv.Itoa(n), expectedValue))
	}
	// hstore
	hstore, err := store.InitHStore()
	store.FailOnErr(err)
	defer hstore.Close()
	store.FailOnErr(hstore.Clear())
	for n := 0; n < manyRowsForQuery; n++ {
		store.FailOnErr(hstore.Insert("path", strconv.Itoa(n), expectedValue))
	}
}

var counter int64

func BenchmarkJsonStoreInsert(b *testing.B) {
	db, err := store.InitJsonStore()
	store.FailOnErr(err)
	defer db.Close()
	for n := 0; n < b.N; n++ {
		counter++
		store.FailOnErr(db.Insert("BenchmarkJsonStoreInsert", strconv.FormatInt(counter, 10), expectedValue))
	}
}

func BenchmarkJsonStoreInsertParallel(b *testing.B) {
	db, err := store.InitJsonStore()
	store.FailOnErr(err)
	defer db.Close()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			store.FailOnErr(db.Insert("BenchmarkJsonStoreInsertParallel", strconv.FormatInt(n, 10), expectedValue))
		}
	})
}

func BenchmarkHStoreInsert(b *testing.B) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()
	for n := 0; n < b.N; n++ {
		counter++
		store.FailOnErr(db.Insert("BenchmarkHStoreInsert", strconv.FormatInt(counter, 10), expectedValue))
	}
}

func BenchmarkHStoreInsertParallel(b *testing.B) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			store.FailOnErr(db.Insert("BenchmarkHStoreInsertParallel", strconv.FormatInt(n, 10), expectedValue))
		}
	})
}

func BenchmarkJsonStoreQueryValue(b *testing.B) {
	db, err := store.InitJsonStore()
	store.FailOnErr(err)
	defer db.Close()
	for n := 0; n < b.N; n++ {
		key := strconv.Itoa((n % manyRowsForQuery))
		actualValue, err := db.QueryValue("path", key)
		store.FailOnErr(err)
		if len(actualValue) == 0 {
			b.Fatalf("key: %s - Wrong result %v", key, actualValue)
		}
	}
}

func BenchmarkJsonStoreQueryValueParallel(b *testing.B) {
	db, err := store.InitJsonStore()
	store.FailOnErr(err)
	defer db.Close()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := strconv.Itoa((counter % manyRowsForQuery))
			actualValue, err := db.QueryValue("path", key)
			if len(actualValue) == 0 {
				b.Fatalf("key: %s - Wrong result %v", key, actualValue)
			}
			store.FailOnErr(err)
			counter = (counter + 1) % manyRowsForQuery
		}
	})
}

func BenchmarkHStoreQueryValue(b *testing.B) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()
	for n := 0; n < b.N; n++ {
		key := strconv.Itoa((n % manyRowsForQuery))
		actualValue, err := db.QueryValue("path", key)
		store.FailOnErr(err)
		if len(actualValue) == 0 {
			b.Fatalf("key: %s - Wrong result %v", key, actualValue)
		}
	}
}

func BenchmarkHStoreQueryValueParallel(b *testing.B) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := strconv.Itoa((counter % manyRowsForQuery))
			actualValue, err := db.QueryValue("path", key)
			if len(actualValue) == 0 {
				b.Fatalf("key: %s - Wrong result %v", key, actualValue)
			}
			store.FailOnErr(err)
			counter = (counter + 1) % manyRowsForQuery
		}
	})
}

func BenchmarkHStoreToJsonQueryValue(b *testing.B) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()
	for n := 0; n < b.N; n++ {
		key := strconv.Itoa((n % manyRowsForQuery))
		actualValue, err := db.QueryValueHStoreToJson("path", key)
		store.FailOnErr(err)
		if len(actualValue) == 0 {
			b.Fatalf("key: %s - Wrong result %v", key, actualValue)
		}
	}
}

func BenchmarkHStoreToJsonQueryValueParallel(b *testing.B) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := strconv.Itoa((counter % manyRowsForQuery))
			actualValue, err := db.QueryValueHStoreToJson("path", key)
			if len(actualValue) == 0 {
				b.Fatalf("key: %s - Wrong result %v", key, actualValue)
			}
			store.FailOnErr(err)
			counter = (counter + 1) % manyRowsForQuery
		}
	})
}
