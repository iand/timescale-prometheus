package clockcache

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"unsafe"
)

func TestWriteAndGetOnCache(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	cache.Insert("1", 1)
	val, found := cache.Get("1")

	// then
	if !found {
		t.Error("no value found")
	}
	if val != 1 {
		t.Errorf("expected %d found %d", 1, val)
	}
}

func TestEntryNotFound(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	val, found := cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}

	cache.Insert("key", 1)

	val, found = cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		cache.Insert(key, int64(i))
		if i != 5 {
			cache.Get(key)
		}
	}

	cache.Insert("100", 100)
	cache.Get("100")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.Get(key)
		if i != 5 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, got %d", i, val)
		} else if i == 5 && found {
			t.Errorf("5 not evicted")
		}
		if i == 2 {
			cache.Unmark(key)
		}
	}

	val, found := cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}

	cache.Insert("101", 101)
	cache.Get("101")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.Get(key)
		if i != 5 && i != 2 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, (found: %v) got %d", i, found, val)
		} else if (i == 5 || i == 2) && found {
			t.Errorf("%d not evicted", i)
		}
	}

	val, found = cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}
	val, found = cache.Get("101")
	if !found || val != 101 {
		t.Errorf("missing value 101, got %d", val)
	}
}

func printCache(cache *Cache, t *testing.T) {
	str := "["
	for k, v := range cache.elements {
		str = fmt.Sprintf("%s\n\t%v: %v, ", str, k, v)
	}
	t.Logf("%s]", str)
}

func TestCacheGetRandomly(t *testing.T) {
	t.Parallel()

	cache := WithMax(10000)
	var wg sync.WaitGroup
	var ntest = 800000
	wg.Add(2)
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63() % 20000
			key := fmt.Sprintf("%d", r)
			cache.Insert(key, r+1)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63()
			key := fmt.Sprintf("%d", r)
			if val, found := cache.Get(key); found && val != r+1 {
				t.Errorf("got %s ->\n %x\n expected:\n %x\n ", key, val, r+1)
			}
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestBatch(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)

	cache.InsertBatch([]interface{}{3, 6, 9, 12}, []interface{}{4, 7, 10, 13})

	keys := []interface{}{1, 2, 3, 6, 9, 12, 13}
	vals := make([]interface{}, len(keys))
	numFound := cache.GetValues(keys, vals)

	if numFound != 4 {
		t.Errorf("found incorrect number of values: expected 4, found %d\n\tkeys: %v\n\t%v", numFound, keys, vals)
	}

	expectedKeys := []interface{}{12, 9, 3, 6, 2, 13, 1}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("unexpected keys:\nexpected\n\t%v\nfound\n\t%v", keys, expectedKeys)
	}

	expectedVals := []interface{}{13, 10, 4, 7, nil, nil, nil}
	if !reflect.DeepEqual(vals, expectedVals) {
		t.Errorf("unexpected values:\nexpected\n\t%v\nfound\n\t%v", expectedVals, vals)
	}
}

func TestElementCacheAligned(t *testing.T) {
	elementSize := unsafe.Sizeof(Element{})
	if elementSize%64 != 0 {
		t.Errorf("unaligned element size: %d", elementSize)
	}
	if elementSize != 64 {
		t.Errorf("unexpected element size: %d", elementSize)
	}
}

var bval interface{}

func BenchmarkInsertUnderCapacity(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))
			zipf := rand.NewZipf(rng, 1.07, 1, 1e9)

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = zipf.Uint64(), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n))
			b.ReportAllocs()
			b.ResetTimer()

			inserts := 0
			var inserted bool
			for i := 0; i < b.N; i++ {
				bval, inserted = cache.Insert(keys[i%n], vals[i%n])
				if inserted {
					inserts++
				}
				b.ReportMetric(float64(inserts)/float64(b.N), "inserts/op")
			}
		})
	}
}

func BenchmarkInsertOverCapacity(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))
			zipf := rand.NewZipf(rng, 1.07, 1, 1e9)

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = zipf.Uint64(), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n / 4))
			b.ReportAllocs()
			b.ResetTimer()

			inserts := 0
			var inserted bool
			for i := 0; i < b.N; i++ {
				bval, inserted = cache.Insert(keys[i%n], vals[i%n])
				if inserted {
					inserts++
				}
			}
			b.ReportMetric(float64(inserts)/float64(b.N), "inserts/op")
		})
	}
}

func BenchmarkInsertConcurrent(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))
			zipf := rand.NewZipf(rng, 1.07, 1, 1e9)

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = zipf.Uint64(), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n / 4))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var val interface{}
				i := 0
				for pb.Next() {
					val, _ = cache.Insert(keys[i%n], vals[i%n])
					i++
				}
				bval = val
			})
		})
	}
}

func BenchmarkMembership(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = rng.Intn(1e9), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n))
			cache.InsertBatch(keys, vals)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bval, _ = cache.Get(i % n)
			}
		})
	}
}

func BenchmarkNotFound(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = rng.Intn(1e9), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n))
			cache.InsertBatch(keys, vals)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bval, _ = cache.Get(n + i%n)
			}
		})
	}
}
