package clockcache

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// CLOCK based approximate LRU storing mappings from strings to
// int64s designed for concurrent usage.
// It is sharded into 127 blocks, each of which can be
// locked independently. Gets only require a read lock on the
// individual block the element can be found in. When the cache is
// not full, Insert only requires a write lock on the individual
// block. Eviction locks blocks one at a time looking for a value
// that's valid to evict/
type Cache struct {
	lock sync.RWMutex
	// stores indexes into storage
	elements map[interface{}]*Element
	storage  []Element

	// CLOCK sweep state, must have write lock
	next int
}

type Element struct {
	// The value stored with this element.
	key   interface{}
	Value interface{}

	//CLOCK marker if this is recently used
	used uint32

	// pad Elements out to be cache aligned
	_padding [24]byte
}

func WithMax(max uint64) *Cache {
	if max < 1 {
		panic("must have max greater than 0")
	}
	return &Cache{
		elements: make(map[interface{}]*Element, max),
		storage:  make([]Element, 0, max),
	}
}

// Insert a key/value mapping into the cache if the key is not already present
// returns the value present in the map, and true if it is newley inserted
func (self *Cache) Insert(key interface{}, value interface{}) (canonicalValue interface{}, inserted bool) {
	self.lock.Lock()
	defer self.lock.Unlock()

	_, canonicalValue, inserted = self.insert(key, value)
	return
}

// Insert a bactch of keys with their corresponding values.
// This function will _overwrite_ the keys and values slices with their
// canonical versions.
func (self *Cache) InsertBatch(keys []interface{}, values []interface{}) {
	if len(keys) != len(values) {
		panic(fmt.Sprintf("keys and values are not the same len. %d keys, %d values", len(keys), len(values)))
	}
	values = values[:len(keys)]
	self.lock.Lock()
	defer self.lock.Unlock()

	for idx := range keys {
		keys[idx], values[idx], _ = self.insert(keys[idx], values[idx])
	}
	return
}

func (self *Cache) insert(key interface{}, value interface{}) (canonicalKey interface{}, canonicalValue interface{}, inserted bool) {
	elem, present := self.elements[key]
	if present {
		return elem.key, elem.Value, false
	}

	var insertLocation *Element
	if len(self.storage) >= cap(self.storage) {
		insertLocation = self.evict()
		*insertLocation = Element{key: key, Value: value}
	} else {
		self.storage = append(self.storage, Element{key: key, Value: value})
		insertLocation = &self.storage[len(self.storage)-1]
	}

	self.elements[key] = insertLocation
	return key, value, true
}

func (self *Cache) evict() (insertPtr *Element) {
	for {
		insertLocation, evicted := self.tryEvict()
		if evicted {
			return insertLocation
		}
	}
}

func (self *Cache) tryEvict() (insertPtr *Element, evicted bool) {
	if self.next >= len(self.storage) {
		self.next = 0
	}

	evicted = false
	reachedEnd := false
	for !evicted && !reachedEnd {
		elem := &self.storage[self.next]
		if elem.used != 0 {
			elem.used = 0
		} else {
			insertPtr = elem
			key := elem.key
			delete(self.elements, key)
			evicted = true
		}
		self.next += 1
		reachedEnd = self.next >= len(self.storage)
	}

	return
}

// tries to get a batch of keys and store the corresponding values is valuesOut
// returns the number of keys that were actually found.
// NOTE: this function does _not_ preserve the order of keys; the first numFound
//       keys will be the keys whose values are present, while the remainder
//       will be the keys not present in the cache
func (self *Cache) GetValues(keys []interface{}, valuesOut []interface{}) (numFound int) {
	if len(keys) != len(valuesOut) {
		panic(fmt.Sprintf("keys and values are not the same len. %d keys, %d values", len(keys), len(valuesOut)))
	}
	valuesOut = valuesOut[:len(keys)]
	n := len(keys)
	idx := 0

	self.lock.RLock()
	defer self.lock.RUnlock()

	for idx < n {
		value, found := self.get(keys[idx])
		if !found {
			if n == 0 {
				return 0
			}
			// no value found for key, swap the key with the last element, and shrink n
			n -= 1
			keys[n], keys[idx] = keys[idx], keys[n]
			continue
		}
		valuesOut[idx] = value
		idx += 1
	}
	return n
}

func (self *Cache) Get(key interface{}) (interface{}, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.get(key)
}

func (self *Cache) get(key interface{}) (interface{}, bool) {

	elem, present := self.elements[key]
	if !present {
		return 0, false
	}

	if atomic.LoadUint32(&elem.used) == 0 {
		atomic.StoreUint32(&elem.used, 1)
	}

	return elem.Value, true
}

func (self *Cache) Unmark(key string) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()

	elem, present := self.elements[key]
	if !present {
		return false
	}

	if atomic.LoadUint32(&elem.used) != 0 {
		atomic.StoreUint32(&elem.used, 0)
	}

	return true
}

func (self *Cache) Len() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return len(self.storage)
}
