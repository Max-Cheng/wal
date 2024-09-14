package wal

import "sync"

var (
	// batchPool is a pool of Batch objects.
	batchPool = sync.Pool{
		New: func() interface{} {
			return &Batch{}
		},
	}
)

// Batch of entries. Used to write multiple entries at once using WriteBatch().
type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	index uint64
	size  int
}

// Write an entry to the batch
func (b *Batch) Write(index uint64, data []byte) {
	b.entries = append(b.entries, batchEntry{index, len(data)})
	b.datas = append(b.datas, data...)
}

// Clear the batch for reuse.
func (b *Batch) Clear() {
	b.entries = b.entries[:0]
	b.datas = b.datas[:0]
}

func GetBatch() *Batch {
	batch := batchPool.Get().(*Batch)
	batch.Clear()
	return batch
}

func PutBatch(batch *Batch) {
	batchPool.Put(batch)
}
