package engine

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/stretchr/testify/require"
	"github.com/pkg/errors"
	"github.com/tysontate/gommap"
)

var (
	ErrIndexCorrupt = errors.New("corrupt index file")
	ErrSegmentNotFound = errors.New("segment not found")
	Encoding           = binary.BigEndian
)

const (
	offsetWidth  = 4
	offsetOffset = 0

	positionWidth  = 4
	positionOffset = offsetWidth

	entryWidth = offsetWidth + positionWidth
	logNameFormat   = "%020d.log"
	indexNameFormat = "%020d.index"
)


type index struct {
	options
	mmap     gommap.MMap
	file     *os.File
	mu       sync.RWMutex
	position int64
}

type Entry struct {
	Offset   int64
	Position int64
}

// relEntry 相对于基准偏移的输入
type relEntry struct {
	Offset   int32
	Position int32
}

func newRelEntry(e Entry, baseOffset int64) relEntry {
	return relEntry{
		Offset:   int32(e.Offset - baseOffset),
		Position: int32(e.Position),
	}
}

func (rel relEntry) fill(e *Entry, baseOffset int64) {
	e.Offset = baseOffset + int64(rel.Offset)
	e.Position = int64(rel.Position)
}

type options struct {
	path       string
	bytes      int64
	baseOffset int64
}

func roundDown(total, factor int64) int64 {
        return factor * (total / factor)
}

func newIndex(opts options) (idx *index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}
	if opts.path == "" {
		return nil, errors.New("path is empty")
	}
	idx = &index{
		options: opts,
	}
	idx.file, err = os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	fi, err := idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	} else if fi.Size() > 0 {
		idx.position = fi.Size()
	}
	if err := idx.file.Truncate(roundDown(opts.bytes, entryWidth)); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "mmap file failed")
	}
	return idx, nil
}

func (idx *index) WriteEntry(entry Entry) (err error) {
	b := new(bytes.Buffer)
	relEntry := newRelEntry(entry, idx.baseOffset)
	if err = binary.Write(b, Encoding, relEntry); err != nil {
		return errors.Wrap(err, "binary write failed")
	}
	idx.WriteAt(b.Bytes(), idx.position)
	idx.mu.Lock()
	idx.position += entryWidth
	idx.mu.Unlock()
	return nil
}

func (idx *index) ReadEntry(e *Entry, offset int64) error {
	p := make([]byte, entryWidth)
	idx.ReadAt(p, offset)
	b := bytes.NewReader(p)
	rel := &relEntry{}
	err := binary.Read(b, Encoding, rel)
	if err != nil {
		return errors.Wrap(err, "binary read failed")
	}
	idx.mu.RLock()
	rel.fill(e, idx.baseOffset)
	idx.mu.RUnlock()
	return nil
}

func (idx *index) ReadAt(p []byte, offset int64) (n int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return copy(p, idx.mmap[offset:offset+entryWidth])
}

func (idx *index) Write(p []byte) (n int, err error) {
	return idx.WriteAt(p, idx.position), nil
}

func (idx *index) WriteAt(p []byte, offset int64) (n int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return copy(idx.mmap[offset:offset+entryWidth], p)
}

func (idx *index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.file.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.Wrap(err, "mmap sync failed")
	}
	return nil
}

func (idx *index) Close() (err error) {
	if err = idx.Sync(); err != nil {
		return
	}
	if err = idx.file.Truncate(idx.position); err != nil {
		return
	}
	return idx.file.Close()
}

func (idx *index) Name() string {
	return idx.file.Name()
}

func (idx *index) TruncateEntries(number int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if int64(number*entryWidth) > idx.position {
		return errors.New("bad truncate number")
	}
	idx.position = int64(number * entryWidth)
	return nil
}

func (idx *index) SanityCheck() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position == 0 {
		return nil
	} else if idx.position%entryWidth != 0 {
		return ErrIndexCorrupt
	} else {
		//read last entry
		entry := new(Entry)
		if err := idx.ReadEntry(entry, idx.position-entryWidth); err != nil {
			return err
		}
		if entry.Offset < idx.baseOffset {
			return ErrIndexCorrupt
		}
		return nil
	}
}
/* 
func Mmap_test () {
	t := *new(require.TestingT)
	path := filepath.Join("", fmt.Sprintf(indexNameFormat, rand.Int63()))
	totalEntries := rand.Intn(10) + 10
	//case for roundDown
	bytes := int64(totalEntries*entryWidth + 1)

	idx, err := newIndex(options{
		path:  path,
		bytes: bytes,
	})
	if err != nil {
		panic(err)
	}
	// defer os.Remove(path)

	stat, err := idx.file.Stat()
	if err != nil {
		panic(err)
	}
	require.Equal(t, roundDown(bytes, entryWidth), stat.Size())
	entries := []Entry{}
	for i := 0; i < totalEntries; i++ {
		entries = append(entries, Entry{
			int64(i),
			int64(i * 5),
		})
	}
	for _, e := range entries {
		if err := idx.WriteEntry(e); err != nil {
			panic(err)
		}
	}
	if err = idx.Sync(); err != nil {
		panic(err)
	}
	act := &Entry{}
	for i, exp := range entries {
		if err = idx.ReadEntry(act, int64(i*entryWidth)); err != nil {
			panic(err)
		}
		require.Equal(t, exp.Offset, act.Offset, act)
		require.Equal(t, exp.Position, act.Position, act)
	}
	require.Equal(t, nil, idx.SanityCheck())

	//dirty data
	idx.position++
	require.NotEqual(t, nil, idx.SanityCheck())

	idx.position--
	if err = idx.Close(); err != nil {
		panic(err)
	}
	if stat, err = os.Stat(path); err != nil {
		panic(err)
	}

	require.Equal(t, int64(totalEntries*entryWidth), stat.Size())



}
*/
