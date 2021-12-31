package engine

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type Class struct {
	Size int
	Cap  int
}

func Allocator(maxSize int, classes []Class) (alloc func(int) []byte, free func([]byte)) {
	type Pool struct {
		*sync.Mutex
		ptrs *[]uintptr
		len  *int
		base uintptr
		max  uintptr
	}

	type PoolSet struct {
		pools  []Pool
		serial *int64
		base   uintptr
		max    uintptr
	}

	const Align = 64

	shard := int64(runtime.NumCPU()) /*
	   每个 class 使用多个池，可以减少锁争用
	   有一个 percpu.Sharded 的提案比这个方案更好，不过还没实现：
	   https://github.com/golang/proposal/blob/master/design/18802-percpu-sharded.md
	*/
	poolSets := make([]PoolSet, len(classes))
	for i := range poolSets {
		class := classes[i]
		var serial int64
		poolSet := PoolSet{
			serial: &serial,
		}
		spanLen := int(math.Ceil(float64(class.Size)/float64(Align))) * Align /*
		   传入的 class 可能不是对齐到 64 byte，这里调整为每个对象都对齐
		*/
		for range make([]struct{}, shard) {
			base, err := syscall.Mmap(
				0,
				0,
				spanLen*class.Cap,
				syscall.PROT_READ|syscall.PROT_WRITE,
				syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS,
			) /*
			   在 runtime 管理的堆之外分配内存，GC 对这部分内存是忽略的
			*/
			if err != nil {
				panic(err)
			}
			h := (*reflect.SliceHeader)(unsafe.Pointer(&base))
			baseAddr := h.Data
			var ptrs []uintptr
			for i := 0; i < class.Cap; i++ {
				ptrs = append(ptrs, baseAddr+uintptr(i*spanLen))
			}
			length := class.Cap
			poolSet.pools = append(poolSet.pools, Pool{
				Mutex: new(sync.Mutex),
				ptrs:  &ptrs,
				len:   &length,
				base:  ptrs[0],
				max:   ptrs[len(ptrs)-1],
			})
		}
		sort.Slice(poolSet.pools, func(i, j int) bool {
			return poolSet.pools[i].base < poolSet.pools[j].base
		}) /*
		   回收的时候，只知道对象的地址，所以要遍历所有池以决定放回哪里
		   因为同一个 class，池的地址范围不会重叠，所以可以用二分查找，所以这里先排序
		   不同 class，地址范围可能重叠，只能顺序遍历
		*/
		poolSet.base = poolSet.pools[0].base
		poolSet.max = poolSet.pools[len(poolSet.pools)-1].max
		poolSets[i] = poolSet
	}

	var funcs []func() []byte
	for i := 0; i < maxSize; i++ {
		i := i
		poolSet := poolSets[0]
		for idx, class := range classes[1:] {
			if i > class.Size {
				break
			} /*
			   使用最小的合适的 class，所以要求传入的 classes 是从大到小排序的
			*/
			poolSet = poolSets[idx]
		}

		funcs = append(funcs, func() []byte {
			pool := poolSet.pools[atomic.AddInt64(poolSet.serial, 1)%shard]
			pool.Lock()
			if *pool.len > 0 {
				// 如果池里有空闲对象，就使用
				ptr := (*pool.ptrs)[*pool.len-1]
				*pool.len = *pool.len - 1
				pool.Unlock()
				var bs []byte
				h := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
				h.Data = ptr
				h.Len = i
				h.Cap = i
				for i := range bs {
					bs[i] = 0
				} /*
				   编译器会将这个 for 语句优化成 memset
				   如果不需要初始化元素为 0，例如用作缓冲区时，可以去掉，减少一些开销
				*/
				return bs
			}
			pool.Unlock()
			return make([]byte, i) /*
			   如果池里没有空闲对象，就在堆上分配
			   如果频繁触发，可以考虑增加对应 class 的池容量
			*/
		})

	}

	return func(n int) []byte {
			return funcs[n]()
		},
		func(bs []byte) {
			ptr := (*reflect.SliceHeader)(unsafe.Pointer(&bs)).Data
			// 遍历池以决定回收的位置
			for _, set := range poolSets {
				if ptr < set.base || ptr > set.max {
					continue
				}
				pools := set.pools
				// 因为同一个 class/set 的池在前面已经排序了，所以可以用二分查找
				for len(pools) > 0 {
					i := len(pools) / 2
					pool := pools[i]
					if ptr < pool.base {
						pools = pools[:i]
						continue
					} else if ptr > pool.max {
						pools = pools[i+1:]
						continue
					}
					pool.Lock()
					(*pool.ptrs)[*pool.len] = ptr
					*pool.len = *pool.len + 1
					pool.Unlock()
					return
				}
			}
		}
}
/* 
func Allocator_test() {
	alloc, free := Allocator(
		4096, /*
		   最大分配大小
		*/
		[]Class{
			{4096, 512},
			{2048, 512},
			{1024, 512},
			{512, 512},
			{128, 512},
		}, /*
		   class 的大小和容量需要根据具体的负载来决定
		   需要按大小降序排列
		*/
	)

	n := int(1e5)
	t0 := time.Now()
	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for range make([]struct{}, runtime.NumCPU()) {
		go func() {
			for i := 0; i < n; i++ {
				// 分配
				bs := alloc(1 + rand.Intn(4095))
				// 回收
				free(bs)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("%v\n", time.Since(t0)/time.Duration(n*runtime.NumCPU()))
}
*/
