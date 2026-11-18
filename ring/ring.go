// Package ring 实现一个放在共享内存（mmap 文件）里的跨进程无锁环形队列。
//
// 设计要点：
//   - 单生产者、多消费者，生产者和消费者是不同的进程，共同映射同一个内存文件。
//   - 数据区是一个字节环，每条记录 = 16 字节头 {seq, len, flags} + 变长负载，
//     按 16 字节对齐。尾部放不下时写一条 SKIP 标记记录绕回开头。
//   - 每条记录头里带全局递增的序号 seq：生产者先写负载、再 release 写 seq；
//     消费者 acquire 读到 seq 等于期望值即认为记录写完整，全程无锁。
//   - 写游标独占一个缓存行；每个消费者游标按配置独占缓存行（或紧凑排列，
//     用于对比伪共享带来的性能差别）。
//   - 生产者覆盖槽位前要等最慢的活跃消费者；消费者落后太多时按配置
//     等待（PolicyWait）或把它标记为掉队并跳过（PolicySkip）。
package ring

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	Magic   uint64 = 0x4b52494e47310001 // "KRING\x10\x00\x01"
	Version uint32 = 1

	CacheLine    = 64
	MaxConsumers = 32

	// RecordHeaderSize 记录头长度：seq(8) + len(4) + flags(4)。
	RecordHeaderSize = 16
	// RecordAlign 记录对齐。取 16 可保证环尾剩余空间要么是 0，
	// 要么 >= 16，一定放得下一条 SKIP 标记。
	RecordAlign = 16

	// FlagSkip 标记这是一条填充记录，消费者直接跳过，不占序号。
	FlagSkip uint32 = 1

	// PolicyWait 消费者落后时生产者等待。
	PolicyWait = 0
	// PolicySkip 消费者落后时生产者把它标记为掉队并继续写。
	PolicySkip = 1
)

// 共享内存布局（字节偏移）：
//
//	0     头部（魔数/版本/容量/策略/消费者数/就绪计数），占 1 个缓存行
//	64    生产者行：写游标、消息计数、字节计数、完成序号，独占缓存行
//	128   最新记录指针（seqlock：version/pos/seq），供掉队消费者重同步
//	256   消费者游标数组：stride=64（填充）或 8（不填充），预留 32*64
//	2304  消费者信息数组：每个消费者 64 字节（掉队标记、消费计数等）
//	4352  延迟直方图：64 个桶
//	4864  异常计数（独占缓存行）
//	4928  异常记录环：128 条 * 32 字节
//	16384 数据区（页对齐），capacity 字节
const (
	offMagic        = 0
	offVersion      = 8
	offPadMode      = 12
	offCapacity     = 16
	offPolicy       = 24
	offNumConsumers = 28
	offReadyCount   = 32

	offProducerLine = 64
	offWriteCursor  = offProducerLine + 0
	offMsgCount     = offProducerLine + 8
	offByteCount    = offProducerLine + 16
	offDoneSeq      = offProducerLine + 24

	offLatestLine = 128
	offLatestVer  = offLatestLine + 0
	offLatestPos  = offLatestLine + 8
	offLatestSeq  = offLatestLine + 16

	offCursors   = 256
	cursorArea   = MaxConsumers * CacheLine
	offInfos     = offCursors + cursorArea
	infoArea     = MaxConsumers * CacheLine
	offHistogram = offInfos + infoArea
	// HistBuckets 延迟直方图桶数，桶 i 表示延迟 < 2^i 纳秒。
	HistBuckets = 64
	offErrCount = offHistogram + HistBuckets*8
	offErrRing  = offErrCount + CacheLine
	// ErrRingCap 异常记录环容量。
	ErrRingCap   = 128
	errEntrySize = 32
	// DataOffset 数据区起始偏移。
	DataOffset = 16384
)

// 消费者信息槽内偏移（每槽 64 字节）。
const (
	infoFlags    = 0 // u32，bit0 = 掉队
	infoConsumed = 8 // u64，已消费记录数
	infoLagCount = 16 // u64，掉队/重同步次数
)

const flagLagging uint32 = 1

// 异常记录码。
const (
	ErrChecksum uint32 = 1 // 校验和错误
	ErrOrder    uint32 = 2 // 序号不连续
	ErrLagged   uint32 = 3 // 消费者掉队并重同步
)

// Ring 是映射到共享内存文件上的环形队列句柄。
type Ring struct {
	f            *os.File
	buf          []byte
	capacity     uint64
	mask         uint64
	numConsumers int
	policy       int
	padded       bool
	cursorStride int
}

// Create 创建并初始化一个共享内存环形队列文件。
// capacity 必须是 >= 4096 的 2 的幂。padded 决定消费者游标是否按缓存行填充。
func Create(path string, capacity uint64, numConsumers, policy int, padded bool) (*Ring, error) {
	if capacity < 4096 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("ring: 容量必须是 >= 4096 的 2 的幂, 得到 %d", capacity)
	}
	if numConsumers < 1 || numConsumers > MaxConsumers {
		return nil, fmt.Errorf("ring: 消费者数量必须在 1..%d 之间", MaxConsumers)
	}
	if policy != PolicyWait && policy != PolicySkip {
		return nil, fmt.Errorf("ring: 未知策略 %d", policy)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	size := DataOffset + int(capacity)
	if err := syscall.Ftruncate(int(f.Fd()), int64(size)); err != nil {
		f.Close()
		return nil, err
	}
	buf, err := syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	r := &Ring{
		f: f, buf: buf,
		capacity: capacity, mask: capacity - 1,
		numConsumers: numConsumers, policy: policy, padded: padded,
	}
	r.cursorStride = 8
	if padded {
		r.cursorStride = CacheLine
	}
	// 先写配置，最后 release 写魔数，打开方看到魔数即表示头部就绪。
	atomic.StoreUint32(r.u32(offVersion), Version)
	if padded {
		atomic.StoreUint32(r.u32(offPadMode), 1)
	}
	atomic.StoreUint64(r.u64(offCapacity), capacity)
	atomic.StoreUint32(r.u32(offPolicy), uint32(policy))
	atomic.StoreUint32(r.u32(offNumConsumers), uint32(numConsumers))
	atomic.StoreUint64(r.u64(offMagic), Magic)
	return r, nil
}

// Open 映射一个已存在的共享内存环形队列，等待头部初始化完成。
func Open(path string) (*Ring, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	buf, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	r := &Ring{f: f, buf: buf}
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadUint64(r.u64(offMagic)) != Magic {
		if time.Now().After(deadline) {
			r.Close()
			return nil, fmt.Errorf("ring: 等待头部初始化超时")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if v := atomic.LoadUint32(r.u32(offVersion)); v != Version {
		r.Close()
		return nil, fmt.Errorf("ring: 版本不匹配 %d", v)
	}
	r.capacity = atomic.LoadUint64(r.u64(offCapacity))
	r.mask = r.capacity - 1
	r.numConsumers = int(atomic.LoadUint32(r.u32(offNumConsumers)))
	r.policy = int(atomic.LoadUint32(r.u32(offPolicy)))
	r.padded = atomic.LoadUint32(r.u32(offPadMode)) == 1
	r.cursorStride = 8
	if r.padded {
		r.cursorStride = CacheLine
	}
	return r, nil
}

// Close 解除映射并关闭文件。
func (r *Ring) Close() error {
	if r.buf != nil {
		syscall.Munmap(r.buf)
		r.buf = nil
	}
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

func (r *Ring) u64(off int) *uint64 { return (*uint64)(unsafe.Pointer(&r.buf[off])) }
func (r *Ring) u32(off int) *uint32 { return (*uint32)(unsafe.Pointer(&r.buf[off])) }

func (r *Ring) cursorOff(i int) int { return offCursors + i*r.cursorStride }
func (r *Ring) infoOff(i int) int   { return offInfos + i*CacheLine }

// Capacity 返回数据区容量（字节）。
func (r *Ring) Capacity() uint64 { return r.capacity }

// NumConsumers 返回配置的消费者数量。
func (r *Ring) NumConsumers() int { return r.numConsumers }

// Padded 返回消费者游标是否按缓存行填充。
func (r *Ring) Padded() bool { return r.padded }

// Policy 返回掉队策略（PolicyWait / PolicySkip）。
func (r *Ring) Policy() int { return r.policy }

// ConsumerReady 由消费者进程在就绪后调用一次。
func (r *Ring) ConsumerReady() { atomic.AddUint32(r.u32(offReadyCount), 1) }

// ReadyCount 返回已就绪的消费者数量。
func (r *Ring) ReadyCount() int { return int(atomic.LoadUint32(r.u32(offReadyCount))) }

// WriteCursor 返回生产者写游标（虚拟字节位置，单调递增）。
func (r *Ring) WriteCursor() uint64 { return atomic.LoadUint64(r.u64(offWriteCursor)) }

// MsgCount 返回已发布的消息数。
func (r *Ring) MsgCount() uint64 { return atomic.LoadUint64(r.u64(offMsgCount)) }

// ByteCount 返回已发布的负载字节数。
func (r *Ring) ByteCount() uint64 { return atomic.LoadUint64(r.u64(offByteCount)) }

// DoneSeq 返回生产者完成标记（最后一条记录的序号，0 表示未完成）。
func (r *Ring) DoneSeq() uint64 { return atomic.LoadUint64(r.u64(offDoneSeq)) }

// ConsumerCursor 返回消费者 i 的读游标（虚拟字节位置）。
func (r *Ring) ConsumerCursor(i int) uint64 {
	return atomic.LoadUint64(r.u64(r.cursorOff(i)))
}

// ConsumerLagging 返回消费者 i 是否被标记为掉队。
func (r *Ring) ConsumerLagging(i int) bool {
	return atomic.LoadUint32(r.u32(r.infoOff(i)+infoFlags))&flagLagging != 0
}

// ConsumerLagCount 返回消费者 i 的掉队/重同步次数。
func (r *Ring) ConsumerLagCount(i int) uint64 {
	return atomic.LoadUint64(r.u64(r.infoOff(i) + infoLagCount))
}

// ConsumerConsumed 返回消费者 i 已消费的记录数。
func (r *Ring) ConsumerConsumed(i int) uint64 {
	return atomic.LoadUint64(r.u64(r.infoOff(i) + infoConsumed))
}

// MinActiveCursor 返回未掉队消费者中最小的读游标；全部掉队时返回写游标。
func (r *Ring) MinActiveCursor() uint64 {
	min := ^uint64(0)
	for i := 0; i < r.numConsumers; i++ {
		if r.ConsumerLagging(i) {
			continue
		}
		if c := r.ConsumerCursor(i); c < min {
			min = c
		}
	}
	if min == ^uint64(0) {
		return r.WriteCursor()
	}
	return min
}

func alignUp(n uint64) uint64 { return (n + RecordAlign - 1) &^ (RecordAlign - 1) }
