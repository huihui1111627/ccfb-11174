package ring

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// RecordLatency 把一次端到端延迟（纳秒）计入共享直方图。
// 桶 i 覆盖 [2^(i-1), 2^i) 纳秒，可被任何进程并发调用。
func (r *Ring) RecordLatency(ns int64) {
	b := 0
	if ns > 0 {
		b = bits.Len64(uint64(ns))
	}
	if b > HistBuckets-1 {
		b = HistBuckets - 1
	}
	atomic.AddUint64(r.u64(offHistogram+b*8), 1)
}

// Histogram 返回延迟直方图的快照。
func (r *Ring) Histogram() [HistBuckets]uint64 {
	var h [HistBuckets]uint64
	for i := range h {
		h[i] = atomic.LoadUint64(r.u64(offHistogram + i*8))
	}
	return h
}

// ErrorEntry 是一条异常记录。
type ErrorEntry struct {
	Time     int64  `json:"ts"`       // UnixNano
	Code     uint32 `json:"code"`     // ErrChecksum / ErrOrder / ErrLagged
	Consumer int32  `json:"consumer"` // 消费者 id，-1 表示生产者
	A        uint64 `json:"a"`        // 上下文，如期望序号
	B        uint64 `json:"b"`        // 上下文，如实际序号
}

// LogError 追加一条异常记录到共享异常环，任何进程可调用。
func (r *Ring) LogError(code uint32, consumerID int32, a, b uint64) {
	n := atomic.AddUint64(r.u64(offErrCount), 1)
	e := offErrRing + int((n-1)%ErrRingCap)*errEntrySize
	atomic.StoreUint64(r.u64(e+8), uint64(a))
	atomic.StoreUint64(r.u64(e+16), uint64(b))
	atomic.StoreUint32(r.u32(e+20), uint32(consumerID))
	atomic.StoreUint32(r.u32(e+24), code)
	// 时间戳最后写，读到时间戳即认为整条记录有效。
	atomic.StoreUint64(r.u64(e), uint64(time.Now().UnixNano()))
}

// ErrorCount 返回异常记录总数（可能超过环容量，旧记录被覆盖）。
func (r *Ring) ErrorCount() uint64 { return atomic.LoadUint64(r.u64(offErrCount)) }

// RecentErrors 返回最近的异常记录，新的在前，最多 max 条。
func (r *Ring) RecentErrors(max int) []ErrorEntry {
	n := r.ErrorCount()
	if n == 0 || max <= 0 {
		return nil
	}
	count := n
	if count > ErrRingCap {
		count = ErrRingCap
	}
	if uint64(max) < count {
		count = uint64(max)
	}
	out := make([]ErrorEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		e := offErrRing + int((n-1-i)%ErrRingCap)*errEntrySize
		out = append(out, ErrorEntry{
			Time:     int64(atomic.LoadUint64(r.u64(e))),
			A:        atomic.LoadUint64(r.u64(e + 8)),
			B:        atomic.LoadUint64(r.u64(e + 16)),
			Consumer: int32(atomic.LoadUint32(r.u32(e + 20))),
			Code:     atomic.LoadUint32(r.u32(e + 24)),
		})
	}
	return out
}
