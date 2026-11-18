package ring

import (
	"sync/atomic"
)

// Producer 是单生产者句柄。同一时刻只能有一个生产者进程使用。
type Producer struct {
	r       *Ring
	wait    WaitStrategy
	tail    uint64 // 已占位的虚拟字节位置（本地变量，单写者）
	seq     uint64 // 下一条待占位记录的序号，从 1 开始
	latestV uint64 // 最新记录指针的 seqlock 版本号
}

// NewProducer 创建生产者。wait 用于 PolicyWait 下等待慢消费者。
func NewProducer(r *Ring, wait WaitStrategy) *Producer {
	return &Producer{r: r, wait: wait, seq: 1}
}

// gate 确保 [tail, limit) 这段空间不会覆盖任何活跃消费者还没读的数据。
// limit 是即将写到的虚拟字节位置（不含）。
func (p *Producer) gate(limit uint64) {
	if limit <= p.r.capacity {
		return
	}
	required := limit - p.r.capacity // 所有活跃消费者游标都必须 >= required
	for {
		min := ^uint64(0)
		for i := 0; i < p.r.numConsumers; i++ {
			if p.r.ConsumerLagging(i) {
				continue
			}
			if c := p.r.ConsumerCursor(i); c < min {
				min = c
			}
		}
		if min == ^uint64(0) || min >= required {
			return
		}
		if p.r.policy == PolicySkip {
			// 把拖后腿的消费者标记为掉队，之后不再等它。
			for i := 0; i < p.r.numConsumers; i++ {
				if p.r.ConsumerLagging(i) {
					continue
				}
				if p.r.ConsumerCursor(i) < required {
					atomic.StoreUint32(p.r.u32(p.r.infoOff(i)+infoFlags), flagLagging)
				}
			}
			continue
		}
		p.wait.Wait()
	}
}

// claim 为一条负载长度为 payloadLen 的记录占位，返回其虚拟字节位置，
// 并为它分配序号（p.seq 递增）。尾部放不下时先写 SKIP 标记绕回开头，
// SKIP 标记的序号 = 绕回后那条真实记录的序号。
func (p *Producer) claim(payloadLen int) uint64 {
	need := alignUp(RecordHeaderSize + uint64(payloadLen))
	if need+RecordAlign > p.r.capacity {
		panic("ring: 单条记录超过环容量")
	}
	off := p.tail & p.r.mask
	if off+need > p.r.capacity {
		// 尾部剩余空间放不下，填 SKIP 标记绕回开头。
		padLen := p.r.capacity - off // 一定是 16 的倍数且 >= 16
		p.gate(p.tail + padLen)
		ho := DataOffset + int(off)
		atomic.StoreUint32(p.r.u32(ho+8), uint32(padLen-RecordHeaderSize))
		atomic.StoreUint32(p.r.u32(ho+12), FlagSkip)
		// SKIP 记录的 seq 写成下一条真实记录的序号：消费者看到
		// seq 匹配 + SKIP 标记即知道填充记录已写完整，直接跳过。
		atomic.StoreUint64(p.r.u64(ho), p.seq)
		p.tail += padLen
	}
	p.gate(p.tail + need)
	pos := p.tail
	p.tail += need
	p.seq++
	return pos
}

// Batch 是一批已占位、待发布的记录。
type Batch struct {
	p    *Producer
	offs []uint64 // 每条记录的虚拟字节位置
	seq0 uint64   // 批次第一条记录的序号（之后连续）
	lens []int    // 每条记录的负载长度
}

// ClaimBatch 一次为多条记录占位，全部写完后调用 Publish 一起发布。
func (p *Producer) ClaimBatch(lens []int) *Batch {
	b := &Batch{p: p, offs: make([]uint64, len(lens)), seq0: p.seq, lens: append([]int(nil), lens...)}
	for i, l := range lens {
		b.offs[i] = p.claim(l)
	}
	return b
}

// Set 把负载写入批次中第 i 条记录。len(payload) 必须等于占位时的长度。
func (b *Batch) Set(i int, payload []byte) {
	if len(payload) != b.lens[i] {
		panic("ring: Set 长度与占位长度不一致")
	}
	off := DataOffset + int(b.offs[i]&b.p.r.mask) + RecordHeaderSize
	copy(b.p.r.buf[off:off+len(payload)], payload)
}

// Publish 发布整批记录：先写各条记录的长度和标志，再按序 release 写序号，
// 消费者看到序号即认为记录完整。最后更新最新记录指针和对外计数。
func (b *Batch) Publish() {
	p := b.p
	if len(b.offs) == 0 {
		return
	}
	var payloadBytes uint64
	for i, off := range b.offs {
		ho := DataOffset + int(off&p.r.mask)
		atomic.StoreUint32(p.r.u32(ho+8), uint32(b.lens[i]))
		atomic.StoreUint32(p.r.u32(ho+12), 0)
		atomic.StoreUint64(p.r.u64(ho), b.seq0+uint64(i))
		payloadBytes += uint64(b.lens[i])
	}
	// 最新记录指针（seqlock：奇数版本号 = 正在写），供掉队消费者重同步。
	lastSeq := b.seq0 + uint64(len(b.offs)) - 1
	p.latestV++
	atomic.StoreUint64(p.r.u64(offLatestVer), p.latestV)
	atomic.StoreUint64(p.r.u64(offLatestPos), b.offs[len(b.offs)-1])
	atomic.StoreUint64(p.r.u64(offLatestSeq), lastSeq)
	p.latestV++
	atomic.StoreUint64(p.r.u64(offLatestVer), p.latestV)

	atomic.StoreUint64(p.r.u64(offWriteCursor), p.tail)
	atomic.AddUint64(p.r.u64(offMsgCount), uint64(len(b.offs)))
	atomic.AddUint64(p.r.u64(offByteCount), payloadBytes)
}

// PublishOne 发布单条记录。
func (p *Producer) PublishOne(payload []byte) {
	b := p.ClaimBatch([]int{len(payload)})
	b.Set(0, payload)
	b.Publish()
}

// Finish 标记生产结束，消费者消费完最后一条记录后会退出。
func (p *Producer) Finish() {
	atomic.StoreUint64(p.r.u64(offDoneSeq), p.seq-1)
}
