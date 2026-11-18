package ring

import (
	"runtime"
	"sync/atomic"
)

// Consumer 是消费者句柄，每个消费者进程用各自的 id 创建一个。
type Consumer struct {
	r           *Ring
	id          int
	wait        WaitStrategy
	cursor      uint64 // 已消费的虚拟字节位置
	expectedSeq uint64 // 下一条期望的记录序号
	scratch     []byte // 负载拷贝缓冲，复用避免分配
	// OnResync 在发生掉队重同步后回调（此时消息流有空洞）。
	OnResync func()
}

// NewConsumer 创建消费者，id 必须在 [0, NumConsumers) 内。
func NewConsumer(r *Ring, id int, wait WaitStrategy) (*Consumer, error) {
	if id < 0 || id >= r.numConsumers {
		return nil, &ErrInvalidConsumer{ID: id}
	}
	return &Consumer{r: r, id: id, wait: wait, expectedSeq: 1}, nil
}

// ErrInvalidConsumer 消费者 id 越界。
type ErrInvalidConsumer struct{ ID int }

func (e *ErrInvalidConsumer) Error() string { return "ring: 非法消费者 id" }

// Next 取下一条记录的负载。返回的切片在下一次 Next 调用前有效。
// ok=false 表示生产者已完成且所有记录都已读完。
func (c *Consumer) Next() (payload []byte, ok bool) {
	cursorPtr := c.r.u64(c.r.cursorOff(c.id))
	flagsPtr := c.r.u32(c.r.infoOff(c.id) + infoFlags)
	for {
		// 被生产者标记为掉队：跳到最新记录继续。
		if atomic.LoadUint32(flagsPtr)&flagLagging != 0 {
			c.resync()
			continue
		}
		// 发布游标之前的槽位生产者本圈一定已经写过，记录头有效；
		// 游标之后的内存可能还是上一圈的负载字节，不能当记录头解读。
		if c.cursor >= c.r.WriteCursor() {
			// 生产者还没写到，检查是否已经全部完成。
			if done := c.r.DoneSeq(); done != 0 && c.expectedSeq > done {
				return nil, false
			}
			c.wait.Wait()
			continue
		}
		ho := DataOffset + int(c.cursor&c.r.mask)
		seqPtr := c.r.u64(ho)
		s := atomic.LoadUint64(seqPtr)
		switch {
		case s == c.expectedSeq:
			length := atomic.LoadUint32(c.r.u32(ho + 8))
			flags := atomic.LoadUint32(c.r.u32(ho + 12))
			total := alignUp(RecordHeaderSize + uint64(length))
			if flags&FlagSkip != 0 {
				// 填充记录：跳过，不消耗序号。
				c.cursor += total
				atomic.StoreUint64(cursorPtr, c.cursor)
				continue
			}
			// seqlock 读：先拷贝负载，再复核序号。若复核时序号变了，
			// 说明读取过程中被覆盖（只会发生在被标记掉队的瞬间），
			// 丢弃这次读取并重同步，保证不会把撕裂的数据交给上层。
			if cap(c.scratch) < int(length) {
				c.scratch = make([]byte, length)
			}
			payload = c.scratch[:length]
			copy(payload, c.r.buf[ho+RecordHeaderSize:ho+RecordHeaderSize+int(length)])
			if atomic.LoadUint64(seqPtr) != s {
				c.resync()
				continue
			}
			c.cursor += total
			atomic.StoreUint64(cursorPtr, c.cursor)
			c.expectedSeq++
			if c.expectedSeq&0xff == 0 {
				atomic.StoreUint64(c.r.u64(c.r.infoOff(c.id)+infoConsumed), c.expectedSeq-1)
			}
			return payload, true
		case s > c.expectedSeq:
			// 槽位里的序号已经超过了期望值：被生产者覆盖了一圈以上，掉队了。
			c.resync()
		default:
			// cursor 在发布游标之内时不会出现 s < expectedSeq；
			// 防御性等待，避免异常状态下空转打满 CPU。
			c.wait.Wait()
		}
	}
}

// resync 掉队重同步：跳到生产者最新发布的记录处继续消费。
func (c *Consumer) resync() {
	for {
		v1 := atomic.LoadUint64(c.r.u64(offLatestVer))
		if v1&1 != 0 {
			runtime.Gosched()
			continue
		}
		pos := atomic.LoadUint64(c.r.u64(offLatestPos))
		seq := atomic.LoadUint64(c.r.u64(offLatestSeq))
		v2 := atomic.LoadUint64(c.r.u64(offLatestVer))
		if v1 != v2 {
			continue
		}
		if seq == 0 {
			c.cursor, c.expectedSeq = 0, 1
		} else {
			c.cursor, c.expectedSeq = pos, seq
		}
		atomic.StoreUint64(c.r.u64(c.r.cursorOff(c.id)), c.cursor)
		atomic.StoreUint32(c.r.u32(c.r.infoOff(c.id)+infoFlags), 0) // 清除掉队标记
		atomic.AddUint64(c.r.u64(c.r.infoOff(c.id)+infoLagCount), 1)
		if c.OnResync != nil {
			c.OnResync()
		}
		return
	}
}
