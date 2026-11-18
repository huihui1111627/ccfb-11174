package ring

import (
	"encoding/binary"
	"path/filepath"
	"testing"
)

// 进程内基本收发：批量发布、变长负载、内容逐字节校验。
// 用 PolicyWait：完整性场景下生产者必须等消费者，不允许套圈跳过。
func TestInProcessBasic(t *testing.T) {
	r, err := Create(filepath.Join(t.TempDir(), "r.shm"), 1<<20, 1, PolicyWait, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := NewProducer(r, YieldWait{})
	c, err := NewConsumer(r, 0, BusyWait{})
	if err != nil {
		t.Fatal(err)
	}
	const N = 200000
	go func() {
		var lens [64]int
		for base := 0; base < N; {
			n := 64
			if base+n > N {
				n = N - base
			}
			for i := 0; i < n; i++ {
				lens[i] = 8 + (base+i)%57
			}
			b := p.ClaimBatch(lens[:n])
			payload := make([]byte, 64)
			for i := 0; i < n; i++ {
				payload = payload[:lens[i]]
				binary.LittleEndian.PutUint64(payload, uint64(base+i))
				for j := 8; j < len(payload); j++ {
					payload[j] = byte(base + i + j)
				}
				b.Set(i, payload)
			}
			b.Publish()
			base += n
		}
		p.Finish()
	}()
	got := 0
	for {
		payload, ok := c.Next()
		if !ok {
			break
		}
		if binary.LittleEndian.Uint64(payload) != uint64(got) {
			t.Fatalf("第 %d 条序号错乱", got)
		}
		for j := 8; j < len(payload); j++ {
			if payload[j] != byte(got+j) {
				t.Fatalf("第 %d 条内容损坏 @%d", got, j)
			}
		}
		got++
	}
	if got != N {
		t.Fatalf("收到 %d 条, 期望 %d", got, N)
	}
}

// 小环 + 变长记录，强制频繁绕回，验证 SKIP 标记逻辑。
func TestWrapAround(t *testing.T) {
	r, err := Create(filepath.Join(t.TempDir(), "r.shm"), 1<<16, 1, PolicyWait, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := NewProducer(r, BusyWait{})
	c, _ := NewConsumer(r, 0, BusyWait{})
	const N = 100000
	go func() {
		for i := 0; i < N; i++ {
			payload := make([]byte, 8+i%200) // 长度变化大，必然频繁绕回
			binary.LittleEndian.PutUint64(payload, uint64(i))
			p.PublishOne(payload)
		}
		p.Finish()
	}()
	for got := 0; got < N; got++ {
		payload, ok := c.Next()
		if !ok {
			t.Fatalf("第 %d 条提前结束", got)
		}
		if binary.LittleEndian.Uint64(payload) != uint64(got) {
			t.Fatalf("第 %d 条错乱", got)
		}
	}
}

// 消费者不读时，PolicySkip 下生产者应把它标记为掉队并继续写。
func TestLaggingMarked(t *testing.T) {
	r, err := Create(filepath.Join(t.TempDir(), "r.shm"), 1<<16, 2, PolicySkip, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := NewProducer(r, BusyWait{})
	c0, _ := NewConsumer(r, 0, BusyWait{})
	// 消费者 1 永远不读。
	done := make(chan int, 1)
	go func() {
		n := 0
		for {
			if _, ok := c0.Next(); !ok {
				break
			}
			n++
		}
		done <- n
	}()
	total := int(r.Capacity()/32) * 4 // 写满 4 圈
	payload := make([]byte, 16)
	for i := 0; i < total; i++ {
		p.PublishOne(payload)
	}
	p.Finish()
	if n := <-done; n != total {
		t.Fatalf("活跃消费者收到 %d 条, 期望 %d", n, total)
	}
	if !r.ConsumerLagging(1) {
		t.Fatal("不读的消费者应被标记为掉队")
	}
}

// PolicyWait 下生产者必须等慢消费者，不允许覆盖未读数据。
func TestWaitPolicy(t *testing.T) {
	r, err := Create(filepath.Join(t.TempDir(), "r.shm"), 1<<16, 1, PolicyWait, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := NewProducer(r, BusyWait{})
	c, _ := NewConsumer(r, 0, BusyWait{})
	total := int(r.Capacity()/32) * 3 // 3 圈，没有 skip 策略兜底
	go func() {
		payload := make([]byte, 16)
		for i := 0; i < total; i++ {
			p.PublishOne(payload)
		}
		p.Finish()
	}()
	for got := 0; got < total; got++ {
		if _, ok := c.Next(); !ok {
			t.Fatalf("第 %d 条提前结束", got)
		}
	}
	if r.ConsumerLagCount(0) != 0 {
		t.Fatal("wait 策略下不应发生掉队")
	}
}
