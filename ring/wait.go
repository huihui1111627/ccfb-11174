package ring

import (
	"runtime"
	"time"
)

// WaitStrategy 是消费者（或 PolicyWait 下的生产者）等待数据/空间时的策略。
type WaitStrategy interface{ Wait() }

// BusyWait 忙等，延迟最低，占满 CPU。
type BusyWait struct{}

// Wait 空转。
func (BusyWait) Wait() {}

// YieldWait 让出时间片，延迟略高，CPU 占用低一些。
type YieldWait struct{}

// Wait 让出调度。
func (YieldWait) Wait() { runtime.Gosched() }

// SleepWait 睡眠等待，CPU 占用最低，延迟最高。D 为 0 时默认 200µs。
type SleepWait struct{ D time.Duration }

// Wait 睡眠一段时间。
func (s SleepWait) Wait() {
	d := s.D
	if d <= 0 {
		d = 200 * time.Microsecond
	}
	time.Sleep(d)
}

// WaitByName 按名字构造等待策略："busy" / "yield" / "sleep"。
func WaitByName(name string) WaitStrategy {
	switch name {
	case "busy":
		return BusyWait{}
	case "sleep":
		return SleepWait{}
	default:
		return YieldWait{}
	}
}
