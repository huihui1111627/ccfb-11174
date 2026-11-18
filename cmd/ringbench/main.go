// ringbench 是环形队列的多进程压测程序。
//
// 子命令：
//   run       父进程：创建共享内存，拉起 1 个生产者进程和 N 个消费者进程，
//             校验每个消费者收到的序列完整、顺序正确、校验和无误，
//             统计吞吐与延迟分布，验证掉队消费者被正确标记。
//   producer  生产者进程（由 run 拉起，也可单独运行）
//   consumer  消费者进程（由 run 拉起）
//   web       对一个已存在的共享内存文件单独启动监控页
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"kilnring/ring"
)

// 消息负载格式（变长，24~64 字节）：
//
//	0   msgSeq   uint64  消息序号，从 1 开始
//	8   sendTime int64   发送时间 UnixNano（用于端到端延迟）
//	16  crc32    uint32  对 [0:16] 和 [20:] 的 Castagnoli 校验
//	20  filler   变长填充
const minPayload = 24

var crcTab = crc32.MakeTable(crc32.Castagnoli)

func payloadLenFor(seq uint64) int { return minPayload + int(seq%41) } // 24..64 字节

func buildMessage(p []byte, seq uint64) {
	binary.LittleEndian.PutUint64(p[0:], seq)
	binary.LittleEndian.PutUint64(p[8:], uint64(time.Now().UnixNano()))
	for i := 20; i < len(p); i++ {
		p[i] = byte(seq) + byte(i)
	}
	c := crc32.Update(crc32.Update(0, crcTab, p[:16]), crcTab, p[20:])
	binary.LittleEndian.PutUint32(p[16:], c)
}

func checkMessage(p []byte) (seq uint64, ts int64, ok bool) {
	if len(p) < minPayload {
		return 0, 0, false
	}
	seq = binary.LittleEndian.Uint64(p[0:])
	ts = int64(binary.LittleEndian.Uint64(p[8:]))
	want := binary.LittleEndian.Uint32(p[16:])
	got := crc32.Update(crc32.Update(0, crcTab, p[:16]), crcTab, p[20:])
	return seq, ts, want == got
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: ringbench <run|producer|consumer|web> [参数]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		os.Exit(cmdRun(os.Args[2:]))
	case "producer":
		cmdProducer(os.Args[2:])
	case "consumer":
		cmdConsumer(os.Args[2:])
	case "web":
		cmdWeb(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "未知子命令:", os.Args[1])
		os.Exit(2)
	}
}

// ---------- producer ----------

type producerResult struct {
	Published uint64  `json:"published"`
	Seconds   float64 `json:"seconds"`
}

func cmdProducer(args []string) {
	fs := flag.NewFlagSet("producer", flag.ExitOnError)
	shm := fs.String("shm", "", "共享内存文件路径")
	messages := fs.Int64("messages", 0, "消息总数")
	batch := fs.Int("batch", 256, "每批发布条数")
	rate := fs.Float64("rate", 0, "限速(条/秒, 0=不限)")
	result := fs.String("result", "", "结果 JSON 输出路径")
	fs.Parse(args)

	r, err := ring.Open(*shm)
	must(err)
	defer r.Close()
	p := ring.NewProducer(r, ring.YieldWait{})

	start := time.Now()
	lens := make([]int, 0, *batch)
	scratch := make([]byte, minPayload+41)
	var seq uint64 = 1
	total := uint64(*messages)
	for seq <= total {
		n := *batch
		if rem := total - seq + 1; uint64(n) > rem {
			n = int(rem)
		}
		lens = lens[:0]
		for i := 0; i < n; i++ {
			lens = append(lens, payloadLenFor(seq+uint64(i)))
		}
		b := p.ClaimBatch(lens)
		for i := 0; i < n; i++ {
			buildMessage(scratch[:lens[i]], seq+uint64(i))
			b.Set(i, scratch[:lens[i]])
		}
		b.Publish()
		seq += uint64(n)
		if *rate > 0 { // 限速：模拟采集进程的固定产出速率
			if want := time.Duration(float64(seq-1) / *rate * float64(time.Second)); want > 0 {
				if ahead := want - time.Since(start); ahead > 0 {
					time.Sleep(ahead)
				}
			}
		}
	}
	p.Finish()
	dur := time.Since(start)

	must(os.WriteFile(*result, mustJSON(producerResult{
		Published: total, Seconds: dur.Seconds(),
	}), 0o644))
	fmt.Fprintf(os.Stderr, "生产者完成: %d 条, 耗时 %v, %.0f 条/秒\n",
		total, dur.Round(time.Millisecond), float64(total)/dur.Seconds())
}

// ---------- consumer ----------

type consumerResult struct {
	ID         int    `json:"id"`
	Received   uint64 `json:"received"`
	FirstMsg   uint64 `json:"firstMsg"`
	LastMsg    uint64 `json:"lastMsg"`
	CrcErrs    uint64 `json:"crcErrs"`
	OrderErrs  uint64 `json:"orderErrs"`
	GapEvents  uint64 `json:"gapEvents"`
}

func cmdConsumer(args []string) {
	fs := flag.NewFlagSet("consumer", flag.ExitOnError)
	shm := fs.String("shm", "", "共享内存文件路径")
	id := fs.Int("id", -1, "消费者 id")
	waitName := fs.String("wait", "busy", "等待策略 busy|yield|sleep")
	lagSleep := fs.Duration("lag-sleep", 0, "启动后先睡眠多久（模拟掉队）")
	result := fs.String("result", "", "结果 JSON 输出路径")
	fs.Parse(args)

	r, err := ring.Open(*shm)
	must(err)
	defer r.Close()
	c, err := ring.NewConsumer(r, *id, ring.WaitByName(*waitName))
	must(err)

	res := consumerResult{ID: *id}
	var expectMsg uint64 = 1 // 期望的消息序号；0 表示“重同步后认领下一条”
	c.OnResync = func() {
		res.GapEvents++
		expectMsg = 0
		r.LogError(ring.ErrLagged, int32(*id), 0, 0)
	}

	r.ConsumerReady()
	if *lagSleep > 0 {
		time.Sleep(*lagSleep) // 故意睡觉，让生产者把它套圈
	}
	for {
		payload, ok := c.Next()
		if !ok {
			break
		}
		seq, ts, ok := checkMessage(payload)
		if !ok {
			res.CrcErrs++
			r.LogError(ring.ErrChecksum, int32(*id), seq, 0)
		}
		if expectMsg == 0 {
			expectMsg = seq + 1 // 重同步后认领第一条
			if res.FirstMsg == 0 {
				res.FirstMsg = seq
			}
		} else if seq != expectMsg {
			res.OrderErrs++
			r.LogError(ring.ErrOrder, int32(*id), expectMsg, seq)
			expectMsg = seq + 1
		} else {
			expectMsg++
		}
		if res.FirstMsg == 0 {
			res.FirstMsg = seq
		}
		res.LastMsg = seq
		res.Received++
		r.RecordLatency(time.Now().UnixNano() - ts)
	}
	must(os.WriteFile(*result, mustJSON(res), 0o644))
}

// ---------- run ----------

type runConfig struct {
	messages  int64
	consumers int
	capacity  uint64
	policy    int
	padded    bool
	batch     int
	rate      float64
	waitName  string
	lagSleep  time.Duration
	webAddr   string
	keep      bool
}

type runReport struct {
	cfg        runConfig
	producer   producerResult
	consumers  []consumerResult
	lagCounts  []uint64
	histogram  [ring.HistBuckets]uint64
	errors     []ring.ErrorEntry
	shmPath    string
	checks     []checkResult
}

type checkResult struct {
	name string
	pass bool
	info string
}

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	messages := fs.Int64("messages", 30000000, "消息总数")
	consumers := fs.Int("consumers", 3, "消费者进程数")
	capacity := fs.Uint64("ring", 64<<20, "环容量(字节, 2 的幂)")
	policyName := fs.String("policy", "skip", "掉队策略 wait|skip")
	padding := fs.String("padding", "on", "游标缓存行填充 on|off")
	comparePadding := fs.Bool("compare-padding", false, "跑两遍对比填充开/关的吞吐差别")
	batch := fs.Int("batch", 256, "生产者每批发布条数")
	rate := fs.Float64("rate", 5000000, "生产者限速(条/秒, 0=不限)")
	waitName := fs.String("wait", "busy", "消费者等待策略 busy|yield|sleep")
	lagSleep := fs.Duration("lag-sleep", 3*time.Second, "最后一个消费者启动后睡眠时长(0=不掉队演示)")
	webAddr := fs.String("web", "", "同时在此地址起监控页(如 :8080)，跑完后保持服务")
	keep := fs.Bool("keep", false, "保留共享内存文件")
	fs.Parse(args)

	policy := ring.PolicySkip
	if *policyName == "wait" {
		policy = ring.PolicyWait
	}
	base := runConfig{
		messages: *messages, consumers: *consumers, capacity: *capacity,
		policy: policy, padded: *padding == "on", batch: *batch, rate: *rate,
		waitName: *waitName, lagSleep: *lagSleep, webAddr: *webAddr, keep: *keep,
	}

	if *comparePadding {
		// A/B 对比：wait 策略 + 不限速，让瓶颈落在消费者游标更新上，
		// 同样的负载分别跑填充开/关，观察伪共享的影响。
		var results [2]*runReport
		for i, padded := range [2]bool{true, false} {
			cfg := base
			cfg.padded = padded
			cfg.lagSleep = 0
			cfg.webAddr = ""
			cfg.policy = ring.PolicyWait
			cfg.rate = 0
			fmt.Printf("\n===== 第 %d/2 轮: 缓存行填充 %v =====\n", i+1, map[bool]string{true: "开", false: "关"}[padded])
			rep, err := runOnce(cfg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "运行失败:", err)
				return 1
			}
			printReport(rep)
			results[i] = rep
		}
		fmt.Println("\n===== 填充对比 =====")
		p0 := float64(results[0].producer.Published) / results[0].producer.Seconds
		p1 := float64(results[1].producer.Published) / results[1].producer.Seconds
		fmt.Printf("填充开:  %.0f 条/秒\n填充关:  %.0f 条/秒\n", p0, p1)
		if p0 > 0 {
			fmt.Printf("不开填充吞吐下降 %.1f%%（消费者游标挤在同一缓存行互相失效）\n", (1-p1/p0)*100)
		}
		return 0
	}

	rep, err := runOnce(base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "运行失败:", err)
		return 1
	}
	printReport(rep)
	ok := true
	for _, c := range rep.checks {
		if !c.pass {
			ok = false
		}
	}
	if base.webAddr != "" {
		fmt.Printf("\n监控页保持在 http://%s ，按 Ctrl+C 退出\n", base.webAddr)
		select {}
	}
	if !ok {
		return 1
	}
	return 0
}

func runOnce(cfg runConfig) (*runReport, error) {
	dir, err := os.MkdirTemp("", "kilnring-*")
	if err != nil {
		return nil, err
	}
	if !cfg.keep && cfg.webAddr == "" {
		defer os.RemoveAll(dir)
	}
	shmPath := filepath.Join(dir, "ring.shm")
	r, err := ring.Create(shmPath, cfg.capacity, cfg.consumers, cfg.policy, cfg.padded)
	if err != nil {
		return nil, err
	}
	if cfg.webAddr == "" {
		defer r.Close() // 开了监控页就不能解除映射，页面要一直读这块内存
	}
	if cfg.webAddr != "" {
		go ring.ServeStatus(r, cfg.webAddr)
		fmt.Printf("监控页: http://%s\n", cfg.webAddr)
	}

	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// 先拉起全部消费者进程。
	var cmds []*exec.Cmd
	for i := 0; i < cfg.consumers; i++ {
		resPath := filepath.Join(dir, fmt.Sprintf("consumer-%d.json", i))
		args := []string{"consumer", "--shm", shmPath, "--id", strconv.Itoa(i),
			"--wait", cfg.waitName, "--result", resPath}
		if cfg.lagSleep > 0 && i == cfg.consumers-1 {
			args = append(args, "--lag-sleep", cfg.lagSleep.String())
		}
		c := exec.CommandContext(ctx, exe, args...)
		c.Stderr = os.Stderr
		logf, err := os.Create(filepath.Join(dir, fmt.Sprintf("consumer-%d.log", i)))
		if err != nil {
			return nil, err
		}
		defer logf.Close()
		c.Stdout = logf
		if err := c.Start(); err != nil {
			return nil, err
		}
		cmds = append(cmds, c)
	}
	// 等所有消费者就绪后再启动生产者，避免起跑阶段的误掉队。
	deadline := time.Now().Add(30 * time.Second)
	for r.ReadyCount() < cfg.consumers {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("等待消费者就绪超时")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 拉起生产者进程。
	presPath := filepath.Join(dir, "producer.json")
	pc := exec.CommandContext(ctx, exe, "producer", "--shm", shmPath,
		"--messages", strconv.FormatInt(cfg.messages, 10),
		"--batch", strconv.Itoa(cfg.batch),
		"--rate", strconv.FormatFloat(cfg.rate, 'f', 0, 64),
		"--result", presPath)
	pc.Stderr = os.Stderr
	if err := pc.Start(); err != nil {
		return nil, err
	}
	if err := pc.Wait(); err != nil {
		return nil, fmt.Errorf("生产者进程异常: %w", err)
	}
	for _, c := range cmds {
		if err := c.Wait(); err != nil {
			return nil, fmt.Errorf("消费者进程异常: %w", err)
		}
	}

	rep := &runReport{cfg: cfg, shmPath: shmPath, histogram: r.Histogram(), errors: r.RecentErrors(10)}
	must(json.Unmarshal(mustRead(presPath), &rep.producer))
	for i := 0; i < cfg.consumers; i++ {
		var cr consumerResult
		must(json.Unmarshal(mustRead(filepath.Join(dir, fmt.Sprintf("consumer-%d.json", i))), &cr))
		rep.consumers = append(rep.consumers, cr)
		rep.lagCounts = append(rep.lagCounts, r.ConsumerLagCount(i))
	}
	rep.checks = validate(rep)
	return rep, nil
}

// validate 校验：序列完整、顺序正确、内容无误、掉队者被正确标记。
func validate(rep *runReport) []checkResult {
	cfg := rep.cfg
	total := uint64(cfg.messages)
	var checks []checkResult
	add := func(name string, pass bool, info string) {
		checks = append(checks, checkResult{name, pass, info})
	}

	add("生产者发布条数", rep.producer.Published == total,
		fmt.Sprintf("%d/%d", rep.producer.Published, total))

	lagID := -1
	if cfg.lagSleep > 0 && cfg.policy == ring.PolicySkip {
		lagID = cfg.consumers - 1
	}
	for i, cr := range rep.consumers {
		if i == lagID {
			// 故意掉队的消费者：必须被标记过，且已收部分内部顺序和校验仍然正确。
			add(fmt.Sprintf("消费者#%d 被标记掉队", i), rep.lagCounts[i] > 0,
				fmt.Sprintf("掉队/重同步 %d 次", rep.lagCounts[i]))
			add(fmt.Sprintf("消费者#%d 已收段内顺序+校验", i),
				cr.OrderErrs == 0 && cr.CrcErrs == 0,
				fmt.Sprintf("收到 %d 条, 顺序错 %d, 校验错 %d", cr.Received, cr.OrderErrs, cr.CrcErrs))
			continue
		}
		add(fmt.Sprintf("消费者#%d 序列完整", i),
			cr.Received == total && cr.FirstMsg == 1 && cr.LastMsg == total && cr.GapEvents == 0,
			fmt.Sprintf("收到 %d/%d, 首条 %d, 末条 %d, 空洞 %d",
				cr.Received, total, cr.FirstMsg, cr.LastMsg, cr.GapEvents))
		add(fmt.Sprintf("消费者#%d 顺序+校验", i),
			cr.OrderErrs == 0 && cr.CrcErrs == 0,
			fmt.Sprintf("顺序错 %d, 校验错 %d", cr.OrderErrs, cr.CrcErrs))
	}
	return checks
}

func printReport(rep *runReport) {
	cfg := rep.cfg
	fmt.Println("\n========== 压测报告 ==========")
	fmt.Printf("消息总数: %s   环容量: %d MiB   缓存行填充: %v   掉队策略: %s   消费者等待: %s   限速: %s 条/秒\n",
		comma(uint64(cfg.messages)), cfg.capacity>>20, map[bool]string{true: "开", false: "关"}[cfg.padded],
		map[int]string{ring.PolicySkip: "skip(标记掉队)", ring.PolicyWait: "wait(等待)"}[cfg.policy],
		cfg.waitName, comma(uint64(cfg.rate)))
	mps := float64(rep.producer.Published) / rep.producer.Seconds
	fmt.Printf("生产者: 发布 %s 条, 耗时 %.2fs, %s 条/秒\n",
		comma(rep.producer.Published), rep.producer.Seconds, comma(uint64(mps)))
	for i, cr := range rep.consumers {
		note := ""
		if rep.lagCounts[i] > 0 {
			note = fmt.Sprintf("  [掉队 %d 次]", rep.lagCounts[i])
		}
		fmt.Printf("消费者#%d: 收到 %s 条 (首 %d 末 %d), 顺序错 %d, 校验错 %d, 空洞 %d%s\n",
			i, comma(cr.Received), cr.FirstMsg, cr.LastMsg, cr.OrderErrs, cr.CrcErrs, cr.GapEvents, note)
	}
	printLatency(rep.histogram)
	if len(rep.errors) > 0 {
		fmt.Printf("异常记录: 最近 %d 条\n", len(rep.errors))
		for _, e := range rep.errors {
			fmt.Printf("  %s 码=%d 消费者=%d A=%d B=%d\n",
				time.Unix(0, e.Time).Format("15:04:05.000"), e.Code, e.Consumer, e.A, e.B)
		}
	} else {
		fmt.Println("异常记录: 无")
	}
	fmt.Println("---------- 校验 ----------")
	allPass := true
	for _, c := range rep.checks {
		mark := "PASS"
		if !c.pass {
			mark = "FAIL"
			allPass = false
		}
		fmt.Printf("[%s] %s: %s\n", mark, c.name, c.info)
	}
	if allPass {
		fmt.Println("总体结果: PASS")
	} else {
		fmt.Println("总体结果: FAIL")
	}
}

// printLatency 从共享直方图计算并打印延迟分位数。
func printLatency(h [ring.HistBuckets]uint64) {
	var total uint64
	for _, v := range h {
		total += v
	}
	if total == 0 {
		fmt.Println("延迟分布: 无样本")
		return
	}
	pick := func(p float64) int {
		want := uint64(float64(total) * p)
		var cum uint64
		for i, v := range h {
			cum += v
			if cum >= want {
				return i
			}
		}
		return ring.HistBuckets - 1
	}
	fmt.Printf("延迟分布(端到端, 样本 %s): p50 ≤ %s  p90 ≤ %s  p99 ≤ %s  p99.9 ≤ %s  max ≤ %s\n",
		comma(total), bucketBound(pick(0.50)), bucketBound(pick(0.90)),
		bucketBound(pick(0.99)), bucketBound(pick(0.999)), bucketBound(pick(1.0)))
}

func bucketBound(b int) time.Duration {
	if b > 62 {
		b = 62
	}
	return time.Duration(uint64(1) << uint(b))
}

// ---------- web ----------

func cmdWeb(args []string) {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	shm := fs.String("shm", "", "共享内存文件路径")
	addr := fs.String("addr", ":8080", "监听地址")
	fs.Parse(args)
	r, err := ring.Open(*shm)
	must(err)
	fmt.Printf("监控页: http://%s  (shm=%s)\n", *addr, *shm)
	must(ring.ServeStatus(r, *addr))
}

// ---------- 工具 ----------

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	must(err)
	return b
}

func mustRead(path string) []byte {
	b, err := os.ReadFile(path)
	must(err)
	return b
}

func comma(n uint64) string {
	s := strconv.FormatUint(n, 10)
	var out []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}
