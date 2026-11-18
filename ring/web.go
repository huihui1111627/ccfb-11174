package ring

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Sample 是一个吞吐采样点。
type Sample struct {
	T   int64   `json:"t"`   // Unix 秒
	Mps float64 `json:"mps"` // 该采样间隔内的消息/秒
}

type consumerStatus struct {
	ID       int    `json:"id"`
	Cursor   uint64 `json:"cursor"`
	Consumed uint64 `json:"consumed"`
	Behind   uint64 `json:"behind"` // 落后写游标的字节数
	Lagging  bool   `json:"lagging"`
	LagCount uint64 `json:"lagCount"`
}

type statusSnapshot struct {
	Capacity    uint64          `json:"capacity"`
	WriteCursor uint64          `json:"writeCursor"`
	Occupancy   float64         `json:"occupancy"` // 0..1
	Messages    uint64          `json:"messages"`
	Bytes       uint64          `json:"bytes"`
	Done        bool            `json:"done"`
	Padded      bool            `json:"padded"`
	Policy      string          `json:"policy"`
	Consumers   []consumerStatus `json:"consumers"`
	Histogram   []uint64        `json:"histogram"`
	Errors      []ErrorEntry    `json:"errors"`
	Slots       []int           `json:"slots"` // 128 格占用图，1=有未消费数据
	Samples     []Sample        `json:"samples"`
}

type statusServer struct {
	r       *Ring
	mu      sync.Mutex
	samples []Sample
	lastMsg uint64
	lastT   time.Time
}

// ServeStatus 在给定地址上启动队列状态监控页（阻塞）。
// 页面包含：槽位占用、各消费者游标、吞吐曲线、延迟分布、异常记录。
func ServeStatus(r *Ring, addr string) error {
	s := &statusServer{r: r, lastT: time.Now()}
	go s.sampleLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/status", s.handleStatus)
	return http.ListenAndServe(addr, mux)
}

func (s *statusServer) sampleLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		msg := s.r.MsgCount()
		dt := now.Sub(s.lastT).Seconds()
		mps := float64(msg-s.lastMsg) / dt
		s.lastMsg, s.lastT = msg, now
		s.mu.Lock()
		s.samples = append(s.samples, Sample{T: now.Unix(), Mps: mps})
		if len(s.samples) > 480 {
			s.samples = s.samples[len(s.samples)-480:]
		}
		s.mu.Unlock()
	}
}

func (s *statusServer) snapshot() statusSnapshot {
	r := s.r
	wc := r.WriteCursor()
	minC := r.MinActiveCursor()
	occ := uint64(0)
	if wc > minC {
		occ = wc - minC
		if occ > r.capacity {
			occ = r.capacity
		}
	}
	snap := statusSnapshot{
		Capacity:    r.capacity,
		WriteCursor: wc,
		Messages:    r.MsgCount(),
		Bytes:       r.ByteCount(),
		Done:        r.DoneSeq() != 0,
		Padded:      r.padded,
		Occupancy:   float64(occ) / float64(r.capacity),
		Slots:       make([]int, 128),
	}
	if r.policy == PolicySkip {
		snap.Policy = "skip(标记掉队)"
	} else {
		snap.Policy = "wait(等待)"
	}
	for i := 0; i < r.numConsumers; i++ {
		cur := r.ConsumerCursor(i)
		snap.Consumers = append(snap.Consumers, consumerStatus{
			ID:       i,
			Cursor:   cur,
			Consumed: r.ConsumerConsumed(i),
			Behind:   wc - cur,
			Lagging:  r.ConsumerLagging(i),
			LagCount: r.ConsumerLagCount(i),
		})
	}
	h := r.Histogram()
	snap.Histogram = h[:]
	snap.Errors = r.RecentErrors(50)
	// 槽位占用图：把数据区均分为 128 格，按格中点是否落在
	// [最慢活跃消费者游标, 写游标) 区间内判断占用。
	start := minC & r.mask
	cell := r.capacity / 128
	for i := 0; i < 128; i++ {
		mid := uint64(i)*cell + cell/2
		if (mid-start)&r.mask < occ {
			snap.Slots[i] = 1
		}
	}
	s.mu.Lock()
	snap.Samples = append(snap.Samples, s.samples...)
	s.mu.Unlock()
	return snap
}

func (s *statusServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.snapshot())
}

func (s *statusServer) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

const indexHTML = `<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>环形队列监控</title>
<style>
  body { background:#14171c; color:#d7dde5; font-family:-apple-system,"PingFang SC",sans-serif; margin:24px; }
  h1 { font-size:20px; } h2 { font-size:15px; color:#8fa3bf; margin:0 0 8px; }
  section { background:#1c2129; border:1px solid #2a313c; border-radius:8px; padding:14px 18px; margin-bottom:16px; }
  table { border-collapse:collapse; width:100%; font-size:13px; }
  th,td { text-align:left; padding:4px 10px; border-bottom:1px solid #2a313c; }
  th { color:#8fa3bf; font-weight:normal; }
  canvas { display:block; }
  .big { font-size:26px; font-weight:600; }
  .lag { color:#ff7a7a; } .ok { color:#6fd08c; }
  .meta { color:#8fa3bf; font-size:12px; margin-left:12px; }
</style>
</head>
<body>
<h1>窑炉采集环形队列监控<span class="meta" id="meta"></span></h1>
<section><h2>槽位占用</h2>
  <div class="big" id="occText">-</div>
  <canvas id="slots" width="768" height="40"></canvas>
</section>
<section><h2>各消费者游标</h2><table id="consumers"></table></section>
<section><h2>吞吐曲线（消息/秒）</h2><canvas id="tps" width="768" height="160"></canvas></section>
<section><h2>延迟分布（纳秒，对数桶）</h2><canvas id="lat" width="768" height="160"></canvas></section>
<section><h2>异常记录</h2><table id="errs"></table></section>
<script>
const errNames = {1:"校验和错误",2:"序号不连续",3:"掉队重同步"};
function fmtBytes(n){const u=["B","KiB","MiB","GiB"];let i=0;while(n>=1024&&i<3){n/=1024;i++}return n.toFixed(1)+" "+u[i]}
function fmtNum(n){return n.toLocaleString("en-US")}
function fmtNs(b){const ns=Math.pow(2,b);if(ns<1e3)return ns.toFixed(0)+"ns";if(ns<1e6)return (ns/1e3).toFixed(1)+"µs";if(ns<1e9)return (ns/1e6).toFixed(1)+"ms";return (ns/1e9).toFixed(2)+"s"}
async function refresh(){
  const s = await (await fetch("/api/status")).json();
  document.getElementById("meta").textContent =
    "  容量 "+fmtBytes(s.capacity)+" · 缓存行填充 "+(s.padded?"开":"关")+" · 掉队策略 "+s.policy+(s.done?" · 已完成":"");
  document.getElementById("occText").textContent =
    (s.occupancy*100).toFixed(1)+"%  ("+fmtBytes(s.occupancy*s.capacity)+" / "+fmtBytes(s.capacity)+")";
  // 槽位图
  const sc = document.getElementById("slots").getContext("2d");
  sc.clearRect(0,0,768,40);
  const cw = 768/s.slots.length;
  s.slots.forEach((v,i)=>{ sc.fillStyle = v?"#e8a13d":"#2f3a48"; sc.fillRect(i*cw+1,4,cw-2,32); });
  // 消费者
  let rows = "<tr><th>ID</th><th>游标(字节)</th><th>已消费记录</th><th>落后</th><th>掉队次数</th><th>状态</th></tr>";
  s.consumers.forEach(c=>{
    rows += "<tr><td>#"+c.id+"</td><td>"+fmtNum(c.cursor)+"</td><td>"+fmtNum(c.consumed)+
      "</td><td>"+fmtBytes(c.behind)+"</td><td>"+c.lagCount+
      "</td><td class="+(c.lagging?"'lag'>掉队":"'ok'>正常")+"</td></tr>";
  });
  document.getElementById("consumers").innerHTML = rows;
  // 吞吐曲线
  const tc = document.getElementById("tps").getContext("2d");
  tc.clearRect(0,0,768,160);
  if(s.samples.length>1){
    const max = Math.max(...s.samples.map(x=>x.mps),1);
    tc.strokeStyle="#5aa9e6"; tc.lineWidth=1.5; tc.beginPath();
    s.samples.forEach((p,i)=>{
      const x=i*(768/(s.samples.length-1)), y=155-(p.mps/max)*145;
      i?tc.lineTo(x,y):tc.moveTo(x,y);
    });
    tc.stroke();
    tc.fillStyle="#8fa3bf"; tc.font="11px sans-serif";
    tc.fillText("峰值 "+fmtNum(Math.round(max))+" msg/s",8,14);
    tc.fillText("累计 "+fmtNum(s.messages)+" 条",8,30);
  }
  // 延迟分布
  const lc = document.getElementById("lat").getContext("2d");
  lc.clearRect(0,0,768,160);
  const hb = s.histogram, hmax = Math.max(...hb,1);
  const bw = 768/hb.length;
  hb.forEach((v,i)=>{
    if(!v) return;
    const h = Math.max(2,(v/hmax)*130);
    lc.fillStyle="#7fc8a9"; lc.fillRect(i*bw+1,150-h,bw-2,h);
  });
  lc.fillStyle="#8fa3bf"; lc.font="10px sans-serif";
  for(let b=0;b<hb.length;b+=10){ lc.fillText(fmtNs(b), b*bw, 159); }
  // 异常
  let er = "<tr><th>时间</th><th>类型</th><th>消费者</th><th>参数A</th><th>参数B</th></tr>";
  if(!s.errors.length) er += "<tr><td colspan=5 style='color:#6fd08c'>无异常</td></tr>";
  s.errors.forEach(e=>{
    er += "<tr><td>"+new Date(e.ts/1e6).toLocaleTimeString()+"</td><td>"+(errNames[e.code]||e.code)+
      "</td><td>"+(e.consumer<0?"生产者":"#"+e.consumer)+"</td><td>"+e.a+"</td><td>"+e.b+"</td></tr>";
  });
  document.getElementById("errs").innerHTML = er;
}
refresh(); setInterval(refresh, 1000);
</script>
</body>
</html>`
