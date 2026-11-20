"""FastAPI 入口：工单提交/查询、交接、订阅暂停恢复，以及演示网页。"""
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db
from .config import get_settings
from .cutover import CutoverError, CutoverManager
from .state import StateStore

settings = get_settings()
store = StateStore(settings.state_db_path)
manager = CutoverManager(settings, store)

app = FastAPI(title="巡检工单库交接", version="1.1")


class WorkOrderIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    detail: str = Field(default="", max_length=2000)


class CutoverRequest(BaseModel):
    confirm: bool = False


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


@app.get("/api/status")
def status() -> dict:
    state = manager.current_state()
    return {
        "state": state,
        "accepting_db": manager.accepting_db(),
        "inflight_writes": manager.inflight,
        "subscription": manager.subscription_overview(),
        "recovered_run": manager.recovered_run,
        "active_progress": manager.progress(),
    }


@app.get("/api/work-orders")
def work_orders() -> dict:
    # 两库数据都展示，source_db 标明每条工单实际由哪个库接单
    return {
        "accepting_db": manager.accepting_db(),
        "old": db.list_work_orders(settings.old_dsn),
        "new": db.list_work_orders(settings.new_dsn),
        "old_summary": {"count_max": db.count_and_max(settings.old_dsn)},
        "new_summary": {"count_max": db.count_and_max(settings.new_dsn)},
    }


@app.post("/api/work-orders")
def submit_work_order(payload: WorkOrderIn) -> dict:
    state = manager.current_state()
    if state == "SWITCHING":
        raise HTTPException(status_code=503, detail="交接进行中，旧库已暂停接单，请稍后再试")
    if state == "NEW":
        target_dsn, target_name = settings.new_dsn, "new"
    else:
        target_dsn, target_name = settings.old_dsn, "old"

    manager.enter_write()
    try:
        row = db.insert_work_order(target_dsn, payload.title, payload.detail, target_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"写入失败: {exc}") from exc
    finally:
        manager.exit_write()
    return {"work_order": row, "accepted_by": target_name, "state": state}


@app.post("/api/cutover")
def start_cutover(payload: CutoverRequest) -> JSONResponse:
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="需要 confirm=true 确认发起单向交接")
    try:
        result = manager.start_cutover()
    except CutoverError as exc:
        # 交接串行：进行中再次发起返回 409
        return JSONResponse(
            status_code=409,
            content={"ok": False, "error": str(exc), "state": manager.current_state()},
        )
    if result.get("already_done"):
        return JSONResponse(status_code=200, content={"ok": True, **result})
    # 交接在后台线程执行，立即返回 run_id；进度经 /api/progress 轮询
    return JSONResponse(status_code=202, content={"ok": True, **result})


@app.get("/api/progress")
def progress(run_id: str | None = None) -> dict:
    data = manager.progress(run_id)
    if data is None:
        return {"has_progress": False}
    return {"has_progress": True, **data}


@app.get("/api/runs")
def runs() -> dict:
    return {"runs": manager.list_runs()}


@app.get("/api/runs/{run_id}")
def run_detail(run_id: str) -> dict:
    run = manager.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="交接运行记录不存在")
    return run


@app.post("/api/subscription/pause")
def pause_subscription() -> dict:
    try:
        return manager.pause_subscription()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"暂停订阅失败: {exc}") from exc


@app.post("/api/subscription/resume")
def resume_subscription() -> dict:
    try:
        return manager.resume_subscription()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"恢复订阅失败: {exc}") from exc


@app.get("/api/events")
def events() -> list:
    return store.list_events()


PAGE_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>巡检工单库交接</title>
<style>
  body { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif; margin: 24px; background: #f6f7f9; color: #1f2933; }
  h1 { font-size: 20px; } h2 { font-size: 16px; margin-top: 28px; }
  .card { background: #fff; border: 1px solid #e4e7eb; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #e4e7eb; padding: 6px 8px; text-align: left; }
  th { background: #f0f2f5; }
  button { padding: 8px 14px; border: 0; border-radius: 6px; cursor: pointer; margin-right: 8px; }
  .primary { background: #2563eb; color: #fff; } .warn { background: #d97706; color: #fff; }
  .ok { background: #059669; color: #fff; } .muted { background: #e4e7eb; color: #334155; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .b-old { background: #dbeafe; color: #1d4ed8; } .b-new { background: #d1fae5; color: #047857; }
  .b-switching { background: #fef3c7; color: #b45309; }
  .b-phase { background: #ede9fe; color: #6d28d9; }
  input, textarea { width: 100%; padding: 8px; margin: 4px 0 10px; border: 1px solid #cbd2d9; border-radius: 6px; box-sizing: border-box; }
  pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px; font-size: 12px; overflow:auto; max-height: 260px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  #msg { margin: 8px 0; font-size: 13px; white-space: pre-wrap; }
  .kv { display: grid; grid-template-columns: 160px 1fr; gap: 4px 12px; font-size: 13px; margin: 8px 0; }
  .kv .k { color: #64748b; }
  #progressCard { display: none; border-color: #c4b5fd; background: #faf8ff; }
  .spin { display:inline-block; width:12px; height:12px; border:2px solid #c4b5fd; border-top-color:#6d28d9; border-radius:50%; animation: rot 0.8s linear infinite; }
  @keyframes rot { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<h1>巡检工单库交接演示（PostgreSQL 逻辑复制）</h1>

<div class="card">
  <strong>当前状态：</strong><span id="state" class="badge b-old">-</span>
  &nbsp;接单库：<span id="accepting" class="badge b-old">-</span>
  &nbsp;在途写入：<span id="inflight">-</span>
  &nbsp;订阅：<span id="substate">-</span>
  <div style="margin-top:10px">
    <button class="muted" onclick="refresh()">刷新状态</button>
    <button class="primary" onclick="startCutover()">发起单向交接（旧库 → 新库）</button>
    <button class="warn" onclick="pauseSub()">暂停订阅（制造追平延迟）</button>
    <button class="ok" onclick="resumeSub()">恢复订阅</button>
  </div>
  <div id="msg"></div>
  <div id="recovermsg" style="margin-top:6px;font-size:13px;color:#b45309"></div>
</div>

<div class="card" id="progressCard">
  <h2 style="margin-top:0">交接实时进度 <span id="waitSpin" class="spin"></span></h2>
  <div class="kv">
    <span class="k">交接运行号</span><span id="pRunId">-</span>
    <span class="k">等待阶段</span><span><span id="pPhase" class="badge b-phase">-</span></span>
    <span class="k">已耗时</span><span id="pElapsed">-</span>
    <span class="k">在途写入数</span><span id="pInflight">-</span>
    <span class="k">库内活动写事务</span><span id="pDbInflight">-</span>
    <span class="k">发布端 current_lsn</span><span id="pCurrentLsn">-</span>
    <span class="k">订阅端 replay_lsn</span><span id="pReplayLsn">-</span>
    <span class="k">复制状态 / 订阅</span><span id="pSubState">-</span>
    <span class="k">旧库 / 新库行数</span><span id="pCounts">-</span>
    <span class="k">检查次数（0.5s/次）</span><span id="pChecks">-</span>
  </div>
  <div id="pResult" style="font-size:13px;margin-top:8px"></div>
</div>

<div class="card">
  <h2 style="margin-top:0">提交巡检工单（统一入口，自动发往当前接单库）</h2>
  <label>标题</label><input id="title" placeholder="例如：3号机组例行巡检">
  <label>详情</label><textarea id="detail" rows="2"></textarea>
  <button class="primary" onclick="submitOrder()">提交工单</button>
  <div id="submitmsg"></div>
</div>

<div class="grid">
  <div class="card">
    <h2 style="margin-top:0">旧库工单（发布端） <small id="oldsum"></small></h2>
    <table><thead><tr><th>编号</th><th>标题</th><th>来源</th><th>时间</th></tr></thead>
    <tbody id="oldrows"></tbody></table>
  </div>
  <div class="card">
    <h2 style="margin-top:0">新库工单（订阅端） <small id="newsum"></small></h2>
    <table><thead><tr><th>编号</th><th>标题</th><th>来源</th><th>时间</th></tr></thead>
    <tbody id="newrows"></tbody></table>
  </div>
</div>

<div class="card">
  <h2 style="margin-top:0">交接事件日志（仅阶段变化与结果，持久化）</h2>
  <pre id="events"></pre>
</div>

<script>
const PHASE_LABELS = {
  PAUSED: '旧库暂停接单',
  WAIT_INFLIGHT: '等待在途写入结束',
  WAIT_REPLAY: '等待逻辑复制追平',
  ALIGN_SEQUENCE: '对齐新库序列',
  COMMIT: '提交切换'
};
// 页面当前跟踪的交接运行号：等待阶段与读数只展示同一次交接
let activeRunId = null;
let progressTimer = null;

function badgeClass(v){ return v === 'NEW' ? 'b-new' : v === 'SWITCHING' ? 'b-switching' : 'b-old'; }
function show(el, text, color){ el.textContent = text; el.style.color = color || '#334155'; }
function shortId(id){ return id ? id.slice(0, 8) : '-'; }

async function refresh(){
  const s = await (await fetch('/api/status')).json();
  const st = document.getElementById('state');
  st.textContent = s.state; st.className = 'badge ' + badgeClass(s.state);
  const ac = document.getElementById('accepting');
  ac.textContent = s.accepting_db; ac.className = 'badge ' + badgeClass(s.accepting_db === 'new' ? 'NEW' : 'OLD');
  document.getElementById('inflight').textContent = s.inflight_writes;
  const sub = s.subscription.subscriber;
  document.getElementById('substate').textContent =
    sub.exists ? (sub.enabled ? '启用中' : '已暂停') : '不存在';
  if(s.recovered_run){
    document.getElementById('recovermsg').textContent =
      '重启恢复：检测到一次生效前中断的交接（' + shortId(s.recovered_run.run_id) +
      '），已恢复旧库接单并补记中断，见事件日志。';
  }
  // 页面打开时若已有交接在跑，自动绑定到该次交接的实时进度
  if(s.state === 'SWITCHING' && s.active_progress && s.active_progress.run_id && !activeRunId){
    attachProgress(s.active_progress.run_id);
  }

  const w = await (await fetch('/api/work-orders')).json();
  fillRows('oldrows', w.old); fillRows('newrows', w.new);
  document.getElementById('oldsum').textContent = '行数/最大号: ' + w.old_summary.count_max.join(' / ');
  document.getElementById('newsum').textContent = '行数/最大号: ' + w.new_summary.count_max.join(' / ');
  const ev = await (await fetch('/api/events')).json();
  document.getElementById('events').textContent =
    ev.map(e => new Date(e.ts*1000).toLocaleTimeString() + ' [' + e.phase + '] ' + e.message).join('\\n') || '（暂无）';
}

function fillRows(id, rows){
  document.getElementById(id).innerHTML = rows.map(r =>
    '<tr><td>' + r.id + '</td><td>' + escapeHtml(r.title) + '</td><td>' + r.source_db +
    '</td><td>' + new Date(r.created_at).toLocaleString() + '</td></tr>').join('') ||
    '<tr><td colspan="4">（无）</td></tr>';
}
function escapeHtml(s){ return (s||'').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

async function submitOrder(){
  const body = { title: document.getElementById('title').value.trim(), detail: document.getElementById('detail').value };
  if(!body.title){ show(document.getElementById('submitmsg'), '请填写标题', '#b91c1c'); return; }
  const r = await fetch('/api/work-orders', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  const d = await r.json();
  if(r.ok){
    show(document.getElementById('submitmsg'),
      '已受理：编号 ' + d.work_order.id + '，接单库：' + d.accepted_by, '#047857');
    document.getElementById('title').value = ''; document.getElementById('detail').value = '';
    refresh();
  } else {
    show(document.getElementById('submitmsg'), '提交失败：' + (d.detail || r.status), '#b91c1c');
  }
}

async function startCutover(){
  if(!confirm('确认发起单向交接？旧库将暂停接单，追平并对齐序列后由新库接单。')) return;
  const r = await fetch('/api/cutover', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({confirm:true}) });
  const d = await r.json();
  if(r.status === 409){
    show(document.getElementById('msg'), '已有交接正在进行中：' + d.error, '#b45309');
    if(d.state === 'SWITCHING'){ const s = await (await fetch('/api/progress')).json(); if(s.run_id) attachProgress(s.run_id); }
    return;
  }
  if(r.ok && d.already_done){
    show(document.getElementById('msg'), '交接早已生效，当前接单库：新库（重复点击不重复切换）', '#047857');
    refresh(); return;
  }
  if(r.ok && d.run_id){
    show(document.getElementById('msg'), '交接已开始（运行号 ' + shortId(d.run_id) + '），旧库已暂停接单', '#6d28d9');
    attachProgress(d.run_id);
  } else {
    show(document.getElementById('msg'), '发起失败：' + (d.error || r.status), '#b91c1c');
  }
  refresh();
}

// 绑定一次具体交接：之后所有等待阶段与读数都只来自该 run_id
function attachProgress(runId){
  activeRunId = runId;
  document.getElementById('progressCard').style.display = 'block';
  document.getElementById('pRunId').textContent = shortId(runId);
  if(progressTimer) clearInterval(progressTimer);
  pollProgress();
  progressTimer = setInterval(pollProgress, 500);
}

async function pollProgress(){
  if(!activeRunId) return;
  const r = await fetch('/api/progress?run_id=' + encodeURIComponent(activeRunId));
  const d = await r.json();
  if(d.stale){
    // 返回的读数不属于本次交接，停止展示避免串用不同次的等待状态
    finishProgress('本次交接的进度已不可用（读数属于另一次交接）', '#b91c1c');
    return;
  }
  if(!d.has_progress){ return; }
  document.getElementById('pPhase').textContent = PHASE_LABELS[d.phase] || d.phase;
  document.getElementById('pElapsed').textContent =
    ((Date.now() / 1000 - d.started_at)).toFixed(1) + ' s';
  document.getElementById('pChecks').textContent = d.checks_count;
  const c = d.latest_check || {};
  if(c.phase === 'WAIT_INFLIGHT'){
    document.getElementById('pInflight').textContent = c.inflight_writes;
    document.getElementById('pDbInflight').textContent = c.db_active_writes;
  } else {
    document.getElementById('pInflight').textContent = '0（已结束）';
    document.getElementById('pDbInflight').textContent = '0';
  }
  if(c.phase === 'WAIT_REPLAY'){
    document.getElementById('pCurrentLsn').textContent = c.current_lsn || '-';
    document.getElementById('pReplayLsn').textContent = c.replay_lsn || '-';
    document.getElementById('pSubState').textContent =
      (c.sub_state || '无连接') + ' / ' + (c.subscription_enabled ? '订阅启用' : '订阅暂停');
    document.getElementById('pCounts').textContent =
      c.old_count + ' / ' + c.new_count + (c.caught_up ? '（已追平）' : '（追平中）');
  }
  if(d.result){
    // 有结果后结束等待显示
    if(d.result.ok){
      finishProgress('交接成功！首个新编号：' + d.result.first_new_id +
        '，旧库最高预留号：' + d.result.old_highest_reserved + '，新库开始接单。', '#047857');
    } else {
      finishProgress('交接失败已回滚，旧库恢复接单：' + d.result.reason, '#b91c1c');
    }
    refresh();
  }
}

function finishProgress(text, color){
  if(progressTimer){ clearInterval(progressTimer); progressTimer = null; }
  document.getElementById('waitSpin').style.display = 'none';
  const el = document.getElementById('pResult');
  show(el, text, color);
  activeRunId = null;
}

async function pauseSub(){
  const r = await fetch('/api/subscription/pause', { method:'POST' });
  show(document.getElementById('msg'), r.ok ? '订阅已暂停，可继续在旧库写工单制造延迟，再发起交接观察长等待与超时' : '操作失败', r.ok ? '#b45309' : '#b91c1c');
  refresh();
}
async function resumeSub(){
  const r = await fetch('/api/subscription/resume', { method:'POST' });
  show(document.getElementById('msg'), r.ok ? '订阅已恢复，积压数据将开始追平' : '操作失败', r.ok ? '#047857' : '#b91c1c');
  refresh();
}
refresh(); setInterval(refresh, 4000);
</script>
</body>
</html>
"""
