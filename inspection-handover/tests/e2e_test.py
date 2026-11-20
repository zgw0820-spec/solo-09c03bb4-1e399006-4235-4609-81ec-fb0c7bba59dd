#!/usr/bin/env python3
"""端到端试跑（异步交接 + 实时进度 + 长等待 + 生效前后重启恢复）。

前置：scripts/local_pg.sh start 已启动两库。
本脚本自行通过 scripts/local_app.sh 启停应用（含 SIGKILL 模拟生效前崩溃），
用 -I 运行（隔离模式），仅使用标准库 + 虚拟环境内的 psycopg2。

覆盖：
  0. 初始状态与统一入口接单
  1. 逻辑复制同步
  2. 暂停订阅 -> 交接长等待（0.5s 检查、实时进度、在途数与 LSN 读数）-> 超时回滚
  3. 持久日志只含阶段变化/结果，半秒检查不落盘；run_id 隔离读数
  4. 恢复订阅追平 + 在途慢事务结束才完成交接
  5. 序列预留（回滚空号、缓存未用号）与首个新编号
  6. 生效前重启（SIGKILL）：恢复旧库接单并补记中断
  7. 重新交接成功；生效后重启：仍由新库接单，原结果可查
  8. 重复交接幂等、503 暂停接单、409 串行冲突
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg2

BASE = "http://127.0.0.1:8000"
OLD_DSN = "host=127.0.0.1 port=55433 user=node dbname=inspection"
NEW_DSN = "host=127.0.0.1 port=55434 user=node dbname=inspection"
ROOT = Path(__file__).resolve().parents[1]
STATE_DB = ROOT / ".localrun" / "data" / "handover_state.db"
APP_SCRIPT = ROOT / "scripts" / "local_app.sh"
APP_LOG = ROOT / ".localrun" / "logs" / "app_test.log"

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"[PASS] {name}" + (f" -- {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"[FAIL] {name}" + (f" -- {detail}" if detail else ""))


def api(method, path, body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def sql(dsn, query):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query)
        if cur.description:
            return cur.fetchall()
        conn.commit()


def wait_http_up(timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            api("GET", "/api/status")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def start_app():
    log = open(APP_LOG, "ab")
    proc = subprocess.Popen([str(APP_SCRIPT)], stdout=log, stderr=log,
                            start_new_session=True)
    assert wait_http_up(), "应用启动失败"
    return proc


def stop_app(proc, hard=False):
    if proc.poll() is not None:
        return
    if hard:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    else:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    time.sleep(0.5)


def start_cutover():
    code, body = api("POST", "/api/cutover", {"confirm": True})
    return code, body


def wait_run_terminal(run_id, timeout=30):
    """轮询 /api/progress 直到该 run 出结果，返回最终进度。"""
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = api("GET", f"/api/progress?run_id={run_id}")
        if last.get("has_progress") and last.get("result"):
            return last
        time.sleep(0.2)
    raise AssertionError(f"run {run_id} 未在 {timeout}s 内结束: {last}")


def wait_state(expected, timeout=20):
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = api("GET", "/api/status")
        if last.get("state") == expected:
            return last
        time.sleep(0.3)
    raise AssertionError(f"状态未变为 {expected}: {last}")


def wait_rows_equal(timeout=20):
    deadline = time.time() + timeout
    old = new = None
    while time.time() < deadline:
        old = sql(OLD_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        new = sql(NEW_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        if old == new:
            return old, new
        time.sleep(0.5)
    raise AssertionError(f"两库行数未追平: old={old} new={new}")


def seq_info(dsn):
    return sql(dsn, """
        SELECT seqstart, seqcache, pg_sequence_last_value('work_orders_id_seq')
        FROM pg_sequence WHERE seqrelid='work_orders_id_seq'::regclass""")[0]


def reset_state_to_old():
    conn = sqlite3.connect(STATE_DB)
    conn.execute("UPDATE kv SET value='OLD' WHERE key='state'")
    conn.commit()
    conn.close()


def main():
    app = start_app()
    try:
        print("== 0. 初始状态 ==")
        _, body = api("GET", "/api/status")
        check("初始接单库为旧库 OLD", body["state"] == "OLD" and body["accepting_db"] == "old", str(body)[:120])
        check("初始无未恢复的中断记录", body.get("recovered_run") is None)

        print("== 1. 旧库提交工单，逻辑复制同步 ==")
        ids = []
        for i in range(3):
            code, body = api("POST", "/api/work-orders", {"title": f"巡检工单{i+1}"})
            check(f"工单{i+1}由旧库受理", code == 200 and body["accepted_by"] == "old")
            ids.append(body["work_order"]["id"])
        # 回滚空号 + 缓存未用号
        with psycopg2.connect(OLD_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT nextval('work_orders_id_seq')")
            conn.rollback()
        c = psycopg2.connect(OLD_DSN); cur = c.cursor()
        cur.execute("SELECT nextval('work_orders_id_seq')"); cur.close(); c.close()
        wait_rows_equal()
        check("新库收齐旧库已提交工单", True)

        print("== 2. 暂停订阅 -> 交接长等待 -> 超时回滚 ==")
        code, body = api("POST", "/api/subscription/pause")
        check("订阅暂停成功", code == 200 and body["subscriber"]["enabled"] is False)
        code, body = api("POST", "/api/work-orders", {"title": "暂停期间工单"})
        check("暂停期间旧库仍接单", code == 200 and body["accepted_by"] == "old")
        t0 = time.time()
        code, body = start_cutover()
        check("交接请求被受理并立即返回 run_id（202）",
              code == 202 and body.get("run_id") and body["state"] == "SWITCHING", f"code={code}")
        run_id = body["run_id"]

        # 进行中：503 暂停接单、409 串行冲突
        code, _ = api("POST", "/api/work-orders", {"title": "切换期拦截"})
        check("SWITCHING 期间网页提交返回 503", code == 503, f"code={code}")
        code, body2 = start_cutover()
        check("交接进行中再次发起返回 409", code == 409, f"code={code}")

        # 实时进度：等待阶段与读数属于同一次交接（按 run_id 隔离）
        time.sleep(1.2)
        _, prog = api("GET", f"/api/progress?run_id={run_id}")
        check("进度绑定本次交接 run_id", prog.get("run_id") == run_id and prog.get("running") is True)
        check("进度阶段为等待复制追平", prog.get("phase") == "WAIT_REPLAY", prog.get("phase"))
        latest = prog.get("latest_check") or {}
        check("实时读数含在途写入数", latest.get("inflight_writes") == 0)
        check("实时读数含复制位置 current/replay LSN",
              latest.get("current_lsn") and latest.get("replay_lsn") is None
              and latest.get("subscription_enabled") is False, str(latest)[:160])
        check("每 0.5s 一次检查，检查数随时间增长", prog.get("checks_count", 0) >= 2,
              f"checks={prog.get('checks_count')}")
        # 别的 run_id 拿不到这次的读数
        _, stale = api("GET", "/api/progress?run_id=other-run-id")
        check("不同交接 run_id 的读数隔离（stale）", stale.get("stale") is True)

        final = wait_run_terminal(run_id, timeout=20)
        elapsed = time.time() - t0
        check("长等待后超时回滚，结果为失败", final["result"]["ok"] is False
              and "超时" in final["result"]["reason"], str(final["result"])[:120])
        check("超时耗时接近阈值 8s", 7 <= elapsed <= 14, f"{elapsed:.1f}s")
        _, st = api("GET", "/api/status")
        check("回滚后恢复 OLD 旧库接单", st["state"] == "OLD" and st["accepting_db"] == "old")
        code, _ = api("POST", "/api/work-orders", {"title": "回滚后工单"})
        check("回滚后网页可继续向旧库提交", code == 200)

        print("== 3. 持久日志只随阶段/结果变化，检查读数不落盘 ==")
        _, ev = api("GET", "/api/events")
        run_events = [e for e in ev if run_id in json.dumps(e, ensure_ascii=False)]
        phases = {e["phase"] for e in run_events}
        check("持久事件仅阶段变化与回滚结果（无半秒检查刷屏）",
              phases == {"SWITCHING", "WAIT_REPLAY", "OLD"}
              and len(run_events) == 3, str([e["phase"] for e in run_events]))
        check("没有任何事件携带 LSN 轮询读数",
              all("current=" not in e["message"] or "超时" in e["message"] for e in run_events))
        check("运行记录可按 run_id 查询且状态 ROLLED_BACK",
              api("GET", f"/api/runs/{run_id}")[1]["status"] == "ROLLED_BACK")

        print("== 4. 恢复订阅追平 + 在途慢事务结束才完成 ==")
        code, _ = api("POST", "/api/subscription/resume")
        check("订阅恢复成功", code == 200)
        wait_rows_equal()
        slow = subprocess.Popen(
            [sys.executable, "-c",
             "import psycopg2,time;"
             "c=psycopg2.connect('" + OLD_DSN + "');cur=c.cursor();"
             "cur.execute(\"INSERT INTO work_orders(title,detail,source_db) VALUES ('慢事务工单','在途','old')\");"
             "time.sleep(3);c.commit();c.close()"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.0)
        t0 = time.time()
        code, body = start_cutover()
        slow_run = body["run_id"]
        _, p0 = api("GET", f"/api/progress?run_id={slow_run}")
        check("交接先停在等待在途写入阶段", p0.get("phase") in ("PAUSED", "WAIT_INFLIGHT"))
        final = wait_run_terminal(slow_run, timeout=20)
        elapsed = time.time() - t0
        slow.wait(timeout=10)
        check("在途写入结束后交接成功", final["result"]["ok"] is True, str(final["result"])[:100])
        check("交接确实等待了在途写入（>=2.0s）", elapsed >= 2.0, f"{elapsed:.1f}s")
        # 慢事务工单已随交接同步：两库行数一致
        wait_rows_equal()

        print("== 5. 生效后新库序列与编号接续 ==")
        old_seq = seq_info(OLD_DSN)
        _, _, old_last = old_seq
        actual_max = sql(OLD_DSN, "SELECT max(id) FROM work_orders")[0][0]
        check("存在缓存未用号（实际最大发号 < 预留最高号）", actual_max < old_last,
              f"max={actual_max} reserved={old_last}")
        st = wait_state("NEW")
        check("状态已切 NEW 由新库接单", st["accepting_db"] == "new")
        new_seq = seq_info(NEW_DSN)
        check("新库序列 setval 到预留最高号", new_seq[2] == old_last, str(new_seq))
        code, body = api("POST", "/api/work-orders", {"title": "新库首单"})
        check("新库首个编号严格大于旧库预留最高号",
              code == 200 and body["accepted_by"] == "new"
              and body["work_order"]["id"] == old_last + 1,
              f"id={body.get('work_order', {}).get('id')} expected={old_last + 1}")

        print("== 6. 生效前重启（SIGKILL 模拟崩溃）：恢复旧库并补记中断 ==")
        # 状态拨回 OLD 后不向旧库写新数据（新库已有第 5 节的工单，避免主键冲突）；
        # 暂停订阅制造长等待窗口，确保 SIGKILL 时交接尚未生效
        reset_state_to_old()
        api("POST", "/api/subscription/pause")
        _, body = start_cutover()
        crash_run = body["run_id"]
        time.sleep(1.0)
        _, mid = api("GET", f"/api/progress?run_id={crash_run}")
        check("崩溃前交接处于等待阶段且无结果",
              mid.get("running") is True and mid.get("result") is None, mid.get("phase"))
        stop_app(app, hard=True)
        app = start_app()
        _, st = api("GET", "/api/status")
        check("生效前重启后恢复旧库接单", st["state"] == "OLD" and st["accepting_db"] == "old")
        rec = st.get("recovered_run") or {}
        check("启动恢复识别到中断的交接运行", rec.get("run_id") == crash_run
              and rec.get("status") == "ROLLED_BACK", str(rec)[:120])
        _, runrec = api("GET", f"/api/runs/{crash_run}")
        check("中断运行记录可查且原因为生效前退出",
              runrec["status"] == "ROLLED_BACK" and "生效前退出" in runrec["result_detail"]["reason"])
        _, ev = api("GET", "/api/events")
        check("事件日志补记了本次交接中断",
              any("补记本次交接中断" in e["message"] and crash_run in json.dumps(e, ensure_ascii=False)
                  for e in ev))
        api("POST", "/api/subscription/resume")
        # 恢复后新库包含全部旧库工单（新库另有第 5 节直接写入的新库首单，属正常）
        deadline = time.time() + 20
        synced = False
        while time.time() < deadline:
            old_rows = sql(OLD_DSN, "SELECT count(*) FROM work_orders")[0][0]
            new_rows = sql(NEW_DSN, "SELECT count(*) FROM work_orders")[0][0]
            if new_rows >= old_rows:
                synced = True
                break
            time.sleep(0.5)
        check("恢复订阅后旧库工单全部到达新库", synced, f"old={old_rows} new={new_rows}")
        check("恢复后旧库接单链路可用（状态 OLD，未产生分歧数据）",
              api("GET", "/api/status")[1]["accepting_db"] == "old")

        print("== 7. 重新交接成功；生效后重启：仍由新库接单、原结果可查 ==")
        # 清掉第 5 节直接写入新库的演示工单，使两库回到“旧库数据已全部同步”的
        # 交接前置状态（序列保持对齐，不影响编号接续验证）
        sql(NEW_DSN, "DELETE FROM work_orders WHERE source_db = 'new'")
        code, body = start_cutover()
        final = wait_run_terminal(body["run_id"], timeout=20)
        check("重新交接成功", final["result"]["ok"] is True)
        good_run = body["run_id"]
        wait_state("NEW")
        code, body = api("POST", "/api/work-orders", {"title": "新库首单"})
        first_new = body["work_order"]["id"]
        check("新库接单且编号接续", code == 200 and body["accepted_by"] == "new"
              and first_new > old_last, f"id={first_new}")
        # 生效后重启（优雅退出）
        stop_app(app, hard=False)
        app = start_app()
        _, st = api("GET", "/api/status")
        check("生效后重启仍由新库接单", st["state"] == "NEW" and st["accepting_db"] == "new")
        check("生效后重启无中断恢复提示", st.get("recovered_run") is None)
        _, runrec = api("GET", f"/api/runs/{good_run}")
        check("原交接结果仍可查到（COMPLETED，首个新编号保留）",
              runrec["status"] == "COMPLETED" and runrec["result_detail"]["first_new_id"],
              str(runrec)[:120])
        code, body = api("POST", "/api/work-orders", {"title": "重启后新库工单"})
        check("重启后新库继续接单且编号递增",
              code == 200 and body["accepted_by"] == "new"
              and body["work_order"]["id"] > first_new)

        print("== 8. 重复交接幂等 ==")
        code, body = start_cutover()
        check("NEW 下重复交接返回 already_done 不重复切换",
              code == 200 and body.get("already_done") is True and body["accepting_db"] == "new")

    finally:
        stop_app(app)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
