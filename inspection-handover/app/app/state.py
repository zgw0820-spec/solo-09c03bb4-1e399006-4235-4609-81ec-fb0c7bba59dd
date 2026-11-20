"""交接状态持久化。

接单库状态与交接运行记录保存在 SQLite（挂载到持久卷），应用重启后沿用。
状态取值：
  OLD        旧库接单（初始）
  SWITCHING  交接进行中：旧库已暂停接单，等待追平
  NEW        新库接单（已生效）

关键事务边界：
  - begin_cutover：交接运行记录（INTERRUPTED）与 SWITCHING 状态同一个事务落盘，
    因此应用在生效前任一时刻退出，重启时都能查到这次未完成的交接。
  - commit_cutover：接单库切到 NEW、最终事件、运行结果同一个事务提交（一起生效）。
  - rollback_cutover / resume_interrupted：回到 OLD 并补记回滚/中断事件。
持久事件只在交接阶段变化与出结果时写入；半秒一次的轮询读数只留在内存进度里。
"""
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

VALID_STATES = ("OLD", "SWITCHING", "NEW")
RUN_TERMINAL = ("COMPLETED", "ROLLED_BACK")


class StateStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts         REAL NOT NULL,
                    phase      TEXT NOT NULL,
                    message    TEXT NOT NULL,
                    detail     TEXT
                );
                CREATE TABLE IF NOT EXISTS cutover_runs (
                    run_id        TEXT PRIMARY KEY,
                    status        TEXT NOT NULL,
                    last_phase    TEXT,
                    final_phase   TEXT,
                    started_at    REAL NOT NULL,
                    ended_at      REAL,
                    result_detail TEXT
                );
                """
            )
            cur = self._conn.execute("SELECT value FROM kv WHERE key = 'state'")
            if cur.fetchone() is None:
                self._conn.execute(
                    "INSERT INTO kv(key, value) VALUES ('state', 'OLD')"
                )
            self._conn.commit()

    # ---- 接单库状态 ---------------------------------------------------
    def get_state(self) -> str:
        with self._lock:
            cur = self._conn.execute("SELECT value FROM kv WHERE key = 'state'")
            return cur.fetchone()["value"]

    # ---- 持久事件（仅阶段变化/结果/人工操作）--------------------------
    def log_event(self, phase: str, message: str, detail: dict | None = None) -> dict:
        with self._lock:
            ts = time.time()
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (ts, phase, message, json.dumps(detail, ensure_ascii=False) if detail else None),
            )
            self._conn.commit()
            return {"ts": ts, "phase": phase, "message": message, "detail": detail or {}}

    def list_events(self, limit: int = 100) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT ts, phase, message, detail FROM events "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [
                {
                    "ts": r["ts"],
                    "phase": r["phase"],
                    "message": r["message"],
                    "detail": json.loads(r["detail"]) if r["detail"] else {},
                }
                for r in cur.fetchall()
            ]

    # ---- 交接运行记录 -------------------------------------------------
    @staticmethod
    def new_run_id() -> str:
        return uuid.uuid4().hex

    def begin_cutover(self, run_id: str, started_at: float) -> None:
        """交接开始：运行记录（INTERRUPTED）+ SWITCHING 状态 + 首条事件，同一事务。"""
        with self._lock:
            self._conn.execute(
                "INSERT INTO cutover_runs(run_id, status, started_at) VALUES (?, 'INTERRUPTED', ?)",
                (run_id, started_at),
            )
            self._conn.execute("UPDATE kv SET value = 'SWITCHING' WHERE key = 'state'")
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (started_at, "SWITCHING", "旧库已暂停接单，开始单向交接",
                 json.dumps({"run_id": run_id}, ensure_ascii=False)),
            )
            self._conn.commit()

    def update_run_phase(self, run_id: str, phase: str, message: str,
                         detail: dict | None = None) -> None:
        """阶段变化才落盘：更新运行记录的最近阶段并写一条阶段事件（同一事务）。"""
        with self._lock:
            ts = time.time()
            payload = {"run_id": run_id, **(detail or {})}
            self._conn.execute(
                "UPDATE cutover_runs SET last_phase = ? WHERE run_id = ?",
                (phase, run_id),
            )
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (ts, phase, message, json.dumps(payload, ensure_ascii=False)),
            )
            self._conn.commit()

    def commit_cutover(self, run_id: str, ended_at: float, summary: dict) -> None:
        """交接生效：NEW 接单库 + 最终事件 + 运行结果，同一事务一起生效。"""
        with self._lock:
            self._conn.execute(
                "UPDATE cutover_runs SET status = 'COMPLETED', final_phase = 'NEW', "
                "ended_at = ?, result_detail = ? WHERE run_id = ?",
                (ended_at, json.dumps(summary, ensure_ascii=False), run_id),
            )
            self._conn.execute("UPDATE kv SET value = 'NEW' WHERE key = 'state'")
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (ended_at, "NEW", "交接生效，新库开始接单",
                 json.dumps({"run_id": run_id, **summary}, ensure_ascii=False)),
            )
            self._conn.commit()

    def rollback_cutover(self, run_id: str, ended_at: float, reason: str) -> None:
        """交接失败/超时：OLD 接单库 + 回滚事件 + 运行结果，同一事务。"""
        with self._lock:
            detail = {"run_id": run_id, "reason": reason}
            self._conn.execute(
                "UPDATE cutover_runs SET status = 'ROLLED_BACK', final_phase = 'OLD', "
                "ended_at = ?, result_detail = ? WHERE run_id = ?",
                (ended_at, json.dumps(detail, ensure_ascii=False), run_id),
            )
            self._conn.execute("UPDATE kv SET value = 'OLD' WHERE key = 'state'")
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (ended_at, "OLD", f"交接回滚，旧库恢复接单：{reason}",
                 json.dumps(detail, ensure_ascii=False)),
            )
            self._conn.commit()

    def get_interrupted_run(self) -> dict | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT run_id, status, last_phase, started_at FROM cutover_runs "
                "WHERE status = 'INTERRUPTED' ORDER BY started_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def get_run(self, run_id: str) -> dict | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT run_id, status, last_phase, final_phase, started_at, "
                "ended_at, result_detail FROM cutover_runs WHERE run_id = ?",
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            item = dict(row)
            item["result_detail"] = (
                json.loads(item["result_detail"]) if item["result_detail"] else {}
            )
            return item

    def list_runs(self, limit: int = 50) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT run_id, status, last_phase, final_phase, started_at, "
                "ended_at, result_detail FROM cutover_runs "
                "ORDER BY started_at DESC LIMIT ?",
                (limit,),
            )
            items = []
            for r in cur.fetchall():
                item = dict(r)
                item["result_detail"] = (
                    json.loads(item["result_detail"]) if item["result_detail"] else {}
                )
                items.append(item)
            return items

    def resume_interrupted(self) -> dict | None:
        """启动恢复：生效前退出的交接（INTERRUPTED）一律补记中断并恢复旧库接单。"""
        run = self.get_interrupted_run()
        if not run:
            return None
        reason = (
            f"应用在交接生效前退出（中断于阶段 {run['last_phase'] or '启动交接'}），"
            "重启后恢复旧库接单并补记本次交接中断"
        )
        self.rollback_cutover(run["run_id"], time.time(), reason)
        return self.get_run(run["run_id"])
