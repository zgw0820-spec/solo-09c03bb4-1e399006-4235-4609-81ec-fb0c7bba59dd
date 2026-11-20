"""单向交接状态机（后台线程 + 阶段化进度 + 重启恢复）。

流程（OLD -> SWITCHING -> NEW）：
  1. 旧库暂停接单（持久化 SWITCHING 与交接运行记录，同一事务）。
  2. 等待应用在途写入结束（进程内计数器 + 旧库活动写事务双重确认）。
  3. 每 0.5s 检查一次逻辑复制进度：发布端 current_lsn 与 replay_lsn 一致、
     state=streaming，且两库工单表行数/最大编号一致。
  4. 对齐新库序列（缓存未用号、回滚空号计入旧库预留最高号）。
  5. 接单库切 NEW 与最终事件在同一事务提交，一起生效。

日志策略：
  - 每 0.5s 的检查读数只写入内存进度（供网页实时展示），不落 SQLite；
  - 只有阶段变化与最终结果才写持久事件，反复检查同一阶段不重复落盘。

重启恢复（见 state.resume_interrupted）：
  - 生效前退出：运行记录停在 INTERRUPTED，重启恢复旧库接单并补记中断；
  - 生效后退出：NEW 与 COMPLETED 已同事务提交，重启仍由新库接单，原结果可查。

重复触发：已在 NEW 时直接返回当前接单库；交接进行中再次发起返回 409。
"""
import threading
import time
from collections import deque

from . import db
from .config import Settings
from .state import StateStore

# 交接阶段（持久事件只在阶段变化时写一条）
PHASE_PAUSED = "PAUSED"                # 旧库已暂停接单
PHASE_WAIT_INFLIGHT = "WAIT_INFLIGHT"  # 等待在途写入结束
PHASE_WAIT_REPLAY = "WAIT_REPLAY"      # 等待逻辑复制追平（0.5s 轮询）
PHASE_ALIGN_SEQUENCE = "ALIGN_SEQUENCE"  # 对齐新库序列
PHASE_COMMIT = "COMMIT"                # 提交切换


class CutoverError(Exception):
    """交接失败（含超时）。触发后旧库恢复接单。"""


class CutoverManager:
    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store
        # 应用自身在途写事务计数；单进程 uvicorn 下即可反映网页提交的在途写入
        self.inflight = 0
        self.inflight_lock = threading.Lock()
        # 交接串行化：交接进行中的再次触发返回 409
        self.switch_lock = threading.Lock()
        # 内存实时进度（半秒检查读数只留这里，不落盘）
        self._progress: dict | None = None
        self._progress_lock = threading.Lock()
        self._active_run_id: str | None = None
        self._recovered: dict | None = None
        self._recover_on_startup()

    # ---- 启动恢复 -----------------------------------------------------
    def _recover_on_startup(self) -> None:
        state = self.store.get_state()
        if state == "NEW":
            # 交接已生效：继续由新库接单，结果在事件与运行记录中可查
            return
        interrupted = self.store.get_interrupted_run()
        if interrupted or state == "SWITCHING":
            # 生效前退出：补记中断，恢复旧库接单
            self._recovered = self.store.resume_interrupted()

    @property
    def recovered_run(self) -> dict | None:
        return self._recovered

    # ---- 在途写入计数 -------------------------------------------------
    def enter_write(self) -> None:
        with self.inflight_lock:
            self.inflight += 1

    def exit_write(self) -> None:
        with self.inflight_lock:
            self.inflight = max(0, self.inflight - 1)

    def current_state(self) -> str:
        return self.store.get_state()

    def accepting_db(self) -> str:
        return "new" if self.store.get_state() == "NEW" else "old"

    # ---- 交接发起（立即返回，后台线程执行）----------------------------
    def start_cutover(self) -> dict:
        if self.store.get_state() == "NEW":
            return {
                "already_done": True,
                "state": "NEW",
                "accepting_db": "new",
                "message": "交接已生效，当前由新库接单",
            }
        if not self.switch_lock.acquire(blocking=False):
            raise CutoverError("已有交接正在进行中")

        run_id = self.store.new_run_id()
        started_at = time.time()
        # 运行记录（INTERRUPTED）+ SWITCHING 状态 + 首条事件同事务落盘：
        # 即使此刻应用退出，重启也能查到这次未生效的交接。
        self.store.begin_cutover(run_id, started_at)
        with self._progress_lock:
            self._progress = {
                "run_id": run_id,
                "running": True,
                "phase": PHASE_PAUSED,
                "started_at": started_at,
                "checks_count": 0,
                "checks": deque(maxlen=300),
                "result": None,
            }
            self._active_run_id = run_id
        thread = threading.Thread(
            target=self._run_cutover, args=(run_id,), name=f"cutover-{run_id[:8]}",
            daemon=True,
        )
        thread.start()
        return {
            "accepted": True,
            "run_id": run_id,
            "state": "SWITCHING",
            "message": "交接已开始，旧库暂停接单，可通过 /api/progress 查看实时进度",
        }

    def _set_phase(self, run_id: str, phase: str, message: str,
                   detail: dict | None = None) -> None:
        with self._progress_lock:
            if self._progress and self._progress["run_id"] == run_id:
                self._progress["phase"] = phase
        # 只有阶段变化才写持久日志
        self.store.update_run_phase(run_id, phase, message, detail)

    def _add_check(self, run_id: str, check: dict) -> None:
        with self._progress_lock:
            if self._progress and self._progress["run_id"] == run_id:
                check = {"ts": time.time(), **check}
                self._progress["checks"].append(check)
                self._progress["checks_count"] += 1

    def _run_cutover(self, run_id: str) -> None:
        s = self.settings
        deadline = time.monotonic() + s.cutover_timeout_seconds

        def remaining() -> float:
            return deadline - time.monotonic()

        def sleep_with_timeout(seconds: float) -> None:
            if remaining() <= 0:
                raise CutoverError("交接超时")
            time.sleep(min(seconds, remaining()))

        try:
            # 1) 等待在途写入结束（应用计数 + 数据库活动事务双重确认）
            self._wait_inflight(run_id, remaining, sleep_with_timeout)

            # 2) 阶段变化落盘：进入复制追平等待
            self._set_phase(
                run_id, PHASE_WAIT_REPLAY,
                "在途写入已全部结束，开始等待逻辑复制追平",
            )
            self._wait_replication_caught_up(run_id, remaining, sleep_with_timeout)

            # 3) 数据一致性核对 + 序列对齐
            self._set_phase(
                run_id, PHASE_ALIGN_SEQUENCE,
                "逻辑复制已追平，旧库已提交工单全部到达新库，开始对齐序列",
            )
            seq = db.sequence_snapshot(s.old_dsn)
            old_count, old_max = db.count_and_max(s.old_dsn)
            new_count, new_max = db.count_and_max(s.new_dsn)
            if old_count != new_count or old_max != new_max:
                raise CutoverError(
                    f"两库数据不一致: old(count={old_count},max={old_max}) "
                    f"new(count={new_count},max={new_max})"
                )
            first_new_id = seq["first_new_id"]
            db.align_sequence(s.new_dsn, first_new_id)
            summary = {
                "old_sequence": seq,
                "old_highest_reserved": seq["highest_reserved"],
                "first_new_id": first_new_id,
                "row_count": old_count,
            }

            # 4) 提交切换：接单库 NEW + 最终事件同事务一起生效
            self._set_phase(
                run_id, PHASE_COMMIT,
                f"新库序列已对齐，首个新编号 {first_new_id}（旧库预留最高号 "
                f"{seq['highest_reserved']}，含缓存未用号与回滚空号），提交切换",
                summary,
            )
            ended_at = time.time()
            self.store.commit_cutover(run_id, ended_at, summary)
            with self._progress_lock:
                if self._progress and self._progress["run_id"] == run_id:
                    self._progress["running"] = False
                    self._progress["result"] = {
                        "ok": True,
                        "ended_at": ended_at,
                        **summary,
                    }
        except CutoverError as exc:
            self._finish_with_rollback(run_id, str(exc))
        except Exception as exc:  # 任何意外错误同样回滚
            self._finish_with_rollback(run_id, f"交接出错: {exc!r}")
        finally:
            self.switch_lock.release()

    def _finish_with_rollback(self, run_id: str, reason: str) -> None:
        ended_at = time.time()
        self.store.rollback_cutover(run_id, ended_at, reason)
        with self._progress_lock:
            if self._progress and self._progress["run_id"] == run_id:
                self._progress["running"] = False
                self._progress["result"] = {"ok": False, "reason": reason,
                                            "ended_at": ended_at}

    def _wait_inflight(self, run_id, remaining, sleep) -> None:
        while True:
            app_inflight = self.inflight
            db_inflight = db.count_active_app_transactions(self.settings.old_dsn)
            # 半秒检查读数只写内存进度，不落盘（阶段未变化）
            self._add_check(run_id, {
                "phase": PHASE_WAIT_INFLIGHT,
                "inflight_writes": app_inflight,
                "db_active_writes": db_inflight,
            })
            if app_inflight == 0 and db_inflight == 0:
                return
            if remaining() <= 0:
                raise CutoverError(
                    f"等待在途写入结束超时（应用计数={app_inflight}，库内活动写事务={db_inflight}）"
                )
            sleep(self.settings.poll_interval_seconds)

    def _wait_replication_caught_up(self, run_id, remaining, sleep) -> None:
        s = self.settings
        while True:
            pub = db.publisher_wal_positions(s.old_dsn)
            sub = db.subscriber_status(s.new_dsn, s.subscription_name)
            replay = pub.get("replay_lsn")
            current = pub["current_lsn"]
            enabled = sub.get("enabled", False)
            old_count, old_max = db.count_and_max(s.old_dsn)
            new_count, new_max = db.count_and_max(s.new_dsn)
            caught_up = (
                enabled
                and replay is not None
                and pub.get("sub_state") == "streaming"
                and str(replay) == str(current)
                and old_count == new_count
                and old_max == new_max
            )
            # 每 0.5s 一条检查日志：仅内存，不写 SQLite
            self._add_check(run_id, {
                "phase": PHASE_WAIT_REPLAY,
                "inflight_writes": self.inflight,
                "db_active_writes": db.count_active_app_transactions(s.old_dsn),
                "current_lsn": str(current),
                "replay_lsn": str(replay) if replay is not None else None,
                "sent_lsn": str(pub["sent_lsn"]) if pub.get("sent_lsn") is not None else None,
                "sub_state": pub.get("sub_state"),
                "subscription_enabled": enabled,
                "old_count": old_count,
                "new_count": new_count,
                "old_max": old_max,
                "new_max": new_max,
                "caught_up": caught_up,
            })
            if caught_up:
                return
            if remaining() <= 0:
                raise CutoverError(
                    f"等待复制追平超时（current={current}，replay={replay}，"
                    f"sub_state={pub.get('sub_state')}）"
                )
            sleep(s.poll_interval_seconds)

    # ---- 实时进度（网页轮询）------------------------------------------
    def progress(self, run_id: str | None = None) -> dict | None:
        with self._progress_lock:
            if not self._progress:
                return None
            if run_id is not None and self._progress["run_id"] != run_id:
                # 请求的是另一次交接：不返回当前进度，避免页面串用不同次的读数
                return {"run_id": run_id, "stale": True, "running": False}
            checks = list(self._progress["checks"])
            return {
                "run_id": self._progress["run_id"],
                "running": self._progress["running"],
                "phase": self._progress["phase"],
                "started_at": self._progress["started_at"],
                "checks_count": self._progress["checks_count"],
                "latest_check": checks[-1] if checks else None,
                "recent_checks": checks[-10:],
                "result": self._progress["result"],
                "state": self.store.get_state(),
                "run": self.store.get_run(self._progress["run_id"]),
            }

    def get_run(self, run_id: str) -> dict | None:
        return self.store.get_run(run_id)

    def list_runs(self) -> list[dict]:
        return self.store.list_runs()

    # ---- 订阅暂停/恢复 ------------------------------------------------
    def pause_subscription(self) -> dict:
        db.alter_subscription(
            self.settings.new_dsn, self.settings.subscription_name, enable=False
        )
        self.store.log_event("SUBSCRIPTION", "订阅已暂停（发布端 WAL 将保留，形成复制延迟）")
        return self.subscription_overview()

    def resume_subscription(self) -> dict:
        db.alter_subscription(
            self.settings.new_dsn, self.settings.subscription_name, enable=True
        )
        self.store.log_event("SUBSCRIPTION", "订阅已恢复")
        return self.subscription_overview()

    def subscription_overview(self) -> dict:
        sub = db.subscriber_status(self.settings.new_dsn, self.settings.subscription_name)
        pub = db.publisher_wal_positions(self.settings.old_dsn)
        return {"subscriber": sub, "publisher": pub}
