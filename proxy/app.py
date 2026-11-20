#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页（含固件联调“演练”编排）。

普通透传（原有能力）：
- 按客户端 IP:端口建立会话，空闲超时（默认 5 秒）后过期；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  回包天然只能属于一个会话；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配，旧回包不可能进入新会话；
- 报文与回包原样透传，不做任何修改；
- 当前目标保存在 SQLite，重启后保留；会话全在内存，重启即清空。

演练（固件联调故障复现，例如“收首条读数不回包、等重传再回”）：
- 首页选择接收器 A/B，编排 2~8 步回包动作：立即回包 / 延迟回包（≤2s）/ 不回包；
- 代理先经控制通道（TCP JSON-lines）把步骤表下发给接收器，接收器确认就绪后，
  代理才开放专用 UDP 演练端口（普通端口始终照常收发，不消耗演练步骤）；
- 一轮只接纳首个测试终端，其他终端的演练报文一律丢弃；
- 接收器按实收顺序执行步骤，同文重发也占用下一步；步骤用完后不再接纳演练报文，
  已安排的延迟回包仍可在本轮结束前发出；
- 手动停止或端口开放满 10 秒：立刻结束演练转发并作废接收器侧全部待发回包；
  无法联系接收器确认停止时，网页提示“停止未确认”；
- 每轮使用独立的上游套接字且端口回收后进入冷却，旧回包不可能进入下一轮；
- 首页内置模拟终端，可连发真实 UDP 报文，并展示接收器逐步实收原文/动作、
  按终端实收顺序排列的回包原文。
"""

import asyncio
import json
import os
import socket
import sqlite3
import threading
import time
import uuid

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
UDP_LISTEN_HOST = os.environ.get("UDP_LISTEN_HOST", "0.0.0.0")
UDP_LISTEN_PORT = int(os.environ.get("UDP_LISTEN_PORT", "9000"))
DRILL_UDP_HOST = os.environ.get("DRILL_UDP_HOST", "0.0.0.0")
DRILL_UDP_PORT = int(os.environ.get("DRILL_UDP_PORT", "9100"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.db")
)
SESSION_IDLE_TIMEOUT = float(os.environ.get("SESSION_IDLE_TIMEOUT", "5"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))

# ---- 演练配置 ----
DRILL_MAX_STEPS = int(os.environ.get("DRILL_MAX_STEPS", "8"))
DRILL_MIN_STEPS = 2
DRILL_MAX_DELAY = float(os.environ.get("DRILL_MAX_DELAY", "2.0"))
DRILL_MAX_DURATION = float(os.environ.get("DRILL_MAX_DURATION", "10"))  # 端口开放上限
DRILL_STOP_TIMEOUT = float(os.environ.get("DRILL_STOP_TIMEOUT", "2"))   # 等停止确认
DRILL_ARM_TIMEOUT = float(os.environ.get("DRILL_ARM_TIMEOUT", "3"))
# 接收器侧武装 TTL：代理异常时接收器自行解除武装，应略大于本轮最长寿命
DRILL_RECEIVER_TTL = float(os.environ.get("DRILL_RECEIVER_TTL", "15"))
DRILL_UPSTREAM_PORT_MIN = int(os.environ.get("DRILL_UPSTREAM_PORT_MIN", "30000"))
DRILL_UPSTREAM_PORT_MAX = int(os.environ.get("DRILL_UPSTREAM_PORT_MAX", "34999"))
DRILL_HISTORY = int(os.environ.get("DRILL_HISTORY", "10"))

RECEIVERS = {
    "A": (
        os.environ.get("RECEIVER_A_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_A_PORT", "9001")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_B_PORT", "9002")),
    ),
}
# 接收器演练 UDP 端口与控制 TCP 端口
RECEIVER_DRILL = {
    "A": (
        os.environ.get("RECEIVER_A_DRILL_HOST", RECEIVERS["A"][0]),
        int(os.environ.get("RECEIVER_A_DRILL_PORT", "9101")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_DRILL_HOST", RECEIVERS["B"][0]),
        int(os.environ.get("RECEIVER_B_DRILL_PORT", "9101")),
    ),
}
RECEIVER_CONTROL = {
    "A": (
        os.environ.get("RECEIVER_A_CONTROL_HOST", RECEIVERS["A"][0]),
        int(os.environ.get("RECEIVER_A_CONTROL_PORT", "9201")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_CONTROL_HOST", RECEIVERS["B"][0]),
        int(os.environ.get("RECEIVER_B_CONTROL_PORT", "9201")),
    ),
}
DEFAULT_TARGET = os.environ.get("DEFAULT_TARGET", "A")

ACTION_LABELS = {"immediate": "立即回包", "delay": "延迟回包", "drop": "不回包"}
DROP_REASON_LABELS = {
    "terminal-locked": "非本轮终端，丢弃",
    "steps-exhausted": "步骤已用完，丢弃",
}


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


def preview(data: bytes) -> str:
    """报文内容展示：可打印文本原样显示，否则用 hex。"""
    try:
        text = data.decode("utf-8")
        if all(ch.isprintable() or ch in "\r\n\t" for ch in text):
            return text if len(text) <= 200 else text[:200] + "…"
    except UnicodeDecodeError:
        pass
    hexed = data.hex()
    return "hex:" + (hexed if len(hexed) <= 200 else hexed[:200] + "…")


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ---------------- SQLite：目标持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)落库，重启后保留。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
        )
        self._conn.commit()
        row = self._conn.execute("SELECT value FROM settings WHERE key='target'").fetchone()
        if row and row[0] in RECEIVERS:
            self._target = row[0]
        else:
            self._target = DEFAULT_TARGET if DEFAULT_TARGET in RECEIVERS else sorted(RECEIVERS)[0]
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (self._target,)
            )
            self._conn.commit()
        self._inserts = 0

    def get_target(self):
        with self._lock:
            return self._target

    def set_target(self, target):
        """先持久化，成功后才切换内存中的当前目标；失败则抛异常并保留原目标。"""
        if target not in RECEIVERS:
            raise ValueError(f"未知接收器: {target!r}")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (target,)
            )
            self._conn.commit()
            self._target = target

    def log(self, direction, client="", session_id=0, receiver="", payload="", note=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log(ts,client,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (time.time(), client, session_id, direction, receiver, payload, note),
                )
                self._conn.commit()
                self._inserts += 1
                if self._inserts % 200 == 0:
                    self._conn.execute(
                        "DELETE FROM message_log WHERE id < "
                        "(SELECT COALESCE(MIN(id),0) FROM "
                        "(SELECT id FROM message_log ORDER BY id DESC LIMIT ?))",
                        (LOG_KEEP,),
                    )
                    self._conn.commit()
        except sqlite3.Error:
            pass  # 日志失败绝不影响转发

    def recent_messages(self, limit=100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,ts,client,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        cols = ("id", "ts", "client", "session_id", "direction", "receiver", "payload", "note")
        return [dict(zip(cols, r)) for r in rows]

    def latest_receiver_for_client(self, client):
        with self._lock:
            row = self._conn.execute(
                "SELECT receiver FROM message_log WHERE client=? AND direction='c2r' "
                "ORDER BY id DESC LIMIT 1",
                (client,),
            ).fetchone()
        return row[0] if row else None


# ---------------- 上游端口分配（带冷却期） ----------------
class PortAllocator:
    """显式管理上游本地端口：关闭后冷却期内不再分配，
    确保旧会话/旧演练迟到的回包不会被复用同端口的新会话/新一轮收到。"""

    def __init__(self, low, high, cooldown):
        self.low, self.high, self.cooldown = low, high, cooldown
        self._next = low
        self._in_use = set()
        self._closed = {}  # port -> 关闭时刻(monotonic)
        self._lock = threading.Lock()

    def allocate(self):
        with self._lock:
            now = time.monotonic()
            for _ in range(self.high - self.low + 1):
                port = self._next
                self._next += 1
                if self._next > self.high:
                    self._next = self.low
                if port in self._in_use:
                    continue
                closed_at = self._closed.get(port)
                if closed_at is not None and now - closed_at < self.cooldown:
                    continue
                self._in_use.add(port)
                self._closed.pop(port, None)
                return port
        raise RuntimeError("上游 UDP 端口耗尽")

    def release(self, port):
        with self._lock:
            self._in_use.discard(port)
            self._closed[port] = time.monotonic()

    def discard(self, port):
        """分配后绑定失败：不进入冷却，直接可用。"""
        with self._lock:
            self._in_use.discard(port)


# ---------------- 普通透传：会话 ----------------
class Session:
    __slots__ = (
        "id", "client_addr", "receiver_name", "receiver_addr",
        "upstream_port", "transport", "state", "closed",
        "created", "last_activity", "pending",
    )

    def __init__(self, sid, client_addr):
        self.id = sid
        self.client_addr = client_addr
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.transport = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []


class ClientProtocol(asyncio.DatagramProtocol):
    """面向遥测终端的普通 UDP 端。"""

    def __init__(self, state):
        self.state = state

    def datagram_received(self, data, addr):
        self.state.on_client_datagram(data, addr)

    def error_received(self, exc):
        pass


class UpstreamProtocol(asyncio.DatagramProtocol):
    """每个会话一条、已 connect 到其接收器的上游 UDP 端。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state):
        self.state = state
        self.session = None

    def datagram_received(self, data, addr):
        sess = self.session
        if sess is not None:
            self.state.on_upstream_datagram(sess, data)

    def error_received(self, exc):
        pass  # 如对端 ICMP 不可达，忽略


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)

    async def start(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}",
              flush=True)

    # ---- 客户端方向 ----
    def on_client_datagram(self, data, addr):
        now = time.monotonic()
        with self.lock:
            sess = self.sessions.get(addr)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：固定到“当前”目标接收器
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                self.sessions[addr] = sess
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    async def _open_session(self, sess):
        target = self.store.get_target()  # 建会话时固定接收器
        host, port = RECEIVERS[target]
        client = fmt_addr(sess.client_addr)
        try:
            transport = None
            upstream_port = None
            last_err = None
            for _ in range(8):
                upstream_port = self.ports.allocate()
                try:
                    transport, proto = await self.loop.create_datagram_endpoint(
                        lambda: UpstreamProtocol(self),
                        local_addr=("0.0.0.0", upstream_port),
                        remote_addr=(host, port),  # connect：只收该接收器的回包
                    )
                    break
                except OSError as exc:
                    last_err = exc
                    self.ports.discard(upstream_port)
            if transport is None:
                raise last_err or RuntimeError("无可用上游端口")
            with self.lock:
                if sess.closed:
                    transport.close()
                    self.ports.release(upstream_port)
                    return
                sess.receiver_name = target
                sess.receiver_addr = (host, port)
                sess.upstream_port = upstream_port
                sess.transport = transport
                sess.state = "open"
                proto.session = sess
                pending, sess.pending = sess.pending, []
            self.store.log("event", client=client, session_id=sess.id, receiver=target,
                           note=f"会话建立，固定到接收器 {target}（上游端口 {upstream_port}）")
            for data in pending:
                self._forward(sess, data)
        except Exception as exc:
            self.store.log("event", client=client, session_id=sess.id,
                           note=f"会话建立失败: {exc}")
            self.close_session(sess, f"open-failed: {exc}")

    def _forward(self, sess, data):
        with self.lock:
            if sess.closed or sess.transport is None:
                return
            sess.last_activity = time.monotonic()
        try:
            sess.transport.sendto(data)  # 已 connect，原样发往该会话的接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           note=f"转发失败: {exc}")
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data):
        now = time.monotonic()
        with self.lock:
            if sess.closed or self.sessions.get(sess.client_addr) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
        self.client_transport.sendto(data, sess.client_addr)  # 原样送回该客户端
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
        transport, sess.transport = sess.transport, None
        port, sess.upstream_port = sess.upstream_port, None
        if transport is not None:
            transport.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, note=f"会话关闭（{reason}）")

    def close_session(self, sess, reason):
        with self.lock:
            self._close_session(sess, reason)

    async def _sweeper(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            with self.lock:
                expired = [s for s in self.sessions.values()
                           if not s.closed and now - s.last_activity > SESSION_IDLE_TIMEOUT]
                for sess in expired:
                    self._close_session(sess, "idle-timeout")

    def snapshot_sessions(self):
        now = time.monotonic()
        with self.lock:
            return [
                {
                    "id": s.id,
                    "client": fmt_addr(s.client_addr),
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
            ]


# ---------------- 演练编排 ----------------
class DrillRound:
    """一轮演练的全部运行态（仅在 asyncio 事件循环线程内流转，
    模拟终端的回包收集线程只追加 sim_replies）。"""

    def __init__(self, round_id, receiver, steps):
        self.round_id = round_id
        self.receiver = receiver
        self.steps = steps                  # [{"action":..., "delay":...}]
        self.state = "arming"               # arming -> running -> ended
        self.end_reason = ""
        self.stop_acked = None              # None=未停止；True/False=接收器是否确认
        self.created_wall = time.time()
        self.started_wall = None
        self.ended_wall = None
        self.started_mono = None
        self.terminal = None                # 本轮唯一测试终端 addr
        self.recv_count = 0                 # 代理已转发给接收器的演练报文数
        self.transport = None               # 面向终端的演练 UDP transport
        self.upstream_transport = None
        self.upstream_port = None
        self.ctrl_reader = None
        self.ctrl_writer = None
        self.ctrl_task = None
        self.end_handle = None
        self.events = []                    # 全过程事件流
        self.recvs = []                     # 接收器逐步实收（来自接收器事件）
        self.replies = []                   # 接收器发出的回包（按接收器顺序）
        self.sim_replies = []               # 模拟终端实收回包（终端实收顺序）
        self.sim_terminal = None            # 模拟终端地址
        self.ctrl_lost = False

    def add_event(self, kind, **fields):
        self.events.append({"ts": time.time(), "kind": kind, **fields})

    def snapshot(self):
        now = time.monotonic()
        age = round(now - self.started_mono, 1) if self.started_mono else 0
        remain = max(0.0, round(DRILL_MAX_DURATION - age, 1)) if self.state == "running" else 0
        return {
            "round_id": self.round_id,
            "receiver": self.receiver,
            "state": self.state,
            "end_reason": self.end_reason,
            "stop_acked": self.stop_acked,
            "steps": [
                {"action": s["action"], "action_label": ACTION_LABELS[s["action"]],
                 "delay": s["delay"]}
                for s in self.steps
            ],
            "terminal": fmt_addr(self.terminal) if self.terminal else "",
            "sim_terminal": self.sim_terminal or "",
            "recv_count": self.recv_count,
            "remaining_steps": max(0, len(self.steps) - self.recv_count),
            "age": age,
            "remain_seconds": remain,
            "max_duration": DRILL_MAX_DURATION,
            "max_delay": DRILL_MAX_DELAY,
            "udp_port": DRILL_UDP_PORT if self.state == "running" else None,
            "ctrl_lost": self.ctrl_lost,
            "started": fmt_ts(self.started_wall) if self.started_wall else "",
            "ended": fmt_ts(self.ended_wall) if self.ended_wall else "",
            "events": [dict(e, tss=fmt_ts(e["ts"])) for e in self.events],
            "recvs": list(self.recvs),
            "replies": list(self.replies),
            "sim_replies": list(self.sim_replies),
        }


class DrillClientProtocol(asyncio.DatagramProtocol):
    """专用 UDP 演练端口：一轮一个 endpoint，轮结束即关闭。"""

    def __init__(self, manager):
        self.manager = manager

    def datagram_received(self, data, addr):
        self.manager.on_drill_datagram(data, addr)

    def error_received(self, exc):
        pass


class DrillUpstreamProtocol(asyncio.DatagramProtocol):
    """每轮一条、已 connect 到接收器演练端口的上游端。"""

    def __init__(self, manager):
        self.manager = manager

    def datagram_received(self, data, addr):
        self.manager.on_drill_upstream(data)

    def error_received(self, exc):
        pass


class DrillManager:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.round = None
        self.history = []  # 已结束轮次快照
        self.ports = PortAllocator(DRILL_UPSTREAM_PORT_MIN,
                                   DRILL_UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)

    # ---------- 对外（Flask 线程） ----------
    def start_round(self, receiver, steps):
        """同步启动一轮：接收器确认就绪后才开放演练 UDP 端口。"""
        if receiver not in RECEIVERS:
            raise ValueError("接收器必须是 A 或 B")
        norm, err = self._validate_steps(steps)
        if err:
            raise ValueError(err)
        if self.round is not None and self.round.state != "ended":
            raise RuntimeError("已有演练在进行中，一轮结束后才能开始下一轮")
        rnd = DrillRound(uuid.uuid4().hex[:12], receiver, norm)
        self.round = rnd
        fut = asyncio.run_coroutine_threadsafe(self._start(rnd), self.loop)
        try:
            fut.result(timeout=DRILL_ARM_TIMEOUT + 3)
        except Exception:
            # 启动失败也要保证状态收敛（清理可能已建立的套接字/控制连接）
            try:
                asyncio.run_coroutine_threadsafe(
                    self._finish(rnd, "arm-failed"), self.loop).result(5)
            except Exception as cleanup_exc:
                print(f"[drill] 武装失败清理异常: {cleanup_exc!r}", flush=True)
            raise
        return rnd.snapshot()

    def stop_round(self):
        """手动停止；返回接收器是否确认（无法联系时返回 False，页面提示）。"""
        rnd = self.round
        if rnd is None or rnd.state == "ended":
            return {"ok": True, "acked": None, "note": "当前没有进行中的演练"}
        fut = asyncio.run_coroutine_threadsafe(
            self._finish(rnd, "manual-stop"), self.loop)
        try:
            fut.result(timeout=DRILL_STOP_TIMEOUT + 4)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            # 收尾异常也要让轮次状态收敛为已结束
            rnd.state = "ended"
            rnd.end_reason = rnd.end_reason or "manual-stop"
            if rnd.stop_acked is None:
                rnd.stop_acked = False
            raise RuntimeError(f"停止流程异常: {exc!r}") from exc
        return {"ok": True, "acked": rnd.stop_acked, "round_id": rnd.round_id}

    def snapshot(self):
        cur = self.round.snapshot() if self.round else None
        return {
            "current": cur,
            "history": list(self.history),
            "udp_port": DRILL_UDP_PORT,
            "max_duration": DRILL_MAX_DURATION,
            "max_steps": DRILL_MAX_STEPS,
            "min_steps": DRILL_MIN_STEPS,
            "max_delay": DRILL_MAX_DELAY,
            "receivers": {k: {"drill": f"{h}:{p}",
                              "control": f"{RECEIVER_CONTROL[k][0]}:{RECEIVER_CONTROL[k][1]}"}
                          for k, (h, p) in RECEIVER_DRILL.items()},
        }

    @staticmethod
    def _validate_steps(steps):
        if not isinstance(steps, list) or not (DRILL_MIN_STEPS <= len(steps) <= DRILL_MAX_STEPS):
            return None, f"动作步数必须在 {DRILL_MIN_STEPS}~{DRILL_MAX_STEPS} 之间"
        norm = []
        for i, s in enumerate(steps):
            if not isinstance(s, dict) or s.get("action") not in ACTION_LABELS:
                return None, f"第 {i + 1} 步动作非法"
            action = s["action"]
            delay = 0.0
            if action == "delay":
                try:
                    delay = float(s.get("delay", 0))
                except (TypeError, ValueError):
                    return None, f"第 {i + 1} 步延迟值非法"
                if not (0 < delay <= DRILL_MAX_DELAY):
                    return None, f"第 {i + 1} 步延迟须在 0~{DRILL_MAX_DELAY} 秒之间"
            norm.append({"action": action, "delay": delay})
        return norm, ""

    # ---------- 事件循环线程内 ----------
    async def _start(self, rnd):
        rnd.add_event("round-arming", receiver=rnd.receiver,
                      control=fmt_addr(RECEIVER_CONTROL[rnd.receiver]))
        ctrl_host, ctrl_port = RECEIVER_CONTROL[rnd.receiver]
        drill_host, drill_port = RECEIVER_DRILL[rnd.receiver]

        # 1) 控制通道下发 ARM，等待接收器确认就绪
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ctrl_host, ctrl_port), timeout=DRILL_ARM_TIMEOUT)
        rnd.ctrl_reader, rnd.ctrl_writer = reader, writer
        arm = {"cmd": "arm", "round_id": rnd.round_id, "steps": rnd.steps,
               "ttl": DRILL_RECEIVER_TTL}
        writer.write((json.dumps(arm, ensure_ascii=False) + "\n").encode())
        await writer.drain()
        ack_line = await asyncio.wait_for(reader.readline(), timeout=DRILL_ARM_TIMEOUT)
        ack = json.loads(ack_line.decode("utf-8"))
        if not ack.get("ok"):
            raise RuntimeError(f"接收器拒绝武装: {ack.get('error')}")
        rnd.add_event("receiver-ready", note=f"接收器 {rnd.receiver} 确认就绪")

        # 2) 每轮独立的上游套接字（独立本地端口，回收后进冷却，旧回包进不了下一轮）
        upstream_port = self.ports.allocate()
        try:
            up_transport, _ = await self.loop.create_datagram_endpoint(
                lambda: DrillUpstreamProtocol(self),
                local_addr=("0.0.0.0", upstream_port),
                remote_addr=(drill_host, drill_port))
        except Exception:
            self.ports.discard(upstream_port)
            raise
        rnd.upstream_transport = up_transport
        rnd.upstream_port = upstream_port

        # 3) 接收器确认就绪后，才开放专用 UDP 演练端口
        # 预绑 socket 并置 SO_REUSEADDR：轮间快速重绑同端口（兼容 Python 3.11，
        # create_datagram_endpoint 的 reuse_address 参数 3.12 才提供）
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            sock.bind((DRILL_UDP_HOST, DRILL_UDP_PORT))
            transport, _ = await self.loop.create_datagram_endpoint(
                lambda: DrillClientProtocol(self), sock=sock)
        except Exception:
            up_transport.close()
            self.ports.release(upstream_port)
            raise
        rnd.transport = transport

        rnd.state = "running"
        rnd.started_wall = time.time()
        rnd.started_mono = time.monotonic()
        rnd.add_event("drill-port-open", port=DRILL_UDP_PORT,
                      note=f"专用演练端口 {DRILL_UDP_PORT} 已开放，"
                           f"满 {DRILL_MAX_DURATION:g}s 自动结束")
        rnd.end_handle = self.loop.call_later(
            DRILL_MAX_DURATION, self._auto_end, rnd)
        rnd.ctrl_task = self.loop.create_task(self._control_loop(rnd))
        print(f"[drill] 一轮开始 round={rnd.round_id} 接收器={rnd.receiver} "
              f"步骤={len(rnd.steps)} 演练端口={DRILL_UDP_PORT}", flush=True)

    def _auto_end(self, rnd):
        if rnd.state != "ended":
            self.loop.create_task(self._finish(rnd, "time-limit"))

    async def _control_loop(self, rnd):
        """读取接收器在演练期间推送的 JSON-lines 事件。"""
        try:
            while True:
                line = await rnd.ctrl_reader.readline()
                if not line:
                    raise EOFError("控制通道关闭")
                msg = json.loads(line.decode("utf-8"))
                if msg.get("type") == "event":
                    self._on_receiver_event(rnd, msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if rnd.state != "ended":
                rnd.ctrl_lost = True
                rnd.add_event("control-lost", note=str(exc))
                print(f"[drill] round={rnd.round_id} 控制通道丢失: {exc}", flush=True)

    def _on_receiver_event(self, rnd, msg):
        ev = msg.get("event")
        seq = msg.get("seq")
        if ev == "recv":
            rnd.recvs.append({
                "seq": seq, "step": seq + 1,
                "preview": msg.get("preview", ""),
                "action": "", "action_label": "", "delay": 0.0,
            })
        elif ev == "action":
            # 与 recv 同 seq，合并进逐步实收表
            for item in reversed(rnd.recvs):
                if item["seq"] == seq:
                    item["action"] = msg.get("action", "")
                    item["action_label"] = ACTION_LABELS.get(msg.get("action"), "")
                    item["delay"] = msg.get("delay", 0.0)
                    break
            rnd.add_event("receiver-action", seq=seq, action=msg.get("action"),
                          action_label=ACTION_LABELS.get(msg.get("action"), ""),
                          delay=msg.get("delay", 0.0), preview=msg.get("preview", ""))
            return
        elif ev == "reply":
            rnd.replies.append({"seq": seq, "step": (seq + 1 if seq is not None else None),
                                "preview": msg.get("preview", ""),
                                "ts": fmt_ts(time.time())})
        elif ev == "drop":
            rnd.add_event("receiver-drop", seq=seq,
                          reason=msg.get("reason", ""),
                          reason_label=DROP_REASON_LABELS.get(msg.get("reason", ""),
                                                             msg.get("reason", "")),
                          preview=msg.get("preview", ""),
                          terminal=msg.get("terminal", ""))
            return
        elif ev == "cancelled":
            rnd.add_event("receiver-cancelled",
                          cancelled_seqs=msg.get("cancelled_seqs", []),
                          reason=msg.get("reason", ""))
            return
        elif ev == "terminal-locked":
            rnd.add_event("receiver-terminal-locked", terminal=msg.get("terminal", ""))
            return
        # recv/reply 也进事件流
        rnd.add_event("receiver-" + ev, seq=seq, preview=msg.get("preview", ""))

    # ---- 演练 UDP：终端 -> 接收器 ----
    def on_drill_datagram(self, data, addr):
        rnd = self.round
        if rnd is None or rnd.state != "running" or rnd.transport is None:
            return  # 端口未开放/已关闭：报文无处可来，双保险
        # 一轮只接纳首个测试终端
        if rnd.terminal is None:
            rnd.terminal = addr
            rnd.add_event("terminal-locked", terminal=fmt_addr(addr))
            print(f"[drill] round={rnd.round_id} 锁定首个测试终端 {fmt_addr(addr)}",
                  flush=True)
        elif rnd.terminal != addr:
            rnd.add_event("drill-drop", reason="terminal-locked",
                          reason_label=DROP_REASON_LABELS["terminal-locked"],
                          terminal=fmt_addr(addr), preview=preview(data))
            return
        # 步骤用完后不再接纳演练报文（已安排的回包仍可发出）
        if rnd.recv_count >= len(rnd.steps):
            rnd.add_event("drill-drop", reason="steps-exhausted",
                          reason_label=DROP_REASON_LABELS["steps-exhausted"],
                          preview=preview(data))
            return
        rnd.recv_count += 1
        try:
            rnd.upstream_transport.sendto(data)  # 原样转发到接收器演练端口
        except Exception as exc:
            rnd.add_event("drill-drop", reason="forward-error",
                          reason_label=f"转发失败: {exc}", preview=preview(data))
            return
        rnd.add_event("forwarded", seq=rnd.recv_count - 1, preview=preview(data))

    # ---- 演练 UDP：接收器回包 -> 终端 ----
    def on_drill_upstream(self, data):
        rnd = self.round
        if rnd is None or rnd.state != "running":
            return  # 轮已结束：上游套接字已关闭，旧回包到不了这里
        if rnd.terminal is None:
            rnd.add_event("drill-drop", reason="no-terminal",
                          reason_label="终端尚未接入，回包丢弃", preview=preview(data))
            return
        try:
            rnd.transport.sendto(data, rnd.terminal)  # 原样送回本轮唯一终端
        except Exception as exc:
            rnd.add_event("drill-drop", reason="reply-error",
                          reason_label=f"回包发送失败: {exc}", preview=preview(data))
            return
        rnd.add_event("reply-to-terminal", preview=preview(data))

    # ---- 结束：立即停转发 + 作废待发回包 ----
    async def _finish(self, rnd, reason):
        if rnd.state == "ended":
            return
        rnd.state = "ended"
        rnd.end_reason = reason
        rnd.ended_wall = time.time()
        if rnd.end_handle is not None:
            rnd.end_handle.cancel()
            rnd.end_handle = None

        # 立刻结束演练转发：关闭终端侧端口与上游套接字
        try:
            if rnd.transport is not None:
                rnd.transport.close()
        except Exception as exc:
            print(f"[drill] round={rnd.round_id} 关闭演练端口异常: {exc}",
                  flush=True)
        finally:
            rnd.transport = None
        try:
            if rnd.upstream_transport is not None:
                rnd.upstream_transport.close()
        except Exception as exc:
            print(f"[drill] round={rnd.round_id} 关闭上游套接字异常: {exc}",
                  flush=True)
        finally:
            rnd.upstream_transport = None
        if rnd.upstream_port is not None:
            self.ports.release(rnd.upstream_port)  # 冷却期内不再分配
            rnd.upstream_port = None

        # 通知接收器作废全部待发回包，并等待确认；无法联系则 stop_acked=False
        try:
            acked, stop_info = await self._send_stop(rnd)
        except Exception as exc:
            print(f"[drill] round={rnd.round_id} 停止流程异常: {exc!r}",
                  flush=True)
            acked, stop_info = False, {}
        rnd.stop_acked = acked
        seqs = stop_info.get("cancelled_seqs") or []
        if seqs:
            rnd.add_event("receiver-cancelled", cancelled_seqs=seqs,
                          reason="proxy-stop")
        rnd.add_event("round-ended", reason=reason, stop_acked=acked,
                      note=("接收器已确认停止，待发回包作废" if acked
                            else "停止未确认：无法联系接收器，请人工核对接收器状态"))

        if rnd.ctrl_task is not None:
            rnd.ctrl_task.cancel()
            try:
                await asyncio.wait_for(rnd.ctrl_task, timeout=1)
            except BaseException:
                pass
            rnd.ctrl_task = None
        if rnd.ctrl_writer is not None:
            try:
                rnd.ctrl_writer.close()
                await asyncio.wait_for(rnd.ctrl_writer.wait_closed(), timeout=1)
            except BaseException:
                pass
            rnd.ctrl_writer = None
            rnd.ctrl_reader = None

        snap = rnd.snapshot()
        self.history.append(snap)
        if len(self.history) > DRILL_HISTORY:
            self.history = self.history[-DRILL_HISTORY:]
        print(f"[drill] 一轮结束 round={rnd.round_id} 原因={reason} "
              f"停止确认={acked}", flush=True)

    async def _send_stop(self, rnd):
        """停止走独立短连接：事件推送与停止 ack 不共用一条 TCP，
        避免接收器推送的事件行与 STOP 确认行交错导致错读。"""
        ctrl_host, ctrl_port = RECEIVER_CONTROL[rnd.receiver]
        writer = None
        try:
            # 事件读取任务先停掉，再关原事件连接
            if rnd.ctrl_task is not None:
                rnd.ctrl_task.cancel()
                try:
                    await asyncio.wait_for(rnd.ctrl_task, timeout=1)
                except BaseException:
                    pass
                rnd.ctrl_task = None
            if rnd.ctrl_writer is not None:
                try:
                    rnd.ctrl_writer.close()
                    await asyncio.wait_for(rnd.ctrl_writer.wait_closed(),
                                           timeout=1)
                except Exception:
                    pass
                rnd.ctrl_writer = None
                rnd.ctrl_reader = None

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ctrl_host, ctrl_port),
                timeout=DRILL_STOP_TIMEOUT)
            writer.write(
                (json.dumps({"cmd": "stop", "round_id": rnd.round_id})
                 + "\n").encode())
            await asyncio.wait_for(writer.drain(), timeout=DRILL_STOP_TIMEOUT)
            line = await asyncio.wait_for(reader.readline(),
                                          timeout=DRILL_STOP_TIMEOUT)
            if not line:
                return False, {}
            ack = json.loads(line.decode("utf-8"))
            return bool(ack.get("ok")), ack
        except Exception as exc:
            print(f"[drill] round={rnd.round_id} 停止确认失败: {exc}", flush=True)
            return False, {}
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    # ---- 模拟终端（Flask 线程调用，阻塞式 socket + 后台收包线程） ----
    def sim_send(self, payloads, local_port):
        rnd = self.round
        if rnd is None or rnd.state != "running":
            raise RuntimeError("演练未进行中，模拟终端无法发送（请先开始一轮演练）")
        if rnd.terminal is not None:
            raise RuntimeError(f"本轮已接纳测试终端 {fmt_addr(rnd.terminal)}，"
                               "一轮只接纳一个终端")
        if not payloads:
            raise ValueError("至少发送一条报文")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if local_port:
                sock.bind(("0.0.0.0", local_port))
            sock.connect(("127.0.0.1", DRILL_UDP_PORT))
            # 在事件循环线程登记终端，保证“首个终端”判定一致
            addr = sock.getsockname()
            asyncio.run_coroutine_threadsafe(
                self._sim_register(rnd, addr), self.loop).result(2)
            if rnd.terminal != addr:  # 并发下被真实终端抢先
                raise RuntimeError(f"本轮已接纳测试终端 {fmt_addr(rnd.terminal)}，"
                                   "模拟终端无法加入")
            rnd.sim_terminal = fmt_addr(addr)

            reader = threading.Thread(
                target=self._sim_reader, args=(rnd, sock), daemon=True)
            reader.start()

            sent = []
            for item in payloads:
                if rnd.state != "running":
                    raise RuntimeError("演练已结束，后续报文未发出")
                data = item["payload"].encode("utf-8", errors="replace")
                sock.send(data)  # 真实 UDP 报文，走代理演练端口
                sent.append(preview(data))
                gap = item.get("gap", 0.0)
                if gap > 0:
                    time.sleep(min(gap, DRILL_MAX_DURATION))
            return {"sent": sent, "terminal": rnd.sim_terminal}
        except Exception:
            try:
                sock.close()  # 收包线程尚未启动或加入失败：立即释放
            except OSError:
                pass
            raise

    async def _sim_register(self, rnd, addr):
        if rnd.terminal is None:
            rnd.terminal = addr
            rnd.add_event("terminal-locked", terminal=fmt_addr(addr),
                          note="模拟终端接入")
            print(f"[drill] round={rnd.round_id} 锁定模拟终端 {fmt_addr(addr)}",
                  flush=True)

    def _sim_reader(self, rnd, sock):
        """按终端实收顺序收集回包原文，直到本轮结束并短暂排空。"""
        sock.settimeout(0.5)
        drained = 0.0
        while True:
            if rnd.state == "ended":
                # 结束后再排空 0.6s，确认没有迟到回包（正常应一个都收不到）
                drained += 0.5
                if drained >= 0.6:
                    break
            try:
                data = sock.recv(65535)
                rnd.sim_replies.append(
                    {"ts": fmt_ts(time.time()), "preview": preview(data)})
            except socket.timeout:
                continue
            except OSError:
                break
        try:
            sock.close()
        except OSError:
            pass


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
state = ProxyState(store)
drill = DrillManager(store)
app = Flask(__name__)
app.secret_key = os.urandom(16)

_last_test = {}
_last_test_lock = threading.Lock()

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "event": "事件",
    "drop": "丢弃",
}


def _format_message(m):
    m = dict(m)
    m["ts"] = fmt_ts(m["ts"])
    m["direction"] = DIRECTION_LABELS.get(m["direction"], m["direction"])
    return m


@app.get("/")
def index():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return render_template(
        "index.html",
        target=store.get_target(),
        receivers={k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        idle_timeout=SESSION_IDLE_TIMEOUT,
        udp_listen_port=UDP_LISTEN_PORT,
        drill_udp_port=DRILL_UDP_PORT,
        last_test=lt,
    )


@app.get("/api/state")
def api_state():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return jsonify({
        "target": store.get_target(),
        "receivers": {k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        "idle_timeout": SESSION_IDLE_TIMEOUT,
        "sessions": state.snapshot_sessions(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "last_test": lt,
        "drill": drill.snapshot(),
    })


@app.post("/switch")
def switch():
    target = request.form.get("target", "")
    try:
        store.set_target(target)
        store.log("event", note=f"网页切换目标为接收器 {target}")
        flash(f"已切换到接收器 {target}。新会话走 {target}，旧会话继续走原接收器直至过期。", "ok")
    except Exception as exc:
        flash(f"切换保存失败，保留原目标 {store.get_target()}。原因：{exc}", "error")
    return redirect(url_for("index"))


@app.post("/test")
def test():
    global _last_test
    result = run_test(
        payload=request.form.get("payload", "PING"),
        delay_s=request.form.get("delay", "").strip(),
        client_port_s=request.form.get("client_port", "").strip(),
        wait_s=request.form.get("wait", "10").strip(),
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_test(payload, delay_s, client_port_s, wait_s):
    """从网页后端经代理普通端口发一条试传报文并等待回包（不消耗演练步骤）。"""
    try:
        wait = min(max(float(wait_s), 0.5), 60.0)
    except ValueError:
        wait = 10.0
    wire = payload
    try:
        delay = float(delay_s)
        if delay > 0:
            wire = f"DELAY:{delay}:{payload}"  # 接收器识别的延迟回包指令
    except ValueError:
        pass
    data = wire.encode()
    result = {"time": fmt_ts(time.time()), "payload": wire, "ok": False,
              "reply": "", "error": "", "rtt": None, "client": "", "receiver": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if client_port_s:
            sock.bind(("0.0.0.0", int(client_port_s)))
        sock.settimeout(wait)
        sock.connect(("127.0.0.1", UDP_LISTEN_PORT))
        result["client"] = fmt_addr(sock.getsockname())
        t0 = time.monotonic()
        sock.send(data)
        try:
            reply = sock.recv(65535)
            result["ok"] = True
            result["rtt"] = round(time.monotonic() - t0, 3)
            result["reply"] = preview(reply)
        except socket.timeout:
            result["error"] = f"等待回包超时（{wait}s）"
    except Exception as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
    time.sleep(0.05)  # 等代理把日志落库
    if result["client"]:
        result["receiver"] = store.latest_receiver_for_client(result["client"])
    return result


# ---------------- 演练相关 HTTP API（JSON） ----------------
@app.post("/api/drill/start")
def api_drill_start():
    body = request.get_json(silent=True) or {}
    receiver = body.get("receiver")
    steps = body.get("steps")
    try:
        snap = drill.start_round(receiver, steps)
        return jsonify({"ok": True, "round": snap})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"启动失败：{exc}"}), 502


@app.post("/api/drill/stop")
def api_drill_stop():
    try:
        result = drill.stop_round()
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502


@app.post("/api/drill/sim")
def api_drill_sim():
    body = request.get_json(silent=True) or {}
    payloads = body.get("payloads")
    local_port = body.get("local_port")
    if not isinstance(payloads, list) or not payloads:
        return jsonify({"ok": False, "error": "请至少填写一条报文"}), 400
    items = []
    for i, p in enumerate(payloads[:DRILL_MAX_STEPS + 4]):
        if not isinstance(p, dict) or not str(p.get("payload", "")):
            return jsonify({"ok": False, "error": f"第 {i + 1} 条报文为空"}), 400
        try:
            gap = float(p.get("gap", 0) or 0)
        except (TypeError, ValueError):
            gap = 0.0
        items.append({"payload": str(p["payload"])[:1400],
                      "gap": min(max(gap, 0.0), DRILL_MAX_DURATION)})
    port = None
    if local_port:
        try:
            port = int(local_port)
            if not (1 <= port <= 65535):
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "模拟终端本地端口非法"}), 400
    try:
        result = drill.sim_send(items, port)
        return jsonify({"ok": True, **result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 409


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    drill.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    print(f"[proxy] 演练 UDP 端口 {DRILL_UDP_PORT}（仅演练期间开放）", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
