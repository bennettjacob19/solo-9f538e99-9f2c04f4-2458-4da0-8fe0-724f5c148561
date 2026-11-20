#!/usr/bin/env python3
"""本地遥测接收器：普通端口原样回显 + 演练端口按步骤表动作。

两个 UDP 端口：
1. 普通端口 LISTEN_PORT（默认 9001）：原样回显报文，可延迟回包，
   与演练完全无关，不消耗任何演练步骤。
   延迟来源（优先级从高到低）：
   a. 报文以 "DELAY:<秒>:" 开头时，按该秒数延迟（回包仍含完整原始报文）；
   b. 否则用环境变量 REPLY_DELAY（秒，默认 0）。
2. 演练端口 DRILL_PORT（默认 9101）：仅在代理通过控制通道 ARM 之后生效，
   按代理下发的步骤表（2~8 步：immediate/delay/drop）逐条执行，
   同文重发也占用下一步，步骤用完后演练报文一律丢弃；
   一轮只接纳首个测试终端，其他终端的演练报文丢弃。

控制通道 CONTROL_PORT（默认 9201，TCP，JSON 每行一条）：
- {"cmd":"arm","round_id":...,"steps":[{"action":"immediate|delay|drop",
  "delay":秒}],"ttl":秒}  →  {"ok":true} / {"ok":false,"error":...}
- {"cmd":"stop","round_id":...}  取消本轮所有待发回包并解除武装 → ack
- {"cmd":"ping"}  →  {"ok":true}
接收器在演练期间主动推送事件（event 字段：recv/action/reply/drop/
cancelled/auto-clear），代理据此在网页逐步展示实收原文与执行动作。

环境变量：
  RECEIVER_NAME   显示名（默认 receiver）
  LISTEN_PORT     普通 UDP 端口（默认 9001）
  DRILL_PORT      演练 UDP 端口（默认 9101）
  CONTROL_PORT    控制 TCP 端口（默认 9201）
  REPLY_DELAY     普通端口默认回包延迟秒数（默认 0）
  MAX_DELAY       普通端口报文级延迟上限（默认 60）
"""

import asyncio
import base64
import json
import os
import re
import time

NAME = os.environ.get("RECEIVER_NAME", "receiver")
PORT = int(os.environ.get("LISTEN_PORT", "9001"))
DRILL_PORT = int(os.environ.get("DRILL_PORT", "9101"))
CONTROL_PORT = int(os.environ.get("CONTROL_PORT", "9201"))
DEFAULT_DELAY = float(os.environ.get("REPLY_DELAY", "0"))
MAX_DELAY = float(os.environ.get("MAX_DELAY", "60"))
MAX_STEPS = 8
MAX_STEP_DELAY = 2.0

DELAY_RE = re.compile(rb"^DELAY:([0-9]+(?:\.[0-9]+)?):")


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


# ---------------- 普通端口：原样回显 ----------------
class EchoProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        delay = DEFAULT_DELAY
        m = DELAY_RE.match(data)
        if m:
            delay = min(float(m.group(1)), MAX_DELAY)
        print(f"[{NAME}][普通] 收到 {len(data)}B 来自 {addr[0]}:{addr[1]}，"
              f"{delay}s 后回包", flush=True)
        asyncio.get_running_loop().call_later(delay, self._reply, data, addr)

    def _reply(self, data, addr):
        try:
            self.transport.sendto(data, addr)  # 原样回显
            print(f"[{NAME}][普通] 已回包 {len(data)}B 到 {addr[0]}:{addr[1]}",
                  flush=True)
        except Exception as exc:
            print(f"[{NAME}][普通] 回包失败: {exc}", flush=True)


# ---------------- 演练武装状态（一轮） ----------------
class DrillArm:
    """一轮演练在接收器侧的状态：步骤表、终端锁、待发回包句柄。"""

    def __init__(self, round_id, steps, ttl, emit, on_expired, loop):
        self.round_id = round_id
        self.steps = steps                # [{"action": ..., "delay": ...}]
        self.ttl = ttl
        self._emit = emit                 # 事件推送回调
        self._on_expired = on_expired     # TTL 到期回调（清理全局武装）
        self._loop = loop
        self.cursor = 0                   # 下一条实收报文占用的步骤序号
        self.terminal = None              # 本轮唯一接纳的测试终端 addr
        self.pending = {}                 # asyncio.TimerHandle -> step 序号
        self.watchdog = loop.call_later(ttl, self._ttl_expire)
        self.closed = False
        self.transport = None  # 演练 UDP transport，首条报文到达时绑定

    def _now(self):
        return time.time()

    def emit(self, event, **fields):
        payload = {"type": "event", "event": event, "receiver": NAME,
                   "round_id": self.round_id, "ts": self._now(), **fields}
        self._emit(payload)

    # ---- 演练报文到达 ----
    def datagram(self, transport, data, addr):
        if self.closed:
            return
        self.transport = transport
        # 一轮只接纳首个测试终端
        if self.terminal is None:
            self.terminal = addr
            self.emit("terminal-locked", terminal=f"{addr[0]}:{addr[1]}")
        elif self.terminal != addr:
            self.emit("drop", reason="terminal-locked",
                      terminal=f"{addr[0]}:{addr[1]}", preview=preview(data))
            print(f"[{NAME}][演练] 拒绝非本轮终端 {addr[0]}:{addr[1]} 的报文",
                  flush=True)
            return

        seq = self.cursor
        if seq >= len(self.steps):
            # 步骤用完：不再接纳演练报文（已安排的延迟回包不受影响）
            self.emit("drop", reason="steps-exhausted", seq=seq,
                      preview=preview(data))
            print(f"[{NAME}][演练] 步骤已用完，丢弃演练报文 {preview(data)}",
                  flush=True)
            return
        step = self.steps[seq]
        self.cursor = seq + 1
        self.emit("recv", seq=seq, preview=preview(data),
                  payload_b64=base64.b64encode(data).decode())
        action = step["action"]
        self.emit("action", seq=seq, action=action,
                  delay=step.get("delay", 0.0), preview=preview(data))
        print(f"[{NAME}][演练] 第 {seq + 1}/{len(self.steps)} 步实收 "
              f"{preview(data)}，动作 {action}"
              + (f"（延迟 {step['delay']}s）" if action == "delay" else ""),
              flush=True)

        if action == "immediate":
            self._send_reply(seq, data, addr)
        elif action == "delay":
            def _fire(d=data, a=addr, s=seq):
                # call_later 的回调无参；handle 用当前闭包变量登记
                self._delayed_reply(handle, s, d, a)
            handle = self._loop.call_later(float(step["delay"]), _fire)
            self.pending[handle] = seq
        # action == "drop"：不回包

    def _delayed_reply(self, handle, seq, data, addr):
        self.pending.pop(handle, None)
        if self.closed:
            return
        self._send_reply(seq, data, addr)

    def _send_reply(self, seq, data, addr):
        # 回包原文 = 触发该步的实收报文原文（同文重发也按原文回）
        if self.transport is None or self.closed:
            return
        try:
            self.transport.sendto(data, addr)
        except Exception as exc:
            print(f"[{NAME}][演练] 回包失败: {exc}", flush=True)
            return
        self.emit("reply", seq=seq, preview=preview(data),
                  payload_b64=base64.b64encode(data).decode())
        print(f"[{NAME}][演练] 第 {seq + 1} 步回包 {preview(data)} "
              f"→ {addr[0]}:{addr[1]}", flush=True)

    # ---- 停止：作废全部待发回包 ----
    def stop(self, reason="stopped"):
        if self.closed:
            return 0
        self.closed = True
        if self.watchdog is not None:
            self.watchdog.cancel()
            self.watchdog = None
        cancelled = list(self.pending.values())
        for handle in self.pending:
            handle.cancel()
        self.pending.clear()
        self.terminal = None
        self.emit("cancelled", reason=reason, cancelled_seqs=cancelled)
        print(f"[{NAME}][演练] 本轮结束（{reason}），作废待发回包 "
              f"{cancelled}", flush=True)
        return len(cancelled)

    def _ttl_expire(self):
        if self.closed:
            return
        self.stop(reason="ttl-expire")
        self.emit("auto-clear")
        self._on_expired(self)


class DrillProtocol(asyncio.DatagramProtocol):
    """演练 UDP 端：仅在接收器已武装（收到代理 ARM）时处理报文。"""

    def __init__(self, arm_holder):
        self._arm_holder = arm_holder  # callable -> 当前 DrillArm 或 None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        arm = self._arm_holder()
        if arm is None or arm.closed:
            print(f"[{NAME}][演练] 未武装，丢弃来自 "
                  f"{addr[0]}:{addr[1]} 的报文", flush=True)
            return
        arm.datagram(self.transport, data, addr)

    def error_received(self, exc):
        print(f"[{NAME}][演练] UDP 错误: {exc}", flush=True)


# ---------------- 控制通道（TCP, JSON-lines） ----------------
class ControlServer:
    def __init__(self, loop):
        self.loop = loop
        self.arm = None
        self._clients = set()      # 全部控制连接
        self._event_writer = None  # 武装期间接收事件推送的连接（即代理的常驻连接）
        self._current_writer = None  # 正在处理命令的连接

    def _broadcast(self, payload):
        # 事件只推给武装时建立的常驻事件连接；短连接（如 STOP）只收自己的应答，
        # 不会把事件行误读成命令确认
        if self._event_writer is None:
            return
        line = (json.dumps(payload, ensure_ascii=False) + "\n").encode()
        try:
            self._event_writer.write(line)
        except Exception:
            self._event_writer = None

    def _clear_arm(self, arm):
        if self.arm is arm:
            self.arm = None

    def _validate_steps(self, steps):
        if not isinstance(steps, list) or not (2 <= len(steps) <= MAX_STEPS):
            return None, f"steps 必须是 2~{MAX_STEPS} 步的列表"
        norm = []
        for i, s in enumerate(steps):
            if not isinstance(s, dict) or s.get("action") not in (
                    "immediate", "delay", "drop"):
                return None, f"第 {i + 1} 步 action 非法"
            action = s["action"]
            delay = 0.0
            if action == "delay":
                try:
                    delay = float(s.get("delay", 0))
                except (TypeError, ValueError):
                    return None, f"第 {i + 1} 步 delay 非法"
                if not (0 < delay <= MAX_STEP_DELAY):
                    return None, (f"第 {i + 1} 步延迟须在 0~{MAX_STEP_DELAY}"
                                  " 秒之间")
            norm.append({"action": action, "delay": delay})
        return norm, ""

    def handle_command(self, msg):
        cmd = msg.get("cmd") if isinstance(msg, dict) else None
        if cmd == "ping":
            return {"ok": True}
        if cmd == "arm":
            steps, err = self._validate_steps(msg.get("steps"))
            if err:
                return {"ok": False, "error": err}
            round_id = str(msg.get("round_id", ""))
            if not round_id:
                return {"ok": False, "error": "round_id 为空"}
            if self.arm is not None and not self.arm.closed:
                # 接收器一次只服务一轮：先解除旧武装
                self.arm.stop(reason="replaced")
            try:
                ttl = float(msg.get("ttl", 15))
            except (TypeError, ValueError):
                ttl = 15.0
            ttl = max(ttl, 1.0)
            self.arm = DrillArm(round_id, steps, ttl, self._broadcast,
                                self._clear_arm, self.loop)
            # ARM 连接即本轮事件推送连接（代理在该连接上常驻读取事件）
            self._event_writer = self._current_writer
            print(f"[{NAME}][控制] 武装就绪 round={round_id}，"
                  f"{len(steps)} 步，TTL {ttl}s", flush=True)
            return {"ok": True, "round_id": round_id, "steps": len(steps)}
        if cmd == "stop":
            round_id = str(msg.get("round_id", ""))
            if self.arm is None or self.arm.closed:
                return {"ok": True, "round_id": round_id, "cancelled": 0,
                        "cancelled_seqs": [], "note": "无在途演练"}
            if self.arm.round_id != round_id:
                return {"ok": False, "error": "round_id 不匹配"}
            cancelled_seqs = sorted(self.arm.pending.values())
            cancelled = self.arm.stop(reason="proxy-stop")
            self.arm = None
            # 事件连接可能已关闭：作废结果随 ack 一并返回，代理据此补记事件
            return {"ok": True, "round_id": round_id, "cancelled": cancelled,
                    "cancelled_seqs": cancelled_seqs}
        return {"ok": False, "error": f"未知命令: {cmd!r}"}

    async def serve_client(self, reader, writer):
        self._clients.add(writer)
        prev_writer = self._current_writer
        self._current_writer = writer
        peer = writer.get_extra_info("peername")
        print(f"[{NAME}][控制] 连接建立 {peer}", flush=True)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode("utf-8"))
                    resp = self.handle_command(msg)
                except json.JSONDecodeError as exc:
                    resp = {"ok": False, "error": f"JSON 解析失败: {exc}"}
                except Exception as exc:  # 控制面异常绝不能打崩接收器
                    resp = {"ok": False, "error": str(exc)}
                try:
                    # 命令应答只写回当前连接，绝不广播
                    writer.write((json.dumps(resp, ensure_ascii=False)
                                  + "\n").encode())
                    await writer.drain()
                except Exception:
                    break
        finally:
            self._clients.discard(writer)
            if self._event_writer is writer:
                self._event_writer = None
            self._current_writer = prev_writer
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            print(f"[{NAME}][控制] 连接断开 {peer}", flush=True)

    def get_arm(self):
        return self.arm


async def main():
    loop = asyncio.get_running_loop()

    # 普通 UDP 端口
    await loop.create_datagram_endpoint(
        EchoProtocol, local_addr=("0.0.0.0", PORT))
    print(f"[{NAME}] 普通 UDP 监听 0.0.0.0:{PORT}，"
          f"默认延迟 {DEFAULT_DELAY}s", flush=True)

    # 演练 UDP 端口（武装前静默丢弃报文）
    control = ControlServer(loop)
    await loop.create_datagram_endpoint(
        lambda: DrillProtocol(control.get_arm),
        local_addr=("0.0.0.0", DRILL_PORT))
    print(f"[{NAME}] 演练 UDP 监听 0.0.0.0:{DRILL_PORT}（武装前静默）",
          flush=True)

    # 控制 TCP 端口
    srv = await asyncio.start_server(
        control.serve_client, "0.0.0.0", CONTROL_PORT)
    print(f"[{NAME}] 控制通道监听 0.0.0.0:{CONTROL_PORT}", flush=True)

    async with srv:
        await srv.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
