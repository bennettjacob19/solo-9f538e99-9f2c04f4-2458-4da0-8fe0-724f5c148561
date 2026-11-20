#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器和代理，
逐项验证需求行为（普通透传 + 固件联调演练）。用法：python3 scripts/verify_local.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

# 无 Flask 的精简环境：注入随仓库附带的最小 Flask 垫片（仅测试用，不参与镜像构建）
try:
    import flask  # noqa: F401
except ImportError:
    shim = os.path.join(ROOT, "testlibs", "flask_shim")
    if os.path.isdir(shim):
        os.environ["PYTHONPATH"] = (
            shim + os.pathsep + os.environ.get("PYTHONPATH", "")).rstrip(os.pathsep)
        sys.path.insert(0, shim)

UDP_PORT = 19000
DRILL_UDP_PORT = 19100
HTTP_PORT = 18080
BASE = f"http://127.0.0.1:{HTTP_PORT}"

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_get(path):
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.load(r)


def http_post(path, data):
    with urllib.request.urlopen(
        BASE + path, data=urllib.parse.urlencode(data).encode(), timeout=40
    ) as r:
        return r.status


def http_json(path, body, expect_error=False):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def wait_http_up(timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            http_get("/api/state")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_drill_state(goal, timeout=12):
    t0 = time.time()
    st = None
    while time.time() - t0 < timeout:
        st = http_get("/api/state")["drill"]
        cur = st["current"]
        if cur and cur["state"] == goal:
            return st
        time.sleep(0.15)
    return st


def wait_round_end(timeout=12):
    return wait_drill_state("ended", timeout)


def udp_client(port=UDP_PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("127.0.0.1", port))
    return s


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def recv_available(sock, duration):
    """在 duration 内尽量收包，返回收到的字节串列表（用于断言“没有回包”）。
    Linux 下端口未监听时 connected UDP 会收到 ICMP 端口不可达 →
    ConnectionRefusedError，同样视为“没有回包”（且证明端口未开放）。"""
    got = []
    t0 = time.time()
    sock.settimeout(0.2)
    while time.time() - t0 < duration:
        try:
            got.append(sock.recv(65535))
        except socket.timeout:
            pass
        except ConnectionRefusedError:
            pass
    return got


def log_entries(client=None, direction=None, text=None):
    msgs = http_get("/api/state")["messages"]
    out = []
    for m in msgs:
        if client and m["client"] != client:
            continue
        if direction and m["direction"] != direction:
            continue
        if text and text not in m["payload"] and text not in m["note"]:
            continue
        out.append(m)
    return out


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port),
               DRILL_PORT=str(19101 if name == "A" else 19102),
               CONTROL_PORT=str(19201 if name == "A" else 19202),
               REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), DRILL_UDP_PORT=str(DRILL_UDP_PORT),
        HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_A_DRILL_PORT="19101", RECEIVER_A_CONTROL_PORT="19201",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        RECEIVER_B_DRILL_PORT="19102", RECEIVER_B_CONTROL_PORT="19202",
        SESSION_IDLE_TIMEOUT="5", DB_PATH=db_path,
    )
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_drill(receiver, steps):
    code, body = http_json("/api/drill/start",
                           {"receiver": receiver, "steps": steps})
    return code, body


def sim_send(payloads, local_port=None):
    return http_json("/api/drill/sim",
                     {"payloads": payloads, "local_port": local_port})


# ======================================================================
# 演练部分验证
# ======================================================================
def verify_drills(procs, tmp):
    # D0. 未开始演练时，演练端口不可达（无 socket 监听 → 发了也没有任何回包）
    c0 = udp_client(DRILL_UDP_PORT)
    c0.send(b"before-any-drill")
    check("演练未开始：演练端口不接纳报文", recv_available(c0, 1.0) == [])
    c0.close()

    # D1. 参数校验：步数 2~8、延迟 ≤2s
    code, body = start_drill("A", [{"action": "immediate"}])
    check("拒绝 1 步编排", code == 400 and not body.get("ok"), str(body))
    code, body = start_drill("A", [{"action": "immediate"}] * 9)
    check("拒绝 9 步编排", code == 400 and not body.get("ok"), str(body))
    code, body = start_drill("A", [
        {"action": "delay", "delay": 3.0}, {"action": "drop"}])
    check("拒绝超过 2 秒的延迟步骤", code == 400 and not body.get("ok"), str(body))
    code, body = start_drill("C", [{"action": "drop"}, {"action": "immediate"}])
    check("拒绝不存在的接收器", code == 400 and not body.get("ok"), str(body))

    # D2. 核心故障复现：首条不回包，重传（同文）占下一步并立即回包
    code, body = start_drill("A", [
        {"action": "drop", "delay": 0},
        {"action": "immediate", "delay": 0}])
    check("演练启动成功", code == 200 and body.get("ok"), str(body))
    st = wait_drill_state("running")
    cur = st["current"]
    check("接收器确认就绪后才开放演练端口",
          cur and cur["state"] == "running" and cur["udp_port"] == DRILL_UDP_PORT,
          str(cur))
    check("演练事件含 receiver-ready 与 port-open",
          any(e["kind"] == "receiver-ready" for e in cur["events"])
          and any(e["kind"] == "drill-port-open" for e in cur["events"]),
          str([e["kind"] for e in cur["events"]]))

    t1 = udp_client(DRILL_UDP_PORT)  # 首个测试终端
    t1.settimeout(3)
    t1.send(b"READING-1")
    check("第 1 步不回包（首条读数被收下但不回）",
          recv_available(t1, 1.0) == [])
    t1.send(b"READING-1")            # 同文重发，占第 2 步
    t1.settimeout(3)
    reply = t1.recv(65535)
    check("第 2 步对重发立即回包，原文一致", reply == b"READING-1")
    st = wait_drill_state("running")
    cur = st["current"]
    check("接收器逐步实收 2 条且动作正确",
          len(cur["recvs"]) == 2
          and cur["recvs"][0]["preview"] == "READING-1"
          and cur["recvs"][0]["action_label"] == "不回包"
          and cur["recvs"][1]["preview"] == "READING-1"
          and cur["recvs"][1]["action_label"] == "立即回包",
          str(cur["recvs"]))
    check("一轮只锁定首个终端",
          cur["terminal"] == f"127.0.0.1:{t1.getsockname()[1]}", cur.get("terminal"))

    # D3. 一轮只接纳首个终端：第二个终端的报文被丢弃
    t2 = udp_client(DRILL_UDP_PORT)
    t2.send(b"INTRUDER")
    check("非本轮终端收不到任何回包", recv_available(t2, 1.0) == [])
    t2.close()
    st = http_get("/api/state")["drill"]
    check("事件记录了非本轮终端丢弃",
          any(e["kind"] in ("drill-drop", "receiver-drop")
              and e.get("reason") == "terminal-locked"
              for e in st["current"]["events"]),
          str(st["current"]["events"]))

    # D4. 步骤用完后不再接纳演练报文（转发计数停在 2）
    t1.send(b"third-message")
    check("步骤用完后的报文无回包", recv_available(t1, 0.8) == [])
    st = http_get("/api/state")["drill"]
    check("步骤用完后转发数不增长（仍为 2/2）",
          st["current"]["recv_count"] == 2
          and st["current"]["remaining_steps"] == 0,
          f"{st['current']['recv_count']}/2")

    # D5. 演练期间普通端口照常收发，且不消耗演练步骤
    n1 = udp_client(UDP_PORT)
    check("演练期间普通端口照常回显", send_recv(n1, b"normal-ping") == b"normal-ping")
    n1.close()
    st = http_get("/api/state")["drill"]
    check("普通端口流量不消耗演练步骤", st["current"]["recv_count"] == 2)

    # D6. 步骤用完后模拟终端 API 也无法加入
    code, body = sim_send([{"payload": "late", "gap": 0}])
    check("演练已有终端时模拟终端被拒", code in (400, 409) and not body.get("ok"),
          str(body))

    # D7. 手动停止立即结束，下一轮可以开始（旧回包不进新轮）
    code, body = http_json("/api/drill/stop", {})
    check("手动停止返回已确认", body.get("acked") is True, str(body))
    st = wait_round_end()
    cur = st["current"]
    check("手动停止后轮次结束且端口关闭",
          cur["state"] == "ended" and cur["end_reason"] == "manual-stop"
          and cur["stop_acked"] is True, str(cur))
    check("停止后演练端口不再收包", recv_available(t1, 1.0) == [])
    t1.close()

    # D8. 延迟回包在轮结束时作废（手动停止取消 pending）
    code, body = start_drill("A", [
        {"action": "delay", "delay": 1.5},
        {"action": "immediate", "delay": 0}])
    check("第二轮启动成功（旧回包不可能进入本轮）",
          code == 200 and body.get("ok"), str(body))
    wait_drill_state("running")
    t3 = udp_client(DRILL_UDP_PORT)
    t3.send(b"will-be-cancelled")
    time.sleep(0.4)  # 延迟回包尚在 pending
    http_json("/api/drill/stop", {})
    st = wait_round_end()
    check("结束事件注明待发回包已作废",
          any(e["kind"] == "receiver-cancelled" for e in st["current"]["events"])
          and st["current"]["stop_acked"] is True,
          str(st["current"]["events"]))
    check("被作废的延迟回包从未送达终端", recv_available(t3, 2.2) == [])
    t3.close()

    # D9. 模拟终端：连发真实 UDP，按终端实收顺序列回包（延迟步放最后，顺序确定）
    code, body = start_drill("B", [
        {"action": "immediate", "delay": 0},
        {"action": "drop", "delay": 0},
        {"action": "immediate", "delay": 0},
        {"action": "delay", "delay": 1.0}])
    check("四步演练启动（接收器 B）", code == 200 and body.get("ok"), str(body))
    wait_drill_state("running")
    code, body = sim_send([
        {"payload": "m1", "gap": 0.2},
        {"payload": "m2", "gap": 0.2},
        {"payload": "m3", "gap": 0.2},
        {"payload": "m4", "gap": 0}])
    check("模拟终端连发 4 条成功", code == 200 and body.get("ok")
          and len(body.get("sent", [])) == 4, str(body))
    time.sleep(2.6)  # 等最后一步 1s 延迟回包落地
    st = http_get("/api/state")["drill"]
    cur = st["current"]
    check("模拟终端登记为本轮终端", cur["sim_terminal"] and cur["terminal"], str(cur))
    check("接收器 4 步实收/动作与编排一致",
          [r["action_label"] for r in cur["recvs"]]
          == ["立即回包", "不回包", "立即回包", "延迟回包"],
          str([r["action_label"] for r in cur["recvs"]]))
    check("终端实收回包按顺序为 m1、m3、m4（m2 不回，m4 为延迟回包）",
          [r["preview"] for r in cur["sim_replies"]] == ["m1", "m3", "m4"],
          str(cur["sim_replies"]))
    check("页面展示的回包即终端实收顺序", len(cur["sim_replies"]) == 3)

    # D10/D12. 一次只开一轮：10s 窗口内再次启动被拒；满 10 秒自动结束
    time.sleep(1.0)  # 此时 D9 那轮仍在 10s 窗口内
    code, body = start_drill("B", [
        {"action": "immediate", "delay": 0},
        {"action": "immediate", "delay": 0}])
    check("演练进行中拒绝再开一轮", code == 502 and not body.get("ok"), str(body))
    # 等自动结束落定
    st = wait_round_end()
    check("D10 端口开放满 10 秒自动结束演练",
          st["current"]["state"] == "ended"
          and st["current"]["end_reason"] == "time-limit",
          str(st["current"]))

    # D11. 自动结束后再开一轮：历史轮次保留，新一轮互不串包
    code, body = start_drill("A", [
        {"action": "immediate", "delay": 0},
        {"action": "drop", "delay": 0}])
    check("上一轮结束后可开始新一轮", code == 200 and body.get("ok"), str(body))
    wait_drill_state("running")
    t4 = udp_client(DRILL_UDP_PORT)
    check("新一轮回包正常且内容属于本轮",
          send_recv(t4, b"fresh-round", timeout=3) == b"fresh-round")
    t4.close()
    http_json("/api/drill/stop", {})
    st = wait_round_end()
    check("历史轮次已记录且不少于 4 轮", len(st["history"]) >= 4,
          f"{len(st['history'])} 轮")
    check("历史轮次均带结束原因与停止确认",
          all(h["end_reason"] and h["stop_acked"] is not None
              for h in st["history"]), str(st["history"]))

    # D13. 停止未确认：接收器进程失联时，停止结果 acked=False 且页面有状态标记
    code, body = start_drill("A", [
        {"action": "drop", "delay": 0},
        {"action": "immediate", "delay": 0}])
    check("末轮启动成功", code == 200 and body.get("ok"), str(body))
    wait_drill_state("running")
    receiver_a = procs[0]
    receiver_a.terminate()
    receiver_a.wait(timeout=5)
    time.sleep(0.5)  # 让代理控制通道感知 EOF
    code, body = http_json("/api/drill/stop", {})
    check("接收器失联时停止返回未确认",
          body.get("acked") is False, str(body))
    st = wait_round_end()
    check("轮次结束且停止确认字段为 False（页面据此红色提示“停止未确认”）",
          st["current"]["state"] == "ended"
          and st["current"]["stop_acked"] is False,
          str(st["current"]))
    # 重启接收器 A，供后续普通透传验证使用
    receiver_a = start_receiver("A", 19001, tmp)
    procs[0] = receiver_a
    time.sleep(1.0)


# ======================================================================
# 普通透传部分验证（原行为不回退）
# ======================================================================
def verify_normal():
    # 1. 初始目标为 A，报文原样往返
    st = http_get("/api/state")
    check("初始目标为 A", st["target"] == "A")
    c1 = udp_client()
    client1 = f"127.0.0.1:{c1.getsockname()[1]}"
    m1 = b"m1-hello"
    check("报文原样回传", send_recv(c1, m1) == m1)
    time.sleep(0.2)
    e = log_entries(client1, "客户端→接收器", "m1-hello")
    check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

    # 2. 二进制原样透传
    blob = bytes(range(256))
    check("二进制报文原样透传", send_recv(c1, blob) == blob)

    # 3. 网页切换目标到 B
    http_post("/switch", {"target": "B"})
    check("切换后当前目标为 B", http_get("/api/state")["target"] == "B")

    # 4. 旧会话（空闲超时内）仍走原接收器 A
    m2 = b"m2-stick"
    check("旧会话报文仍有回包", send_recv(c1, m2) == m2)
    time.sleep(0.2)
    e = log_entries(client1, "客户端→接收器", "m2-stick")
    check("切换后旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))

    # 5. 空闲 5 秒过期后，同 IP:端口 重建会话走新接收器 B
    print("--- 等待 6 秒让会话空闲过期 ---")
    time.sleep(6)
    m3 = b"m3-newsession"
    check("新会话报文有回包", send_recv(c1, m3) == m3)
    time.sleep(0.2)
    e = log_entries(client1, "客户端→接收器", "m3-newsession")
    check("过期重建的新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))

    # 6. 旧会话迟到的回包：同 IP:端口 重建会话也收不到
    c2 = udp_client()
    client2 = f"127.0.0.1:{c2.getsockname()[1]}"
    t0 = time.time()
    c2.send(b"DELAY:7:late1")          # 接收器 7 秒后才回包；会话 5 秒即过期
    time.sleep(6 - (time.time() - t0))  # 等到会话过期之后
    check("过期后新会话报文有回包", send_recv(c2, b"ping2", timeout=3) == b"ping2")
    got_late = False
    c2.settimeout(0.2)
    while time.time() - t0 < 9:
        try:
            if b"late1" in c2.recv(65535):
                got_late = True
        except socket.timeout:
            pass
    check("旧会话迟到的回包未送达重建的会话", not got_late)
    time.sleep(0.3)
    e = log_entries(client2, "接收器→客户端", "late1")
    check("日志中无 late1 的回包下发记录", not e, str(e))

    # 7. 网页试传报文
    http_post("/test", {"payload": "webtest1", "wait": "5", "delay": "0",
                        "client_port": ""})
    time.sleep(0.2)
    lt = http_get("/api/state")["last_test"]
    check("网页试传成功且回包一致",
          lt and lt["ok"] and lt["reply"] == "webtest1", str(lt))
    check("网页试传显示实际去向为 B", lt and lt["receiver"] == "B", str(lt))


def verify_restart(db_path, tmp, proxy):
    c3 = udp_client()
    c3.send(b"before-restart")
    c3.settimeout(3)
    c3.recv(65535)
    proxy.terminate()
    proxy.wait(timeout=10)
    proxy = start_proxy(db_path, tmp, "run2")
    check("代理重启后恢复", wait_http_up())
    st = http_get("/api/state")
    check("重启后目标保留为 B", st["target"] == "B")
    check("重启后会话已清空", st["sessions"] == [])
    c3.send(b"after-restart")  # 旧会话若未清空仍在 5 秒空闲窗口内
    c3.settimeout(3)
    c3.recv(65535)
    time.sleep(0.2)
    e = log_entries(f"127.0.0.1:{c3.getsockname()[1]}", "事件", "会话建立")
    check("重启后同客户端重建了全新会话", len(e) >= 1, str(e))
    return proxy


def verify_store_failure(tmp):
    # 切换保存失败时保留原目标（进程内模拟数据库故障）
    sys.path.insert(0, os.path.join(ROOT, "proxy"))
    os.environ["DB_PATH"] = os.path.join(tmp, "other.db")
    import importlib
    import app as proxy_app
    importlib.reload(proxy_app)
    s = proxy_app.Store(os.path.join(tmp, "t.db"))
    s.set_target("B")
    s._conn.close()  # 模拟存储故障
    failed = False
    try:
        s.set_target("A")
    except Exception:
        failed = True
    check("保存失败时切换报错且保留原目标", failed and s.get_target() == "B")


def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp)]
    proxy = start_proxy(db_path, tmp, "run1")
    procs.append(proxy)
    try:
        if not wait_http_up():
            check("代理启动", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        print("========== 演练行为验证 ==========")
        verify_drills(procs, tmp)
        print("========== 普通透传行为验证 ==========")
        verify_normal()
        print("========== 重启持久化验证 ==========")
        proxy = verify_restart(db_path, tmp, proxy)
        procs.append(proxy)
        verify_store_failure(tmp)

    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    print("---")
    if _failures:
        print(f"共 {len(_failures)} 项失败: {_failures}")
        return 1
    print("全部验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
