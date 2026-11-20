"""仅供无 pip 环境下运行 scripts/verify_local.py 的最小 Flask 垫片。

实现 app.py 实际用到的 Flask 子集（路由、form/json、jsonify、redirect、
flash、支持 if/for/with 与常用过滤器的极简 Jinja、url_for）。
生产/容器内使用 requirements.txt 中的真实 Flask，本模块不参与镜像构建。
"""
import json as _json
import os
import re
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# ---------------------------------------------------------------- Jinja 子集
_TOKEN_RE = re.compile(r"(\{\{.*?\}\}|\{%.*?%\})", re.S)


def _jinja_get(name, ctx):
    if name in ctx:
        return ctx[name]
    raise KeyError(name)


def _dot_to_bracket(expr):
    """Jinja 中 a.b 对 dict 也是取键；改写为 a["b"]，数字索引保持 a[0]。"""
    out = []
    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == "." and i + 1 < len(expr) and (expr[i + 1].isalpha()
                                                or expr[i + 1] == "_"):
            j = i + 1
            while j < len(expr) and (expr[j].isalnum() or expr[j] == "_"):
                j += 1
            out.append('["' + expr[i + 1:j] + '"]')
            i = j
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _jinja_eval(expr, ctx):
    expr = expr.strip()
    expr = re.sub(r"\burl_for\('([^']+)'\)", r"_url_for('\1')", expr)
    expr = re.sub(r"\btrue\b", "True", expr)
    expr = re.sub(r"\bfalse\b", "False", expr)
    expr = re.sub(r"\bnull\b", "None", expr)
    # 过滤器（链式，从左到右）： |e |default(x) |join(',')
    if "|" in expr:
        parts = [p.strip() for p in expr.split("|")]
        val = _jinja_eval(parts[0], ctx)
        for f in parts[1:]:
            fm = re.match(r"(\w+)(?:\((.*)\))?$", f)
            fname, farg = fm.group(1), fm.group(2)
            if fname == "e":
                val = (str(val).replace("&", "&amp;").replace("<", "&lt;")
                       .replace(">", "&gt;").replace('"', "&quot;"))
            elif fname == "default":
                if val is None or val == "":
                    val = _jinja_eval(farg, ctx) if farg else ""
            elif fname == "join":
                sep = _jinja_eval(farg, ctx) if farg else ""
                val = sep.join(str(x) for x in val)
            else:
                raise RuntimeError(f"垫片不支持过滤器 {fname}")
        return val
    expr = _dot_to_bracket(expr)
    try:
        return eval(expr, {"__builtins__": {}}, dict(ctx, _url_for=_url_for))
    except NameError:
        # 模板里未定义的变量按 Jinja 语义视为空
        return ""


def _render_nodes(nodes, ctx):
    out = []
    i = 0
    while i < len(nodes):
        tok = nodes[i]
        if tok["type"] == "text":
            out.append(tok["text"])
        elif tok["type"] == "var":
            val = _jinja_eval(tok["expr"], ctx)
            out.append("" if val is None else str(val))
        elif tok["type"] in ("if", "with", "for"):
            block = tok
            inner = block["body"]
            if block["type"] == "if":
                cond_ctx = ctx
                if _truthy(_jinja_eval(block["cond"], cond_ctx)):
                    out.append(_render_nodes(inner, ctx))
                else:
                    for elif_cond, elif_body in block.get("elifs", []):
                        if _truthy(_jinja_eval(elif_cond, ctx)):
                            out.append(_render_nodes(elif_body, ctx))
                            break
                    else:
                        if block.get("else_body") is not None:
                            out.append(_render_nodes(block["else_body"], ctx))
            elif block["type"] == "with":
                local = dict(ctx)
                for assign in block["assign"].split(","):
                    if not assign.strip():
                        continue
                    k, v = assign.split("=", 1)
                    local[k.strip()] = _jinja_eval(v, local)
                out.append(_render_nodes(inner, local))
            else:  # for
                var, iterable = block["loop_var"], _jinja_eval(block["iterable"], ctx)
                for idx, item in enumerate(iterable):
                    local = dict(ctx)
                    if "," in var:
                        names = [v.strip() for v in var.split(",")]
                        if isinstance(item, dict):
                            for n in names:
                                local[n] = item.get(n)
                        else:
                            for n, v in zip(names, item):
                                local[n] = v
                    else:
                        local[var.strip()] = item
                    local["loop"] = {"index": idx + 1, "index0": idx,
                                     "first": idx == 0, "last": idx == len(iterable) - 1}
                    out.append(_render_nodes(inner, local))
        i += 1
    return "".join(out)


def _truthy(v):
    return bool(v)


def _parse_template(src):
    tokens = _TOKEN_RE.split(src)
    nodes, _ = _parse(tokens, 0, stop_on=None)
    return nodes


def _parse(tokens, pos, stop_on):
    nodes = []
    while pos < len(tokens):
        raw = tokens[pos]
        if raw.startswith("{{"):
            nodes.append({"type": "var", "expr": raw[2:-2]})
        elif raw.startswith("{%"):
            tag = raw[2:-2].strip()
            if stop_on and tag in stop_on:
                return nodes, pos
            if tag.startswith("if "):
                block, pos = _parse_block(tokens, pos, "if", tag[3:])
                nodes.append(block)
                continue
            if tag.startswith("with "):
                block, pos = _parse_block(tokens, pos, "with", tag[5:])
                nodes.append(block)
                continue
            if tag.startswith("for "):
                head = tag[4:]
                var, iterable = head.split(" in ", 1)
                block, pos = _parse_block(tokens, pos, "for", None,
                                          loop_var=var.strip(),
                                          iterable=iterable.strip())
                nodes.append(block)
                continue
            # 独立语句（垫片场景可忽略）
            nodes.append({"type": "text", "text": ""})
        else:
            nodes.append({"type": "text", "text": raw})
        pos += 1
    if stop_on is not None:
        raise RuntimeError("模板块未闭合")
    return nodes, pos


def _parse_block(tokens, pos, kind, head, loop_var=None, iterable=None):
    # tokens[pos] 是开标签；收集块内原始 token 后递归解析为节点
    depth = 1
    body_raw, elif_raw, else_raw = [], [], None
    target = body_raw
    pos += 1
    elif_conds = []
    while pos < len(tokens):
        raw = tokens[pos]
        if raw.startswith("{%"):
            tag = raw[2:-2].strip()
            if tag.startswith(("if ", "with ", "for ")):
                depth += 1
            elif tag in ("endif", "endwith", "endfor"):
                depth -= 1
                if depth == 0:
                    pos += 1
                    body, _ = _parse(body_raw, 0, stop_on=None)
                    elifs = []
                    for cond, raws in elif_conds:
                        sub, _ = _parse(raws, 0, stop_on=None)
                        elifs.append((cond, sub))
                    else_body = None
                    if else_raw is not None:
                        else_body, _ = _parse(else_raw, 0, stop_on=None)
                    block = {"type": kind, "body": body}
                    if kind == "if":
                        block["cond"] = head
                        block["elifs"] = elifs
                        block["else_body"] = else_body
                    elif kind == "with":
                        block["assign"] = head
                    else:
                        block["loop_var"] = loop_var
                        block["iterable"] = iterable
                    return block, pos
            elif kind == "if" and depth == 1 and tag.startswith("elif "):
                elif_conds.append((tag[5:], []))
                target = elif_conds[-1][1]
                pos += 1
                continue
            elif kind == "if" and depth == 1 and tag == "else":
                else_raw = []
                target = else_raw
                pos += 1
                continue
        target.append(raw)
        pos += 1
    raise RuntimeError(f"{kind} 块未闭合")


def _tiny_render(template, ctx):
    with open(template, encoding="utf-8") as f:
        nodes = _parse_template(f.read())
    return _render_nodes(nodes, dict(ctx, url_for=_url_for,
                                     get_flashed_messages=get_flashed_messages))


def _url_for(endpoint, **kw):
    table = {"index": "/", "switch": "/switch", "test": "/test"}
    return table.get(endpoint, "/" + endpoint)


def url_for(endpoint, **kw):
    return _url_for(endpoint, **kw)


# ---------------------------------------------------------------- HTTP 框架
class _Request:
    def __init__(self, handler):
        self.method = handler.command
        self.path = handler.path.split("?", 1)[0]
        self.form = {}
        self._json = None
        length = int(handler.headers.get("Content-Length") or 0)
        raw = handler.rfile.read(length) if length else b""
        ctype = handler.headers.get("Content-Type", "")
        if "application/json" in ctype:
            try:
                self._json = _json.loads(raw.decode("utf-8")) if raw else {}
            except _json.JSONDecodeError:
                self._json = {}
        elif raw:
            self.form = {k: v[0] for k, v in
                         urllib.parse.parse_qs(raw.decode("utf-8")).items()}

    def get_json(self, silent=False):
        return self._json if self._json is not None else ({} if silent else None)


class _Response(Exception):
    def __init__(self, body, status=200, headers=None):
        self.body = body if isinstance(body, bytes) else str(body).encode("utf-8")
        self.status = status
        self.headers = headers or {}


def jsonify(obj):
    return _Response(_json.dumps(obj, ensure_ascii=False),
                     headers={"Content-Type": "application/json; charset=utf-8"})


def redirect(location, code=302):
    return _Response("", code, {"Location": location})


def render_template(name, **ctx):
    import flask as _f
    return _tiny_render(_f._template_path(name), ctx)


_flashes = []


def flash(msg, category="message"):
    _flashes.append((category, msg))


def get_flashed_messages(with_categories=False):
    items = list(_flashes)
    _flashes.clear()
    return items if with_categories else [m for _, m in items]


class Flask:
    def __init__(self, name):
        self.name = name
        self.secret_key = None
        self._routes = {}
        self.config = {}

    def route(self, path, methods=("GET",)):
        def deco(fn):
            for m in methods:
                self._routes[(m, path)] = fn
            return fn
        return deco

    def get(self, path):
        return self.route(path, ("GET",))

    def post(self, path):
        return self.route(path, ("POST",))

    def run(self, host="127.0.0.1", port=5000, threaded=True,
            use_reloader=False, **kw):
        app = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _handle(self):
                request._set(_Request(self))
                fn = app._routes.get((self.command, self.path.split("?", 1)[0]))
                if fn is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                try:
                    rv = fn()
                    if isinstance(rv, tuple):
                        resp, status = rv[0], rv[1]
                    else:
                        resp, status = rv, 200
                    if not isinstance(resp, _Response):
                        resp = _Response(resp, status)
                        status = resp.status
                except _Response as r:
                    resp, status = r, r.status
                self.send_response(status)
                for k, v in resp.headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(resp.body)))
                self.end_headers()
                self.wfile.write(resp.body)

            do_GET = _handle
            do_POST = _handle

        ThreadingHTTPServer((host, port), Handler).serve_forever()


class _ReqProxy:
    def __init__(self):
        self.local = threading.local()

    def _set(self, req):
        self.local.req = req

    def __getattr__(self, item):
        return getattr(self.local.req, item)


request = _ReqProxy()


def _template_path(name):
    # 垫片位于 <workspace>/testlibs/flask_shim/flask/__init__.py，上溯三级到仓库根
    root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))))
    return os.path.join(root, "proxy", "templates", name)
