#!/usr/bin/env python3
"""Regenerate the README screenshots from the synthetic demo dashboard.

A development tool (stdlib only). It starts the demo from ``demo_dashboard.py``
on 127.0.0.1, launches headless Chrome or Chromium, drives it over the DevTools
protocol (click the 24 h window, expand groups), waits for the page to render and
writes three PNGs at 2x scale into ``docs/img``:

    dashboard.png         desktop, default view on the 24 h window
    dashboard-charts.png  desktop, Performance and Cache groups expanded
    dashboard-phone.png   390 px wide phone view

    python tools/capture_screenshots.py [--out docs/img] [--chrome PATH] [--scale 2]

Chrome is started with name resolution blocked for everything except 127.0.0.1,
a throwaway profile and no background networking. The mouse is never moved, so no
hover state appears. Everything on the page is synthetic (see demo_dashboard.py).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import demo_dashboard  # noqa: E402

MAC_CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def find_chrome(explicit: str | None) -> str:
    cands = [explicit, os.environ.get("CHROME")]
    cands += [MAC_CHROME]
    cands += [shutil.which(n) for n in ("google-chrome", "chromium", "chromium-browser", "chrome")]
    for c in cands:
        if c and os.path.exists(c):
            return c
    raise SystemExit("Chrome/Chromium not found; pass --chrome PATH or set CHROME")


# ---- a minimal WebSocket client (RFC 6455), enough for the DevTools protocol ----
class WebSocket:
    def __init__(self, host: str, port: int, path: str, timeout: float = 60.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall(
            (
                f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n"
                f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n\r\n"
            ).encode()
        )
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("websocket handshake failed")
            head += chunk
        if b" 101 " not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError("websocket upgrade refused: " + head[:80].decode("latin1"))
        self.buf = head.split(b"\r\n\r\n", 1)[1]

    def _read(self, n: int) -> bytes:
        while len(self.buf) < n:
            chunk = self.sock.recv(1 << 20)
            if not chunk:
                raise ConnectionError("websocket closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send(self, text: str) -> None:
        data = text.encode()
        n = len(data)
        head = bytearray([0x81])
        if n < 126:
            head.append(0x80 | n)
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        self.sock.sendall(bytes(head) + mask + masked)

    def recv(self) -> str:
        payload = b""
        while True:
            b0, b1 = self._read(2)
            n = b1 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._read(8))[0]
            data = self._read(n)
            op = b0 & 0x0F
            if op == 0x8:
                raise ConnectionError("websocket closed by peer")
            if op in (0x9, 0xA):  # ping / pong
                continue
            payload += data
            if b0 & 0x80:
                return payload.decode()

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


class Page:
    """One DevTools page session."""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self._id = 0

    def call(self, method: str, **params):
        self._id += 1
        mid = self._id
        self.ws.send(json.dumps({"id": mid, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == mid:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})

    def eval(self, expr: str):
        r = self.call("Runtime.evaluate", expression=expr, returnByValue=True, awaitPromise=True)
        if "exceptionDetails" in r:
            raise RuntimeError("page script error: " + json.dumps(r["exceptionDetails"])[:300])
        return r["result"].get("value")

    def wait_for(self, expr: str, what: str, timeout: float = 45.0):
        end = time.time() + timeout
        while time.time() < end:
            try:
                v = self.eval(expr)
            except RuntimeError:
                v = None
            if v:
                return v
            time.sleep(0.25)
        raise TimeoutError("timed out waiting for " + what)

    def viewport(self, width: int, height: int, scale: float, mobile: bool) -> None:
        self.call(
            "Emulation.setDeviceMetricsOverride",
            width=width,
            height=height,
            deviceScaleFactor=scale,
            mobile=mobile,
        )

    def shot(self, path: str) -> int:
        r = self.call("Page.captureScreenshot", format="png", fromSurface=True)
        data = base64.b64decode(r["data"])
        with open(path, "wb") as fh:
            fh.write(data)
        return len(data)


def launch_chrome(chrome: str, profile: str) -> tuple[subprocess.Popen, int]:
    args = [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--hide-scrollbars",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-sync",
        "--disable-extensions",
        "--disable-default-apps",
        "--metrics-recording-only",
        "--mute-audio",
        "--force-color-profile=srgb",
        "--font-render-hinting=none",
        "--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE 127.0.0.1",
        f"--user-data-dir={profile}",
        "--remote-debugging-port=0",
        "about:blank",
    ]
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    port_file = os.path.join(profile, "DevToolsActivePort")
    end = time.time() + 30
    while time.time() < end:
        if os.path.exists(port_file):
            with open(port_file) as fh:
                lines = fh.read().split()
            if lines:
                return proc, int(lines[0])
        if proc.poll() is not None:
            raise SystemExit("Chrome exited at start-up")
        time.sleep(0.2)
    proc.kill()
    raise SystemExit("Chrome did not open a DevTools port")


def open_page(port: int) -> Page:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=10) as r:
        targets = json.load(r)
    tgt = next(t for t in targets if t.get("type") == "page")
    path = "/" + tgt["webSocketDebuggerUrl"].split("/", 3)[3]
    return Page(WebSocket("127.0.0.1", port, path))


# ---- page helpers (run inside the dashboard page) ----
READY = (
    "document.querySelectorAll('#cards .card').length>=12 && document.querySelectorAll('#slots .slot').length>=2"
    " && document.querySelectorAll('#rows tr.trow').length>0"
    " && !/^—/.test(document.querySelector('#cards .card .big').textContent.trim())"
)


def select_window(page: Page, sec: int) -> None:
    page.eval(f"document.querySelector('#wsel button[data-w=\"{sec}\"]').click()")
    page.wait_for(
        f"typeof cs!=='undefined' && cs.last && cs.last[0] && cs.last[0].window_sec==={sec}"
        f" && document.querySelectorAll('#rows tr.trow').length>0 && !tl.busy",
        f"{sec}s window to render",
    )
    time.sleep(0.8)


def row_bottom(page: Page, n: int) -> int:
    """Document y (CSS px) just below the n-th request row."""
    v = page.eval(
        f"(()=>{{const r=document.querySelectorAll('#rows tr.trow')[{n - 1}];"
        "if(!r)return 0;return Math.ceil(r.getBoundingClientRect().bottom+window.scrollY);})()"
    )
    if not v:
        raise RuntimeError("request rows missing")
    return int(v)


def fit_height(page: Page, width: int, scale: float, mobile: bool, rows: int) -> int:
    """Size the viewport to end just below `rows` request rows. Layout can shift when the
    viewport changes, so measure again after resizing until the height is stable."""
    h = row_bottom(page, rows)
    for _ in range(4):
        page.viewport(width, h, scale, mobile)
        time.sleep(0.8)
        h2 = row_bottom(page, rows)
        if abs(h2 - h) <= 1:
            break
        h = h2
    return h


def fresh_page(page: Page, url: str) -> None:
    page.call("Page.navigate", url=url)
    page.wait_for("document.readyState==='complete'", "page load")
    page.eval("try{sessionStorage.clear()}catch(e){}")
    page.call("Page.reload")
    page.wait_for(READY, "cards and request rows")
    time.sleep(0.5)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    default_out = os.path.join(os.path.dirname(HERE), "docs", "img")
    ap.add_argument("--out", default=default_out, help="output directory (default docs/img)")
    ap.add_argument("--chrome", default=None, help="Chrome/Chromium binary")
    ap.add_argument("--scale", type=float, default=2.0, help="device scale factor (default 2)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--desktop-rows", type=int, default=6, help="request rows in dashboard.png")
    ap.add_argument("--phone-rows", type=int, default=3, help="request rows in the phone image")
    args = ap.parse_args(argv)
    os.makedirs(args.out, exist_ok=True)
    chrome = find_chrome(args.chrome)

    # new turns every 15 s keep "last completion" fresh while the pictures are taken
    demo = demo_dashboard.start_demo(seed=args.seed, port=0, live_interval=15, quiet=True)
    profile = tempfile.mkdtemp(prefix="slotpin-chrome-")
    proc = None
    page = None
    try:
        proc, port = launch_chrome(chrome, profile)
        page = open_page(port)
        page.call("Page.enable")
        page.call("Emulation.setTimezoneOverride", timezoneId="UTC")

        # 1 + 2: desktop
        W = 1440
        page.viewport(W, 1800, args.scale, False)
        fresh_page(page, demo.url)
        select_window(page, 86400)
        h = fit_height(page, W, args.scale, False, args.desktop_rows)
        out1 = os.path.join(args.out, "dashboard.png")
        print(out1, W, h, page.shot(out1), "bytes")

        page.viewport(W, 3000, args.scale, False)
        page.eval(
            "for(const g of ['perf','cache']){const s=document.querySelector(`.grp[data-g=\"${g}\"]`);"
            "if(!s.classList.contains('open'))s.querySelector('.ghead').click();}"
        )
        page.wait_for("document.querySelectorAll('.grp.open svg.cp').length>=8", "charts")
        time.sleep(0.8)
        h2 = int(
            page.eval(
                "Math.ceil(document.querySelector('.grp[data-g=\"cache\"] .chs')"
                ".getBoundingClientRect().bottom+window.scrollY)"
            )
        ) + 18
        page.viewport(W, h2, args.scale, False)
        time.sleep(0.8)
        out2 = os.path.join(args.out, "dashboard-charts.png")
        print(out2, W, h2, page.shot(out2), "bytes")

        # 3: phone
        PW = 390
        page.viewport(PW, 1800, args.scale, True)
        fresh_page(page, demo.url)
        select_window(page, 86400)
        h3 = fit_height(page, PW, args.scale, True, args.phone_rows)
        out3 = os.path.join(args.out, "dashboard-phone.png")
        print(out3, PW, h3, page.shot(out3), "bytes")
    finally:
        if page is not None:
            page.ws.close()
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
        shutil.rmtree(profile, ignore_errors=True)
        demo.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
