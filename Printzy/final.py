#!/usr/bin/env python3
"""
Actuator Serial Console
Streamlit app for communicating with an actuator over a serial protocol.
Prints blank pages through CUPS on the Raspberry Pi, then raises the
paper tray with MOVE_UP so the printer can pick the remaining sheets.
"""

import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from collections import deque
from datetime import datetime

import serial
import serial.tools.list_ports
import streamlit as st

# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------
FRAME_SIZE = 7
HEADER = 0xAA

CMDS = {
    "START": 0x01,
    "STOP": 0x02,
    "MOVE_UP": 0x03,
    "MOVE_DOWN": 0x04,
    "HOME": 0x05,
    "STATUS": 0x06,
}
CMD_NAMES = {v: k for k, v in CMDS.items()}

STATES = {
    0x00: "INIT",
    0x01: "HOMING",
    0x02: "READY",
    0x03: "MOVING",
    0x04: "STOPPED",
}

DIRS = {
    0x00: "NONE",
    0x01: "UP",
    0x02: "DOWN",
}

CHECKSUM_MODES = [
    "XOR (bytes 0-5)",
    "SUM (bytes 0-5)",
    "XOR (bytes 1-5, no header)",
    "SUM (bytes 1-5, no header)",
]


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------
def checksum(body: bytes, mode: str) -> int:
    if mode == "XOR (bytes 0-5)":
        c = 0
        for b in body:
            c ^= b
        return c
    if mode == "SUM (bytes 0-5)":
        return sum(body) & 0xFF
    if mode == "SUM (bytes 1-5, no header)":
        return sum(body[1:]) & 0xFF
    if mode == "XOR (bytes 1-5, no header)":
        c = 0
        for b in body[1:]:
            c ^= b
        return c
    return 0


def build_frame(
    cmd: int,
    state: int,
    direction: int,
    pos: int,
    data: int,
    mode: str,
    bad_cksum: bool = False,
) -> bytes:
    body = bytes([HEADER, cmd, state, direction, pos, data])
    ck = checksum(body, mode)
    if bad_cksum:
        ck ^= 0xFF
    return body + bytes([ck])


def parse_frame(raw: bytes, mode: str) -> dict:
    ck_ok = checksum(raw[:6], mode) == raw[6]
    return {
        "raw": raw.hex(" ").upper(),
        "cmd": CMD_NAMES.get(raw[1], f"0x{raw[1]:02X}"),
        "state": STATES.get(raw[2], f"0x{raw[2]:02X}"),
        "dir": DIRS.get(raw[3], f"0x{raw[3]:02X}"),
        "pos": raw[4],
        "data": raw[5],
        "checksum_ok": ck_ok,
    }


# ---------------------------------------------------------------------------
# RX listener (background thread, one instance per Streamlit server)
# ---------------------------------------------------------------------------
class RxListener:
    """Owns the serial port's receive side.

    One instance per Streamlit server (st.cache_resource), so the thread
    keeps running across script reruns.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.ser = None
        self.port_name = ""
        self.ck_mode = CHECKSUM_MODES[0]  # updated from the sidebar
        self.log = deque(maxlen=500)  # newest first
        self.last_status = None
        self.last_rx = None  # time.monotonic() of last frame
        self.rx_count = 0
        self.error = ""
        self._buf = bytearray()
        self._last_raw = None
        self.reset_stats()
        threading.Thread(
            target=self._run, daemon=True, name="rx-listener"
        ).start()

    def reset_stats(self):
        with self.lock:
            self.rx_count = 0
            self.ck_pass = 0  # frames whose checksum matched
            self.ck_fail = 0  # frames whose checksum did not
            self.status_pkts = 0  # valid STATUS frames
            self.last_status_rx = None  # monotonic time of last STATUS
            self.lat = deque(maxlen=200)  # command -> reply latency (s)
            self.last_lat = None
            self.periods = deque(maxlen=200)  # STATUS-to-STATUS interval (s)
            self._last_status_t = None
            self._pending = None  # (cmd byte, t_tx) awaiting reply

    # -- port control (UI thread) ------------------------------------------
    def connect(self, port: str, baud: int):
        self.disconnect()
        ser = serial.Serial(
            port,
            baud,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=0.05,
        )
        with self.lock:
            self._buf.clear()
            self._last_raw = None
            self.last_status = None
            self.reset_stats()
            self.error = ""
            self.port_name = port
            self.ser = ser

    def disconnect(self):
        with self.lock:
            ser, self.ser = self.ser, None
        if ser is not None:
            try:
                ser.close()
            except Exception:
                pass

    def is_open(self) -> bool:
        s = self.ser
        return s is not None and s.is_open

    def write(self, frame: bytes, note: str) -> bool:
        """Send from the UI thread (pyserial allows read + write in parallel)."""
        ser = self.ser
        if ser is None or not ser.is_open:
            return False
        t0 = time.perf_counter()
        # Hold the lock across the write so the print thread and the UI
        # cannot split two frames on the wire.
        with self.lock:
            self._pending = (frame[1], t0)  # latency is measured from here
            ser.write(frame)
            ser.flush()
        self.add_log("TX", frame, note)
        return True

    def add_log(
        self,
        direction: str,
        raw: bytes,
        note: str = "",
        repeated: bool = False,
    ):
        with self.lock:
            self.log.appendleft(
                {
                    "time": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                    "dir": direction,
                    "bytes": raw.hex(" ").upper(),
                    "note": note,
                    "repeated": repeated,
                }
            )

    def clear_log(self):
        with self.lock:
            self.log.clear()

    # -- background thread -------------------------------------------------
    def _drain(self, t_rx: float):
        """Pull every complete frame out of the buffer, resyncing on 0xAA.

        t_rx = perf_counter() taken right after the bytes were read.
        """
        buf = self._buf
        while len(buf) >= FRAME_SIZE:
            if buf[0] != HEADER:
                del buf[0]
                continue
            raw = bytes(buf[:FRAME_SIZE])
            del buf[:FRAME_SIZE]
            p = parse_frame(raw, self.ck_mode)
            ok = p["checksum_ok"]
            repeated = raw == self._last_raw
            self._last_raw = raw
            lat_note = ""
            with self.lock:
                self.last_rx = time.monotonic()
                self.rx_count += 1
                if ok:
                    self.ck_pass += 1
                    # command -> reply latency: first valid frame echoing the
                    # command byte of the most recent TX
                    pend = self._pending
                    if pend and raw[1] == pend[0] and t_rx >= pend[1]:
                        lat = t_rx - pend[1]
                        self.lat.append(lat)
                        self.last_lat = lat
                        self._pending = None
                        lat_note = f" latency={lat * 1000:.1f} ms"
                    # live status comes only from valid STATUS packets
                    if raw[1] == CMDS["STATUS"]:
                        self.last_status = p
                        self.status_pkts += 1
                        self.last_status_rx = self.last_rx
                        if self._last_status_t is not None:
                            self.periods.append(t_rx - self._last_status_t)
                        self._last_status_t = t_rx
                else:
                    self.ck_fail += 1
            self.add_log(
                "RX",
                raw,
                f"{p['cmd']} state={p['state']} dir={p['dir']} "
                f"pos={p['pos']} data={p['data']} "
                f"{'OK' if ok else 'BAD CHECKSUM'}{lat_note}",
                repeated,
            )

    def _run(self):
        while True:
            ser = self.ser
            if ser is None or not ser.is_open:
                time.sleep(0.1)
                continue
            try:
                data = ser.read(ser.in_waiting or 1)  # blocks up to timeout
                if data:
                    t_rx = time.perf_counter()
                    self._buf.extend(data)
                    self._drain(t_rx)
            except Exception as e:
                if self.ser is ser:  # not a deliberate disconnect
                    with self.lock:
                        self.error = f"Serial error: {e}"
                    self.disconnect()


# ---------------------------------------------------------------------------
# Streamlit helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_rx() -> RxListener:
    return RxListener()


def transact(cmd_name: str, rx: RxListener, ss):
    """Send one host frame. The reply is picked up by the RX listener."""
    if not rx.is_open():
        st.error("Port not connected.")
        return
    frame = build_frame(
        CMDS[cmd_name],
        ss.f_state,
        ss.f_dir,
        ss.f_pos,
        ss.f_data,
        ss.ck_mode,
        ss.bad_ck,
    )
    rx.write(
        frame,
        cmd_name + (" (bad checksum)" if ss.bad_ck else ""),
    )


def _ms(x) -> str:
    return "-" if x is None else f"{x * 1000:.1f} ms"


def _avg(xs):
    return sum(xs) / len(xs) if xs else None


# ---------------------------------------------------------------------------
# Blank pages, then raise the paper tray (Raspberry Pi / CUPS)
# ---------------------------------------------------------------------------
MAX_BLANK_PAGES = 100
_BLANK_PS = """%!PS-Adobe-3.0
%%Pages: 1
%%EndComments
showpage
%%EOF
"""


def _decode_output(data: bytes) -> str:
    return data.decode("utf-8", errors="replace").strip()


def run_cups(args, timeout: float):
    """Run a CUPS command. Returns (returncode, stdout, stderr)."""
    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return 124, "", "timed out"
    except FileNotFoundError:
        return 127, "", "not found"
    return proc.returncode, _decode_output(proc.stdout), _decode_output(proc.stderr)


def cups_tools_ready() -> bool:
    return shutil.which("lp") is not None and shutil.which("lpstat") is not None


def parse_lp_job_id(text: str) -> str:
    """Pull the CUPS job id out of `lp` output."""
    match = re.search(r"request id is (\S+)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"(\S+-\d+)", text)
    return match.group(1) if match else ""


@st.cache_data(ttl=15, show_spinner=False)
def list_cups_printers():
    """Return (names, default_name, error)."""
    if not shutil.which("lpstat"):
        return (
            [],
            "",
            "CUPS was not found. On the Raspberry Pi install cups-client "
            "(lp and lpstat).",
        )
    code, out, err = run_cups(["lpstat", "-p"], timeout=15)
    names = []
    for line in out.splitlines():
        match = re.match(r"printer\s+(\S+)\s+", line)
        if match:
            names.append(match.group(1))
    _, default_out, _ = run_cups(["lpstat", "-d"], timeout=15)
    default_name = ""
    default_match = re.search(r"destination:\s*(\S+)", default_out)
    if default_match:
        default_name = default_match.group(1)
    if names:
        return names, default_name, ""
    message = err or out
    if code == 127 or "not found" in message.lower():
        message = (
            "CUPS was not found. On the Raspberry Pi install cups-client "
            "(lp and lpstat)."
        )
    elif not message:
        message = "No CUPS printers found."
    return [], default_name, message[:400]


def submit_blank_pages(printer: str, pages: int) -> str:
    """Queue N blank pages. Returns the CUPS job id."""
    fd, path = tempfile.mkstemp(prefix="printzy-blank-", suffix=".ps")
    try:
        with os.fdopen(fd, "w", encoding="ascii", newline="\n") as handle:
            handle.write(_BLANK_PS)
        code, out, err = run_cups(
            [
                "lp",
                "-d",
                printer,
                "-n",
                str(pages),
                "-t",
                f"Printzy blank pages ({pages})",
                path,
            ],
            timeout=60,
        )
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    text = "\n".join(part for part in (out, err) if part)
    if code != 0:
        raise RuntimeError(text or "The printer rejected the blank pages.")
    job_id = parse_lp_job_id(text)
    if not job_id:
        raise RuntimeError(text or "CUPS did not return a job id.")
    return job_id


def _cups_failed(text: str) -> bool:
    lowered = text.lower()
    return "scheduler is not running" in lowered or "connection refused" in lowered


def _lists_job(job_id: str, text: str) -> bool:
    """True when an lpstat line is exactly this job, not a longer id."""
    for line in text.splitlines():
        parts = line.split()
        if parts and parts[0] == job_id:
            return True
    return False


def job_still_printing(job_id: str) -> bool:
    _code, out, err = run_cups(["lpstat", "-o", job_id], timeout=15)
    if _cups_failed(out + "\n" + err):
        raise RuntimeError("CUPS is not running.")
    return _lists_job(job_id, out)


def job_finished(job_id: str) -> bool:
    _code, out, err = run_cups(
        ["lpstat", "-W", "completed", "-o", job_id],
        timeout=15,
    )
    if _cups_failed(out + "\n" + err):
        raise RuntimeError("CUPS is not running.")
    return _lists_job(job_id, out)


def send_tray_up(
    rx: RxListener,
    state: int,
    direction: int,
    pos: int,
    data: int,
    pages: int,
    ck_mode: str,
) -> bool:
    """Same frame as the MOVE_UP button, sent after the print job finishes.

    The controller raises the lead-screw tray toward the pickup roller.
    The limit switch at the roller stops the rise.
    """
    frame = build_frame(
        CMDS["MOVE_UP"],
        state & 0xFF,
        direction & 0xFF,
        pos & 0xFF,
        data & 0xFF,
        ck_mode,
    )
    return rx.write(frame, f"MOVE_UP after {pages} blank page(s)")


class TrayJob:
    """Print blank pages, wait until CUPS finishes, then raise the tray."""

    def __init__(self):
        self.lock = threading.Lock()
        self.phase = "idle"
        self.detail = ""

    def is_running(self) -> bool:
        with self.lock:
            return self.phase in {"printing", "waiting", "moving"}

    def snapshot(self) -> dict:
        with self.lock:
            return {"phase": self.phase, "detail": self.detail}

    def _set(self, phase: str, detail: str):
        with self.lock:
            self.phase = phase
            self.detail = detail

    def start(
        self, rx, pages, printer, state, direction, pos, data, ck_mode
    ) -> bool:
        with self.lock:
            if self.phase in {"printing", "waiting", "moving"}:
                return False
            self.phase = "printing"
            self.detail = f"Sending {pages} blank page(s) to {printer}..."
        threading.Thread(
            target=self._run,
            args=(rx, pages, printer, state, direction, pos, data, ck_mode),
            daemon=True,
            name="tray-after-print",
        ).start()
        return True

    def _wait_until_printed(self, job_id: str, pages: int) -> str:
        """Return done, timeout, unconfirmed, or cups-down."""
        deadline = time.monotonic() + (40 + 25 * pages)
        seen = False
        while time.monotonic() < deadline:
            try:
                active = job_still_printing(job_id)
                finished = False if active else job_finished(job_id)
            except RuntimeError:
                return "cups-down"
            if active:
                seen = True
                self._set(
                    "waiting",
                    f"Printing {pages} blank page(s) ({job_id}). "
                    "The tray stays down until this job finishes.",
                )
            elif finished or seen:
                # Seen in the queue, then gone: CUPS finished or cancelled it.
                # `finished` covers a job that ended before the first poll.
                return "done"
            time.sleep(1)
        return "timeout" if seen else "unconfirmed"

    def _run(self, rx, pages, printer, state, direction, pos, data, ck_mode):
        try:
            job_id = submit_blank_pages(printer, pages)
            self._set(
                "waiting",
                f"Printing {pages} blank page(s) ({job_id}). "
                "The tray stays down until this job finishes.",
            )
            result = self._wait_until_printed(job_id, pages)
            if result == "timeout":
                self._set(
                    "error",
                    f"Job {job_id} is still in the printer queue. "
                    "The tray was left where it is. Press MOVE_UP after the pages finish.",
                )
                return
            if result == "unconfirmed":
                self._set(
                    "error",
                    f"CUPS accepted job {job_id}, but it never showed as finished. "
                    "The tray was left where it is. Press MOVE_UP after the pages finish.",
                )
                return
            if result == "cups-down":
                self._set(
                    "error",
                    "CUPS stopped during the print. The tray was left where it is.",
                )
                return
            self._set(
                "moving",
                "Print job finished. Sending MOVE_UP to raise the tray...",
            )
            time.sleep(1)
            if not rx.is_open():
                self._set(
                    "error",
                    "The pages printed, but the actuator port is closed, "
                    "so MOVE_UP was not sent.",
                )
                return
            try:
                sent = send_tray_up(
                    rx, state, direction, pos, data, pages, ck_mode
                )
            except Exception as exc:
                self._set(
                    "error",
                    f"The pages printed, but raising the tray failed: {exc}",
                )
                return
            if not sent:
                self._set(
                    "error",
                    "The pages printed, but the actuator port is closed, "
                    "so MOVE_UP was not sent.",
                )
                return
            self._set(
                "done",
                f"Printed {pages} blank page(s) on {printer} and sent MOVE_UP. "
                "The tray raises the remaining paper to the pickup roller.",
            )
        except Exception as exc:
            self._set("error", f"{exc} The tray was not moved.")


@st.cache_resource
def get_tray_job() -> TrayJob:
    return TrayJob()


@st.fragment(run_every=0.5)
def tray_job_status():
    snap = get_tray_job().snapshot()
    phase = snap["phase"]
    detail = snap["detail"]
    if phase == "idle" or not detail:
        return
    if phase == "error":
        st.error(detail)
    elif phase == "done":
        st.success(detail)
    else:
        st.info(detail)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Actuator Console", layout="wide")
    st.title("Actuator Serial Console")

    rx = get_rx()
    ss = st.session_state

    # ---- Sidebar: connection ---------------------------------------------
    with st.sidebar:
        st.header("Connection")
        ports = [p.device for p in serial.tools.list_ports.comports()]
        port = st.selectbox("Port", ports or ["(none found)"])
        manual = st.text_input("…or type a port", "")
        baud = st.selectbox(
            "Baud",
            [9600, 19200, 38400, 57600, 115200, 230400],
            index=4,
        )
        ss.ck_mode = st.selectbox("Checksum", CHECKSUM_MODES)
        rx.ck_mode = ss.ck_mode  # listener decodes with the selected mode

        c1, c2 = st.columns(2)
        if c1.button(
            "Connect", disabled=rx.is_open(), use_container_width=True
        ):
            target = manual or port
            try:
                rx.connect(target, baud)
                st.success(f"Opened {target}")
            except serial.SerialException as e:
                st.error(str(e))
        if c2.button(
            "Disconnect",
            disabled=not rx.is_open(),
            use_container_width=True,
        ):
            rx.disconnect()

        st.caption("🟢 Connected" if rx.is_open() else "🔴 Disconnected")
        st.button("Rescan ports")

    # ---- Main layout -----------------------------------------------------
    left, right = st.columns([1, 1])

    with left:
        st.subheader("Commands")
        cols = st.columns(3)
        for i, name in enumerate(CMDS):
            if cols[i % 3].button(
                name, use_container_width=True, key=f"b_{name}"
            ):
                transact(name, rx, ss)

        st.subheader("Host frame fields")
        st.caption(
            "Sent in the state / direction / position / data bytes of each command."
        )
        f1, f2 = st.columns(2)
        ss.f_state = f1.selectbox(
            "State",
            list(STATES),
            format_func=lambda v: f"{STATES[v]} (0x{v:02X})",
            index=0,
        )
        ss.f_dir = f2.selectbox(
            "Direction",
            list(DIRS),
            format_func=lambda v: f"{DIRS[v]} (0x{v:02X})",
        )
        ss.f_pos = f1.number_input("Position (0-255)", 0, 255, 0)
        ss.f_data = f2.number_input("Data (0-255)", 0, 255, 0)
        ss.bad_ck = st.checkbox("Corrupt checksum (negative test)")

        st.subheader("Paper tray")
        st.caption(
            "Prints this many blank pages on the Raspberry Pi (CUPS). "
            "When that job finishes, the app sends the same MOVE_UP frame "
            "as the MOVE_UP button, using the state, direction, position, "
            "and data above. The lead screw raises the stack to the pickup roller."
        )
        pages = int(
            st.number_input(
                "Number of blank pages",
                1,
                MAX_BLANK_PAGES,
                1,
                1,
            )
        )
        printer_names, default_printer, printer_err = list_cups_printers()
        if printer_err:
            st.warning(printer_err)
        if printer_names:
            printer_index = (
                printer_names.index(default_printer)
                if default_printer in printer_names
                else 0
            )
            printer = st.selectbox(
                "Printer",
                printer_names,
                index=printer_index,
                key="tray_printer",
            )
        else:
            printer = st.text_input(
                "Printer queue name",
                key="tray_printer_name",
            )
        tray_buttons = st.columns(2)
        if tray_buttons[0].button(
            "Refresh printers", use_container_width=True
        ):
            list_cups_printers.clear()
            st.rerun()
        raise_tray = tray_buttons[1].button(
            "Print and raise tray",
            type="primary",
            use_container_width=True,
            disabled=get_tray_job().is_running(),
        )
        if raise_tray:
            if not cups_tools_ready():
                st.error(
                    "CUPS was not found. On the Raspberry Pi install "
                    "cups-client, then refresh the printer list."
                )
            elif not rx.is_open():
                st.error(
                    "Connect the actuator first. The tray is raised over "
                    "the serial port after the pages finish."
                )
            elif not str(printer).strip():
                st.error("Choose a printer queue.")
            else:
                get_tray_job().start(
                    rx,
                    pages,
                    str(printer).strip(),
                    int(ss.f_state),
                    int(ss.f_dir),
                    int(ss.f_pos),
                    int(ss.f_data),
                    ss.ck_mode,
                )
        tray_job_status()

        with st.expander("Raw frame"):
            raw_hex = st.text_input("7 hex bytes", "AA 06 00 00 00 00 AC")
            if st.button("Send raw"):
                try:
                    raw = bytes.fromhex(raw_hex)
                    if len(raw) != FRAME_SIZE:
                        st.error("Frame must be exactly 7 bytes.")
                    elif not rx.write(raw, "raw"):
                        st.error("Port not connected.")
                except ValueError:
                    st.error("Invalid hex.")

    @st.fragment(run_every=0.3)
    def status_panel():
        """Refreshes itself. Status = last valid STATUS packet from the listener."""
        with rx.lock:
            s = rx.last_status
            last_status_rx = rx.last_status_rx
            err = rx.error
            passed, failed = rx.ck_pass, rx.ck_fail
            n_status = rx.status_pkts
            lat = list(rx.lat)
            last_lat = rx.last_lat
            per = list(rx.periods)

        if err:
            st.error(err)

        # ---- live actuator status (STATUS packets only)
        if s:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("State", s["state"])
            m2.metric("Direction", s["dir"])
            m3.metric("Position", s["pos"])
            m4.metric("Data", s["data"])
            age = time.monotonic() - last_status_rx
            if not rx.is_open():
                st.info("Disconnected.")
            elif age > 1.0:
                st.warning(
                    f"Status is stale - last STATUS packet {age:.1f} s ago."
                )
            else:
                st.caption(
                    f"Live - last STATUS packet {int(age * 1000)} ms ago "
                    f"(#{n_status})"
                )
        else:
            st.info(
                "Listening - no valid STATUS packet yet."
                if rx.is_open()
                else "Disconnected."
            )

        # ---- checksum analysis
        total = passed + failed
        st.markdown("**Checksum**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Passed", passed)
        c2.metric("Failed", failed)
        c3.metric("Total", total)
        c4.metric(
            "Pass rate",
            f"{100 * passed / total:.1f} %" if total else "-",
        )
        if failed and not passed:
            st.warning(
                "Every frame failed the checksum - check the checksum mode."
            )

        # ---- latency (TX command -> first valid reply echoing that command)
        st.markdown("**Latency (command -> reply)**")
        l1, l2, l3, l4 = st.columns(4)
        l1.metric("Last", _ms(last_lat))
        l2.metric("Average", _ms(_avg(lat)))
        l3.metric("Min", _ms(min(lat)) if lat else "-")
        l4.metric("Max", _ms(max(lat)) if lat else "-")
        st.caption(
            f"{len(lat)} samples. Measured from write() to the first valid reply "
            "with the same command byte; a STATUS frame already streaming may "
            "make STATUS latency read low."
        )

        # ---- STATUS stream timing
        st.markdown("**STATUS stream**")
        p1, p2, p3, p4 = st.columns(4)
        avg_p = _avg(per)
        p1.metric("Period (last)", _ms(per[-1]) if per else "-")
        p2.metric("Period (avg)", _ms(avg_p))
        p3.metric("Period (max)", _ms(max(per)) if per else "-")
        p4.metric("Rate", f"{1 / avg_p:.1f} Hz" if avg_p else "-")

    @st.fragment(run_every=0.3)
    def log_panel():
        with rx.lock:
            rows = list(rx.log)
        if ss.get("hide_rep", True):
            rows = [r for r in rows if not r["repeated"]]
        st.dataframe(
            [
                {k: v for k, v in r.items() if k != "repeated"}
                for r in rows[:200]
            ],
            use_container_width=True,
            height=320,
        )

    with right:
        st.subheader("Actuator status")
        status_panel()
        if st.button("Reset stats"):
            rx.reset_stats()

    st.subheader("Traffic log")
    lc1, lc2 = st.columns([1, 3])
    if lc1.button("Clear log"):
        rx.clear_log()
    lc2.checkbox(
        "Hide repeated identical RX frames", value=True, key="hide_rep"
    )
    log_panel()


if __name__ == "__main__":
    main()