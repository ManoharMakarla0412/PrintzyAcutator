#!/usr/bin/env python3
"""
Actuator Serial Console
Streamlit app for communicating with an actuator over a serial protocol.
"""

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