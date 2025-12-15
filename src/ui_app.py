import asyncio
import json
import os
import sys
import time
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any

from PySide6.QtCore import Qt, QTimer, QThread, Signal, QEvent
from PySide6.QtGui import QIcon, QPixmap, QPainter, QColor
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLabel, QLineEdit, QSpinBox, QCheckBox, QPushButton,
    QTextEdit, QFormLayout, QSystemTrayIcon, QMenu, QMessageBox
)

APP_NAME = "ASS_Timer"
SETTINGS_FILE_NAME = "WebSocketServerASS.settings.json"
LOCK_FILE_NAME_DEFAULT = "WebSocketServerASS.lock"
UI_LOCK_FILE_NAME = "ASS_Timer_UI.lock"

DEFAULTS = {
    "host": "127.0.0.1",
    "port": 5678,
    "send_minutes": True,
    "send_seconds": False,
    "min_interval_min": 1,      # NOT ZERO
    "sec_interval_s": 5,        # MIN 5
    "initial_send_delay_s": 3,
    "ping_interval": 30,
    "ping_timeout": 5,
    "max_queue": 64,
    "close_timeout": 5,
    "status_interval_s": 2,
    "lock_name": LOCK_FILE_NAME_DEFAULT,
    "protocol_version": 1,
}


def get_appdata_dir() -> Path:
    base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
    if base:
        p = Path(base) / APP_NAME
    else:
        p = Path.home() / ".ass_timer"
    p.mkdir(parents=True, exist_ok=True)
    return p


def settings_path() -> Path:
    return get_appdata_dir() / SETTINGS_FILE_NAME


def lock_path(lock_name: str) -> Path:
    return get_appdata_dir() / lock_name


def ui_lock_path() -> Path:
    return get_appdata_dir() / UI_LOCK_FILE_NAME


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            out = subprocess.check_output(
                ["tasklist", "/FI", f"PID eq {pid}"],
                creationflags=creationflags,
            )
            txt = out.decode(errors="ignore")
            return str(pid) in txt
        except Exception:
            return True
    else:
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False


def resource_path(rel: str) -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / rel
    return Path(__file__).resolve().parent / rel


def app_icon_path() -> Path:
    return resource_path("assets/icons/ASS_Timer.ico")


def load_defaults_from_packaged() -> dict:
    p = resource_path("defaults/defaults.ui.json")
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _clamp_settings(data: dict) -> dict:
    try:
        data["min_interval_min"] = max(1, int(data.get("min_interval_min", DEFAULTS["min_interval_min"])))
    except Exception:
        data["min_interval_min"] = DEFAULTS["min_interval_min"]

    try:
        data["sec_interval_s"] = max(5, int(data.get("sec_interval_s", DEFAULTS["sec_interval_s"])))
    except Exception:
        data["sec_interval_s"] = DEFAULTS["sec_interval_s"]

    try:
        data["initial_send_delay_s"] = max(0, int(data.get("initial_send_delay_s", DEFAULTS["initial_send_delay_s"])))
    except Exception:
        data["initial_send_delay_s"] = DEFAULTS["initial_send_delay_s"]

    return data


def load_settings() -> dict:
    data = dict(DEFAULTS)
    data.update(load_defaults_from_packaged())

    p = settings_path()
    try:
        if p.exists():
            file_data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(file_data, dict):
                data.update(file_data)
    except Exception:
        pass

    return _clamp_settings(data)


def save_settings(data: dict) -> Optional[str]:
    data = _clamp_settings(dict(data))
    p = settings_path()
    try:
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return None
    except Exception as e:
        return f"{type(e).__name__}({e})"


def find_server_exe() -> Optional[Path]:
    exe_name = "ASS_WS_Server.exe"

    if getattr(sys, "frozen", False):
        ui_exe = Path(sys.executable).resolve()
        ui_dir = ui_exe.parent

        candidates = [
            ui_dir.parent / "Server" / exe_name,
            ui_dir.parent / "ASS_WS_Server" / exe_name,
            ui_dir / exe_name,
        ]
        for c in candidates:
            if c.exists():
                return c
        return None

    base = Path(__file__).resolve().parent.parent
    candidates = [
        base / "dist" / "ASS_WS_Server" / exe_name,
        base / "ASS_WS_Server" / exe_name,
        Path.cwd() / "dist" / "ASS_WS_Server" / exe_name,
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


class MonitorThread(QThread):
    event_received = Signal(dict)
    disconnected = Signal(str)

    def __init__(self, url: str, parent=None):
        super().__init__(parent)
        self.url = url
        self._stop = False

    def stop(self):
        self._stop = True

    async def _runner(self):
        import websockets

        try:
            async with websockets.connect(self.url) as ws:
                await ws.send(json.dumps({"event": "hello", "role": "ui"}, ensure_ascii=False))

                while not self._stop:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    except asyncio.TimeoutError:
                        continue

                    if not isinstance(msg, str):
                        continue

                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue

                    if isinstance(data, dict):
                        self.event_received.emit(data)

        except Exception as e:
            self.disconnected.emit(str(e))

    def run(self):
        try:
            asyncio.run(self._runner())
        except Exception as e:
            self.disconnected.emit(str(e))


def format_uptime_human(total_s: int) -> str:
    if total_s <= 0:
        return "-"

    s = int(total_s)
    YEAR = 365 * 24 * 3600
    MONTH = 30 * 24 * 3600
    WEEK = 7 * 24 * 3600
    DAY = 24 * 3600
    HOUR = 3600
    MIN = 60

    y, s = divmod(s, YEAR)
    mo, s = divmod(s, MONTH)
    w, s = divmod(s, WEEK)
    d, s = divmod(s, DAY)
    h, s = divmod(s, HOUR)
    m, s = divmod(s, MIN)

    parts = []
    if y:
        parts.append(f"{y}y")
    if mo:
        parts.append(f"{mo}mo")
    if w:
        parts.append(f"{w}w")
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")

    parts.append(f"{m}m")
    parts.append(f"{s}s")

    return " ".join(parts)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ASS Timer UI")

        # Use the app icon for the main window (and taskbar).
        try:
            p = app_icon_path()
            if p.exists():
                self.setWindowIcon(QIcon(str(p)))
        except Exception:
            pass

        self._server_proc: Optional[subprocess.Popen] = None
        self._monitor_thread: Optional[MonitorThread] = None

        self._last_payload_display: str = "-"
        self._uptime_s: int = 0
        self._active_clients = {"total": 0, "ui": 0, "normal": 0}
        self._next_tick_display: str = "-"
        self._last_stale_lock_pid: Optional[int] = None
        self._exiting: bool = False
        self._tray: Optional[QSystemTrayIcon] = None
        self._tray_act_start = None
        self._tray_act_stop = None
        self._ui_lock_acquired: bool = False

        self._settings = load_settings()

        self._build_ui()
        self._apply_settings_to_ui()
        self._setup_tray()

        self.state_timer = QTimer(self)
        self.state_timer.setInterval(1000)
        self.state_timer.timeout.connect(self._tick_server_state)
        self.state_timer.start()

        self._tick_server_state()

    def changeEvent(self, event):
        try:
            if event.type() == QEvent.Type.WindowStateChange:
                if self._tray and self.isMinimized():
                    QTimer.singleShot(0, self.hide)
                    try:
                        self._tray.showMessage(
                            "ASS Timer UI",
                            "The app is minimized to the system tray.",
                            QSystemTrayIcon.Information,
                            1500,
                        )
                    except Exception:
                        pass
        except Exception:
            pass
        super().changeEvent(event)

    def closeEvent(self, event):
        if self._tray and not self._exiting:
            self.hide()
            try:
                self._tray.showMessage(
                    "ASS Timer UI",
                    "The app is in the system tray. Right-click for menu.",
                    QSystemTrayIcon.Information,
                    1500,
                )
            except Exception:
                pass
            event.ignore()
            return

        if self._exiting and self._is_server_running():
            if self._server_proc is not None:
                r = QMessageBox.question(
                    self,
                    "Server is running",
                    "The server is running. Stop it and exit?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                if r == QMessageBox.Yes:
                    try:
                        self._stop_server_clicked()
                    except Exception:
                        pass
                else:
                    event.ignore()
                    return
            else:
                QMessageBox.information(
                    self,
                    "Server is running",
                    "The server was started externally. Stop it first, then exit the app.",
                )
                event.ignore()
                return

            if self._is_server_running():
                QMessageBox.information(
                    self,
                    "Stop failed",
                    "Could not stop the server. The app will remain open.",
                )
                event.ignore()
                return

        try:
            if self._server_proc is not None:
                self._stop_server_clicked()
        except Exception:
            pass

        try:
            if self._ui_lock_acquired:
                lp = ui_lock_path()
                if lp.exists():
                    lp.unlink()
        except Exception:
            pass

        try:
            if self._tray:
                self._tray.hide()
        except Exception:
            pass

        event.accept()
        super().closeEvent(event)
        if self._exiting:
            try:
                app = QApplication.instance()
                if app:
                    QTimer.singleShot(0, app.quit)
            except Exception:
                pass

    def _pid_alive(self, pid: int) -> bool:
        return pid_alive(pid)

    def _is_server_running(self) -> bool:
        lock_name = self.ed_lock_name.text().strip() or LOCK_FILE_NAME_DEFAULT
        lp = lock_path(lock_name)
        if not lp.exists():
            return False
        pid = None
        try:
            data = json.loads(lp.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                pid = int(data.get("pid") or 0)
        except Exception:
            pid = None
        if pid:
            return pid_alive(pid)
        return True

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        status_box = QGroupBox("Status")
        status_layout = QHBoxLayout(status_box)

        self.lbl_server_state = QLabel("Server: stopped")
        self.lbl_server_state.setStyleSheet("font-weight: bold;")

        self.lbl_right_info = QLabel("Clients: -  Uptime: -  Last payload: -")
        self.lbl_right_info.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        status_layout.addWidget(self.lbl_server_state, 1)
        status_layout.addWidget(self.lbl_right_info, 2)

        root.addWidget(status_box)

        settings_box = QGroupBox("Server Settings")
        form = QFormLayout(settings_box)

        self.ed_host = QLineEdit()
        self.sp_port = QSpinBox()
        self.sp_port.setRange(1, 65535)

        self.cb_minutes = QCheckBox("Send minutes")
        self.cb_seconds = QCheckBox("Send seconds")

        self.sp_min_interval = QSpinBox()
        self.sp_min_interval.setRange(1, 999)

        self.sp_sec_interval = QSpinBox()
        self.sp_sec_interval.setRange(5, 999)

        # NEW: initial delay
        self.sp_initial_delay = QSpinBox()
        self.sp_initial_delay.setRange(0, 60)

        self.sp_ping_interval = QSpinBox()
        self.sp_ping_interval.setRange(0, 999)

        self.sp_ping_timeout = QSpinBox()
        self.sp_ping_timeout.setRange(0, 999)

        self.sp_max_queue = QSpinBox()
        self.sp_max_queue.setRange(1, 9999)

        self.sp_close_timeout = QSpinBox()
        self.sp_close_timeout.setRange(0, 999)

        self.sp_status_interval = QSpinBox()
        self.sp_status_interval.setRange(1, 60)

        self.ed_lock_name = QLineEdit()

        def _tt(widget, text: str):
            try:
                widget.setToolTip(text)
            except Exception:
                pass

        lb_host = QLabel("HOST:")
        lb_port = QLabel("PORT:")
        lb_min_interval = QLabel("MIN_INTERVAL_MIN:")
        lb_sec_interval = QLabel("SEC_INTERVAL_S:")
        lb_initial_delay = QLabel("INITIAL_SEND_DELAY_S:")
        lb_ping_interval = QLabel("PING_INTERVAL:")
        lb_ping_timeout = QLabel("PING_TIMEOUT:")
        lb_max_queue = QLabel("MAX_QUEUE:")
        lb_close_timeout = QLabel("CLOSE_TIMEOUT:")
        lb_status_interval = QLabel("STATUS_INTERVAL_S:")
        lb_lock_name = QLabel("LOCK_NAME:")

        form.addRow(lb_host, self.ed_host)
        form.addRow(lb_port, self.sp_port)
        form.addRow("", self.cb_minutes)
        form.addRow("", self.cb_seconds)
        form.addRow(lb_min_interval, self.sp_min_interval)
        form.addRow(lb_sec_interval, self.sp_sec_interval)
        form.addRow(lb_initial_delay, self.sp_initial_delay)
        form.addRow(lb_ping_interval, self.sp_ping_interval)
        form.addRow(lb_ping_timeout, self.sp_ping_timeout)
        form.addRow(lb_max_queue, self.sp_max_queue)
        form.addRow(lb_close_timeout, self.sp_close_timeout)
        form.addRow(lb_status_interval, self.sp_status_interval)
        form.addRow(lb_lock_name, self.ed_lock_name)

        host_tt = (
            "IP address the server will listen on.\n"
            "Use 127.0.0.1 for the same PC, or your LAN IP to allow other devices."
        )
        port_tt = (
            "TCP port for the server.\n"
            "Leave 5678 unless something else is using it."
        )
        minutes_tt = "When enabled, the server sends the current minute number (0-59) to connected clients."
        seconds_tt = (
            "When enabled, the server sends the current second number (0-59) to connected clients.\n"
            "Minimum interval is 5 seconds."
        )
        min_interval_tt = (
            "How often to send minutes after the first aligned tick.\n"
            "Example: 5 = every 5 minutes, on :00."
        )
        sec_interval_tt = (
            "How often to send seconds after the first aligned tick.\n"
            "Example: 10 = every 10 seconds.\n"
            "Minimum is 5."
        )
        initial_delay_tt = (
            "Initial delay (seconds) before the first number is sent.\n"
            "This helps clients that need a short time to get ready after connecting."
        )
        ping_interval_tt = (
            "WebSocket keep-alive interval (seconds).\n"
            "0 disables pings.\n"
            "Usually leave default."
        )
        ping_timeout_tt = "How long to wait for a ping response (seconds) before considering the connection lost."
        max_queue_tt = "Maximum number of queued messages per client.\nUsually leave default."
        close_timeout_tt = "How long to wait for a clean WebSocket close (seconds) before forcing the connection closed."
        status_interval_tt = "How often the UI updates server status (seconds).\nThis does not change the sending intervals."
        lock_name_tt = (
            "Lock file name used to detect if the server is already running.\n"
            "If the app crashes and the server will not start, delete the lock file in %LOCALAPPDATA%\\ASS_Timer."
        )

        _tt(lb_host, host_tt)
        _tt(self.ed_host, host_tt)
        _tt(lb_port, port_tt)
        _tt(self.sp_port, port_tt)
        _tt(self.cb_minutes, minutes_tt)
        _tt(self.cb_seconds, seconds_tt)
        _tt(lb_min_interval, min_interval_tt)
        _tt(self.sp_min_interval, min_interval_tt)
        _tt(lb_sec_interval, sec_interval_tt)
        _tt(self.sp_sec_interval, sec_interval_tt)
        _tt(lb_initial_delay, initial_delay_tt)
        _tt(self.sp_initial_delay, initial_delay_tt)
        _tt(lb_ping_interval, ping_interval_tt)
        _tt(self.sp_ping_interval, ping_interval_tt)
        _tt(lb_ping_timeout, ping_timeout_tt)
        _tt(self.sp_ping_timeout, ping_timeout_tt)
        _tt(lb_max_queue, max_queue_tt)
        _tt(self.sp_max_queue, max_queue_tt)
        _tt(lb_close_timeout, close_timeout_tt)
        _tt(self.sp_close_timeout, close_timeout_tt)
        _tt(lb_status_interval, status_interval_tt)
        _tt(self.sp_status_interval, status_interval_tt)
        _tt(lb_lock_name, lock_name_tt)
        _tt(self.ed_lock_name, lock_name_tt)

        root.addWidget(settings_box)

        self._server_settings_widgets = [
            self.ed_host,
            self.sp_port,
            self.cb_minutes,
            self.cb_seconds,
            self.sp_min_interval,
            self.sp_sec_interval,
            self.sp_initial_delay,
            self.sp_ping_interval,
            self.sp_ping_timeout,
            self.sp_max_queue,
            self.sp_close_timeout,
            self.sp_status_interval,
            self.ed_lock_name,
        ]

        btn_row = QHBoxLayout()
        self.btn_start = QPushButton("Start")
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setEnabled(False)

        self.btn_start.clicked.connect(self._start_server_clicked)
        self.btn_stop.clicked.connect(self._stop_server_clicked)

        _tt(self.btn_start, "Start the server using the settings above.")
        _tt(self.btn_stop, "Stop the server started by this UI.")

        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_stop)
        btn_row.addStretch(1)

        root.addLayout(btn_row)

        log_box = QGroupBox("Log (info)")
        log_layout = QVBoxLayout(log_box)
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        log_layout.addWidget(self.txt_log)
        root.addWidget(log_box, 1)

    def _log(self, msg: str, tag: str = "UI", replace_last: bool = False):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} [{tag}] {msg}"

        if replace_last:
            try:
                cursor = self.txt_log.textCursor()
                cursor.movePosition(cursor.MoveOperation.End)
                cursor.movePosition(cursor.MoveOperation.StartOfBlock, cursor.MoveMode.KeepAnchor)
                last_line = cursor.selectedText()
                if tag == "EV" and "[EV]" in last_line and ("status" in last_line or "payload_sent" in last_line):
                    cursor.removeSelectedText()
                    cursor.insertText(line)
                    self.txt_log.setTextCursor(cursor)
                    return
            except Exception:
                pass

        self.txt_log.append(line)

    def _setup_tray(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return

        self._tray = QSystemTrayIcon(self)
        self._tray.setToolTip("ASS Timer UI")
        self._tray.activated.connect(self._on_tray_activated)
        # Set an initial icon before showing, to avoid Qt warnings.
        try:
            self._tray.setIcon(self._make_tray_icon(False))
        except Exception:
            pass

        menu = QMenu()
        act_show = menu.addAction("Show/Hide")
        act_show.triggered.connect(self._toggle_visible_from_tray)

        menu.addSeparator()
        self._tray_act_start = menu.addAction("Start server")
        self._tray_act_start.triggered.connect(self._start_server_clicked)
        self._tray_act_stop = menu.addAction("Stop server")
        self._tray_act_stop.triggered.connect(self._stop_server_clicked)

        menu.addSeparator()
        act_exit = menu.addAction("Exit")
        act_exit.triggered.connect(self._exit_from_tray)

        self._tray.setContextMenu(menu)
        self._tray.show()

    def _on_tray_activated(self, reason):
        if reason == QSystemTrayIcon.Trigger:
            self._toggle_visible_from_tray()

    def _toggle_visible_from_tray(self):
        if self.isVisible():
            self.hide()
        else:
            self.showNormal()
            self.raise_()
            self.activateWindow()

    def _exit_from_tray(self):
        self._exiting = True
        self.close()

    def _make_tray_icon(self, running: bool) -> QIcon:
        size = 16
        pix = QPixmap(size, size)
        pix.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pix)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        color = QColor(0, 200, 0) if running else QColor(200, 0, 0)
        painter.setBrush(color)
        painter.setPen(Qt.GlobalColor.black)
        painter.drawEllipse(1, 1, size - 2, size - 2)
        painter.end()
        return QIcon(pix)

    def _collect_settings_from_ui(self) -> dict:
        data = dict(self._settings)

        data["host"] = self.ed_host.text().strip() or DEFAULTS["host"]
        data["port"] = int(self.sp_port.value())

        data["send_minutes"] = self.cb_minutes.isChecked()
        data["send_seconds"] = self.cb_seconds.isChecked()

        data["min_interval_min"] = max(1, int(self.sp_min_interval.value()))
        data["sec_interval_s"] = max(5, int(self.sp_sec_interval.value()))

        data["initial_send_delay_s"] = max(0, int(self.sp_initial_delay.value()))

        data["ping_interval"] = int(self.sp_ping_interval.value())
        data["ping_timeout"] = int(self.sp_ping_timeout.value())
        data["max_queue"] = int(self.sp_max_queue.value())
        data["close_timeout"] = int(self.sp_close_timeout.value())

        data["status_interval_s"] = int(self.sp_status_interval.value())

        lock_name = self.ed_lock_name.text().strip() or LOCK_FILE_NAME_DEFAULT
        data["lock_name"] = lock_name

        return _clamp_settings(data)

    def _apply_settings_to_ui(self):
        s = _clamp_settings(dict(self._settings))

        self.ed_host.setText(str(s.get("host", DEFAULTS["host"])))
        self.sp_port.setValue(int(s.get("port", DEFAULTS["port"])))

        self.cb_minutes.setChecked(bool(s.get("send_minutes", True)))
        self.cb_seconds.setChecked(bool(s.get("send_seconds", False)))

        self.sp_min_interval.setValue(int(s.get("min_interval_min", 1)))
        self.sp_sec_interval.setValue(int(s.get("sec_interval_s", 5)))

        self.sp_initial_delay.setValue(int(s.get("initial_send_delay_s", 0)))

        self.sp_ping_interval.setValue(int(s.get("ping_interval", 30)))
        self.sp_ping_timeout.setValue(int(s.get("ping_timeout", 5)))
        self.sp_max_queue.setValue(int(s.get("max_queue", 64)))
        self.sp_close_timeout.setValue(int(s.get("close_timeout", 5)))

        self.sp_status_interval.setValue(int(s.get("status_interval_s", 2)))

        self.ed_lock_name.setText(str(s.get("lock_name", LOCK_FILE_NAME_DEFAULT)))

    def _save_settings_now(self):
        self._settings = self._collect_settings_from_ui()
        err = save_settings(self._settings)
        if err:
            self._log(f"Failed to save settings: {err}")
        else:
            self._log(f"Saved settings: {settings_path()}")

    def _server_url(self) -> str:
        host = self.ed_host.text().strip() or DEFAULTS["host"]
        port = int(self.sp_port.value())
        return f"ws://{host}:{port}"

    def _tick_server_state(self):
        lock_name = self.ed_lock_name.text().strip() or LOCK_FILE_NAME_DEFAULT
        lp = lock_path(lock_name)
        running = lp.exists()

        if running:
            pid = None
            try:
                data = json.loads(lp.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    pid = int(data.get("pid") or 0)
            except Exception:
                pid = None

            if pid and not self._pid_alive(pid):
                try:
                    lp.unlink()
                    running = False
                    if self._last_stale_lock_pid != pid:
                        self._log(f"Found stale lock (pid={pid}); deleted it.")
                        self._last_stale_lock_pid = pid
                except Exception as e:
                    self._log(f"Could not delete stale lock: {e}")
            elif pid:
                self._last_stale_lock_pid = None

        if running:
            origin = "started by UI" if self._server_proc else "external"
            self.lbl_server_state.setText(f"Server: running ({origin})")
            self.btn_start.setEnabled(False)
            self.btn_stop.setEnabled(True)

            if self._monitor_thread is None:
                self._start_monitor()
        else:
            self.lbl_server_state.setText("Server: stopped")
            self.btn_start.setEnabled(True)
            self.btn_stop.setEnabled(False)

            if self._monitor_thread is not None:
                self._stop_monitor(silent=True)

        # Keep tray menu actions consistent with server state.
        try:
            if self._tray_act_start is not None:
                self._tray_act_start.setEnabled(not running)
            if self._tray_act_stop is not None:
                self._tray_act_stop.setEnabled(running)
        except Exception:
            pass

        try:
            for w in getattr(self, "_server_settings_widgets", []):
                w.setEnabled(not running)
        except Exception:
            pass

        if self._tray:
            try:
                self._tray.setIcon(self._make_tray_icon(running))
            except Exception:
                pass

        self._update_right_info()

    def _start_monitor(self):
        url = self._server_url()
        self._log(f"Connecting to {url} ...")
        self._monitor_thread = MonitorThread(url)
        self._monitor_thread.event_received.connect(self._on_monitor_event)
        self._monitor_thread.disconnected.connect(self._on_monitor_disconnected)
        self._monitor_thread.start()

    def _stop_monitor(self, silent: bool = False):
        if self._monitor_thread:
            try:
                self._monitor_thread.stop()
            except Exception:
                pass
            try:
                self._monitor_thread.wait(500)
            except Exception:
                pass
            self._monitor_thread = None
        if not silent:
            self._log("Monitor disconnected.")

    def _on_monitor_disconnected(self, reason: str):
        self._log(f"Monitor disconnect: {reason}")
        self._monitor_thread = None
        self._update_right_info()

    def _on_monitor_event(self, data: dict):
        ev = data.get("event")

        if ev == "role_ack":
            self._log("Connected as monitor client.")
            return

        if "active_clients" in data and isinstance(data["active_clients"], dict):
            self._active_clients = {
                "total": int(data["active_clients"].get("total", 0)),
                "ui": int(data["active_clients"].get("ui", 0)),
                "normal": int(data["active_clients"].get("normal", 0)),
            }

        if "uptime_s" in data:
            try:
                self._uptime_s = int(data["uptime_s"])
            except Exception:
                pass

        # Optional diagnostics: next scheduled tick (server-side schedule).
        try:
            nxt_min = int(data["next_minute_tick_ts"]) if "next_minute_tick_ts" in data else None
            nxt_sec = int(data["next_second_tick_ts"]) if "next_second_tick_ts" in data else None

            parts = []
            if nxt_min:
                parts.append("M:" + time.strftime("%H:%M:%S", time.localtime(nxt_min)))
            if nxt_sec:
                parts.append("S:" + time.strftime("%H:%M:%S", time.localtime(nxt_sec)))

            self._next_tick_display = " ".join(parts) if parts else "-"
        except Exception:
            self._next_tick_display = "-"

        if "last_payload" in data and isinstance(data["last_payload"], dict):
            self._last_payload_display = self._format_payload_for_header(data["last_payload"])

        if ev == "payload_sent" and isinstance(data.get("payload"), dict):
            self._last_payload_display = self._format_payload_for_header(data["payload"])
            self._log(f"payload_sent {data['payload']}", tag="EV", replace_last=True)
        elif ev == "init_sent":
            self._log(
                f"init_sent delay_s={data.get('delay_s')} sent_msgs={data.get('sent_msgs')} texts={data.get('texts')}",
                tag="EV",
            )
        elif ev in ("client_connected", "client_disconnected"):
            role = data.get("role")
            client_id = data.get("client_id")
            remote = data.get("remote")
            self._log(
                f"{ev} role={role} client_id={client_id} remote={remote} active={self._active_clients}",
                tag="EV",
            )
        elif ev == "status":
            self._log(f"{ev} active={self._active_clients}", tag="EV", replace_last=True)

        self._update_right_info()

    def _format_payload_for_header(self, payload: Dict[str, Any]) -> str:
        if "minutes" in payload:
            return str(payload.get("minutes"))
        if "seconds" in payload:
            return str(payload.get("seconds"))
        return json.dumps(payload, ensure_ascii=False)

    def _update_right_info(self):
        ac = self._active_clients
        clients_str = f"total={ac.get('total', 0)}, normal={ac.get('normal', 0)}, ui={ac.get('ui', 0)}"
        uptime_str = format_uptime_human(self._uptime_s) if self._uptime_s else "-"
        last_str = self._last_payload_display or "-"
        self.lbl_right_info.setText(
            f"Clients: {clients_str}  Uptime: {uptime_str}  Last payload: {last_str}  Next tick: {self._next_tick_display}"
        )

    def _start_server_clicked(self):
        self._save_settings_now()

        if not self._settings.get("send_minutes", False) and not self._settings.get("send_seconds", False):
            QMessageBox.information(
                self,
                "Nothing to send",
                "Enable at least one option: 'Send minutes' or 'Send seconds'.",
            )
            return

        exe = find_server_exe()
        if not exe:
            self._log("✖ Could not find ASS_WS_Server.exe.")
            return

        lock_name = self._settings.get("lock_name", LOCK_FILE_NAME_DEFAULT)
        if lock_path(lock_name).exists():
            self._log("The server appears to be running already (lock found).")
            self._tick_server_state()
            return

        args = [
            str(exe),
            "--settings", str(settings_path()),
            "--host", str(self._settings.get("host", DEFAULTS["host"])),
            "--port", str(self._settings.get("port", DEFAULTS["port"])),
        ]

        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        try:
            self._server_proc = subprocess.Popen(
                args,
                cwd=str(exe.parent),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags
            )
            try:
                mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(exe.stat().st_mtime))
            except Exception:
                mtime = "?"
            self._log(f"Starting server (exe): {exe} (mtime={mtime})")
        except Exception as e:
            self._server_proc = None
            self._log(f"Failed to start server: {e}")

        self._tick_server_state()

    def _stop_server_clicked(self):
        if self._server_proc:
            try:
                self._server_proc.terminate()
                self._log("Stopping server (process terminate).")
                try:
                    self._server_proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    try:
                        self._server_proc.kill()
                        self._log("Process did not stop in time; kill().")
                        self._server_proc.wait(timeout=1.0)
                    except Exception:
                        pass
            except Exception as e:
                self._log(f"Failed to stop server: {e}")

            self._server_proc = None

            lock_name = self.ed_lock_name.text().strip() or LOCK_FILE_NAME_DEFAULT
            try:
                lp = lock_path(lock_name)
                if lp.exists():
                    lp.unlink()
                    self._log("Cleaned lock file.")
            except Exception as e:
                self._log(f"Failed to delete lock file: {e}")
        else:
            self._log("The server was not started by UI.")

        self._tick_server_state()


def main():
    app = QApplication(sys.argv)

    # Set global application icon (window/taskbar).
    try:
        p = app_icon_path()
        if p.exists():
            app.setWindowIcon(QIcon(str(p)))
    except Exception:
        pass

    lp = ui_lock_path()
    if lp.exists():
        pid = None
        try:
            data = json.loads(lp.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                pid = int(data.get("pid") or 0)
        except Exception:
            pid = None

        if pid and pid_alive(pid):
            QMessageBox.information(
                None,
                "ASS Timer UI",
                "The application is already running.",
            )
            sys.exit(0)
        else:
            try:
                lp.unlink()
            except Exception:
                pass

    w = MainWindow()

    try:
        lp.write_text(json.dumps({"pid": os.getpid()}, ensure_ascii=False), encoding="utf-8")
        w._ui_lock_acquired = True
    except Exception:
        w._ui_lock_acquired = False

    w.resize(900, 700)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
