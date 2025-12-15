import asyncio
import json
import os
import sys
import time
import signal
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Set, Dict, Any

import websockets
from websockets.server import WebSocketServerProtocol

APP_NAME = "ASS_Timer"
SETTINGS_FILE_NAME = "WebSocketServerASS.settings.json"
LOCK_FILE_NAME_DEFAULT = "WebSocketServerASS.lock"
STOP_HELP_BAT = "ASS_Server_timerStop.bat"

# If UI doesn't send hello quickly, assume a normal client.
HELLO_TIMEOUT_S = 0.5


def get_appdata_dir() -> Path:
    base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
    if base:
        p = Path(base) / APP_NAME
    else:
        p = Path.home() / ".ass_timer"
    p.mkdir(parents=True, exist_ok=True)
    return p


def default_settings_path() -> Path:
    return get_appdata_dir() / SETTINGS_FILE_NAME


def default_lock_path(lock_name: str) -> Path:
    return get_appdata_dir() / lock_name


def resource_path(rel: str) -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / rel
    return Path(__file__).resolve().parent / rel


def load_json_file(p: Path) -> Optional[dict]:
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 5678

    send_minutes: bool = True
    send_seconds: bool = False

    min_interval_min: int = 1   # MUST NOT be 0
    sec_interval_s: int = 5     # MUST NOT be 0

    # Initial send delay for the first normal client after server start (seconds)
    initial_send_delay_s: int = 3

    ping_interval: int = 30
    ping_timeout: int = 5
    max_queue: int = 64
    close_timeout: int = 5

    status_interval_s: int = 2

    lock_name: str = LOCK_FILE_NAME_DEFAULT
    protocol_version: int = 1


def load_defaults_from_packaged() -> dict:
    p = resource_path("defaults/defaults.settings.json")
    data = load_json_file(p)
    return data or {}


def merge_config(base: ServerConfig, override: dict) -> ServerConfig:
    d = asdict(base)

    override_norm: Dict[str, Any] = {}
    for k, v in override.items():
        if isinstance(k, str):
            override_norm[k.lower()] = v

    for k, v in override_norm.items():
        if k in d:
            d[k] = v

    try:
        d["port"] = int(d["port"])
    except Exception:
        d["port"] = base.port

    for key in (
        "min_interval_min",
        "sec_interval_s",
        "initial_send_delay_s",
        "ping_interval",
        "ping_timeout",
        "max_queue",
        "close_timeout",
        "status_interval_s",
    ):
        try:
            d[key] = int(d[key])
        except Exception:
            d[key] = getattr(base, key)

    # enforce NOT ZERO for intervals
    try:
        d["min_interval_min"] = max(1, int(d["min_interval_min"]))
    except Exception:
        d["min_interval_min"] = base.min_interval_min

    try:
        d["sec_interval_s"] = max(5, int(d["sec_interval_s"]))
    except Exception:
        d["sec_interval_s"] = base.sec_interval_s

    # allow 0+ for initial delay, but clamp to sane range
    try:
        d["initial_send_delay_s"] = max(0, int(d["initial_send_delay_s"]))
    except Exception:
        d["initial_send_delay_s"] = base.initial_send_delay_s

    for key in ("send_minutes", "send_seconds"):
        d[key] = bool(d[key])

    return ServerConfig(**d)


def load_config(settings_path: Optional[Path]) -> ServerConfig:
    cfg = ServerConfig()
    cfg = merge_config(cfg, load_defaults_from_packaged())

    if settings_path is None:
        settings_path = default_settings_path()

    file_data = load_json_file(settings_path)
    if file_data:
        cfg = merge_config(cfg, file_data)

    return cfg


class ClientInfo:
    __slots__ = ("ws", "role", "connected_at", "id", "remote", "_pending_send_task")

    def __init__(self, ws: WebSocketServerProtocol):
        self.ws = ws
        self.role = "pending"  # pending|normal|ui
        self.connected_at = time.time()
        self.id = f"c{int(self.connected_at*1000)}_{id(self)}"
        self.remote = getattr(ws, "remote_address", None)
        self._pending_send_task: Optional[asyncio.Task] = None


class ASSWSServer:
    def __init__(self, settings_path: Optional[Path] = None):
        self.settings_path = settings_path or default_settings_path()
        self.config = load_config(self.settings_path)

        self.clients: Set[ClientInfo] = set()
        self._server = None
        self._stop = asyncio.Event()
        self._start_ts = time.time()
        self._start_dt = datetime.now()

        self.last_payload: Optional[dict] = None
        self.lock_path = default_lock_path(self.config.lock_name)
        self.log = logging.getLogger("ASS_WS_Server")

        # Interval loops run only if there is at least 1 normal client.
        self._normal_ready = asyncio.Event()

        # Initial send tasks per client
        self._init_tasks: Dict[ClientInfo, asyncio.Task] = {}

        # Deduplicate payload sends (prevents bursty repeats due to edge timing).
        # Use epoch-based buckets so intervals like 60m/60s don't get incorrectly dropped.
        self._last_sent_minute_bucket: Optional[int] = None  # int(time.time() // 60)
        self._last_sent_second_bucket: Optional[int] = None  # int(time.time())

        # Apply initial_send_delay_s only once per server run, for the first normal client
        # that successfully receives the initial push.
        self._initial_delay_consumed: bool = False
        self._initial_delay_lock = asyncio.Lock()

        # Next scheduled ticks (for UI diagnostics).
        self._next_minute_tick: Optional[datetime] = None
        self._next_second_tick: Optional[datetime] = None

        # Tick anchor:
        # Start periodic ticking only AFTER the first initial push to a normal client.
        # This prevents seconds ticks from firing before INITIAL_SEND_DELAY_S completes.
        # When there are no normal clients, the anchor is cleared and will be re-created
        # on the next initial push for a new normal session.
        self._tick_anchor_dt: Optional[datetime] = None
        self._tick_anchor_ready = asyncio.Event()

    def uptime_s(self) -> int:
        return int(time.time() - self._start_ts)

    def active_counts(self) -> Dict[str, int]:
        ui = sum(1 for c in self.clients if c.role == "ui")
        normal = sum(1 for c in self.clients if c.role == "normal")
        pending = sum(1 for c in self.clients if c.role == "pending")
        total = ui + normal + pending
        return {"total": total, "ui": ui, "normal": normal, "pending": pending}

    def _recompute_normal_ready(self):
        if any(c.role == "normal" for c in self.clients):
            self._normal_ready.set()
        else:
            self._normal_ready.clear()
            self._tick_anchor_dt = None
            self._tick_anchor_ready.clear()

    async def _wait_for_normal_client(self):
        while not self._stop.is_set():
            if self._normal_ready.is_set():
                return
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                continue

    async def _wait_for_tick_anchor(self) -> Optional[datetime]:
        while not self._stop.is_set():
            if not self._normal_ready.is_set():
                await self._wait_for_normal_client()
                if self._stop.is_set():
                    return None

            if self._tick_anchor_ready.is_set() and self._tick_anchor_dt is not None:
                return self._tick_anchor_dt

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                continue
        return None

    async def send_to_one(self, c: ClientInfo, msg: dict):
        try:
            await asyncio.wait_for(
                c.ws.send(json.dumps(msg, ensure_ascii=False)),
                timeout=1.0,
            )
        except Exception:
            pass

    async def broadcast_ui_event(self, event: str, extra: Optional[dict] = None):
        counts = self.active_counts()
        msg = {
            "event": event,
            "active_clients": counts,
            "uptime_s": self.uptime_s(),
            "cfg_send_minutes": bool(self.config.send_minutes),
            "cfg_send_seconds": bool(self.config.send_seconds),
            "cfg_min_interval_min": int(self.config.min_interval_min),
            "cfg_sec_interval_s": int(self.config.sec_interval_s),
        }
        if self.config.send_minutes and self._next_minute_tick is not None:
            try:
                msg["next_minute_tick_ts"] = int(self._next_minute_tick.timestamp())
            except Exception:
                pass
        if self.config.send_seconds and self._next_second_tick is not None:
            try:
                msg["next_second_tick_ts"] = int(self._next_second_tick.timestamp())
            except Exception:
                pass
        if self.last_payload is not None:
            msg["last_payload"] = self.last_payload
        if extra:
            msg.update(extra)

        # Fire-and-forget UI notifications.
        # The UI is not part of the timing-critical path; if it is slow (or not reading),
        # we must not block minute/second scheduling.
        for c in list(self.clients):
            if c.role == "ui":
                try:
                    asyncio.create_task(self.send_to_one(c, msg))
                except Exception:
                    pass

    @staticmethod
    def _payload_to_text(payload: dict) -> str:
        if (
            isinstance(payload, dict)
            and len(payload) == 1
            and ("minutes" in payload or "seconds" in payload)
        ):
            value = payload.get("minutes") if "minutes" in payload else payload.get("seconds")
            return str(value)
        return json.dumps(payload, ensure_ascii=False)

    async def _send_text_to_client(self, c: ClientInfo, text: str) -> bool:
        try:
            await asyncio.wait_for(c.ws.send(text), timeout=1.0)
            return True
        except Exception:
            return False

    def _schedule_text_send_latest_only(self, c: ClientInfo, text: str) -> None:
        """
        Sending to clients must not block tick scheduling.
        For high-frequency updates (1s), we keep only the latest pending send per client.
        """
        try:
            t = getattr(c, "_pending_send_task", None)
            if t is not None and not t.done():
                try:
                    t.cancel()
                except Exception:
                    pass
            c._pending_send_task = asyncio.create_task(self._send_text_to_client(c, text))
        except Exception:
            pass

    async def broadcast_payload(self, payload: dict, tick_dt: Optional[datetime] = None):
        # This is only for interval ticks to all normal clients.
        self.last_payload = payload

        # De-dupe repeated payloads (can happen around clock boundaries / reschedules).
        try:
            if isinstance(payload, dict) and "minutes" in payload:
                bucket = int((tick_dt.timestamp() if tick_dt is not None else time.time()) // 60)
                if self._last_sent_minute_bucket == bucket:
                    return
                self._last_sent_minute_bucket = bucket
            if isinstance(payload, dict) and "seconds" in payload:
                bucket = int(tick_dt.timestamp() if tick_dt is not None else time.time())
                if self._last_sent_second_bucket == bucket:
                    return
                self._last_sent_second_bucket = bucket
        except Exception:
            pass

        txt = self._payload_to_text(payload)

        for c in list(self.clients):
            if c.role == "normal":
                self._schedule_text_send_latest_only(c, txt)

        await self.broadcast_ui_event("payload_sent", {"payload": payload})

    async def handle_role_hello(self, c: ClientInfo, raw: str):
        try:
            data = json.loads(raw)
        except Exception:
            return

        role = data.get("role")
        old_role = c.role

        if role == "ui":
            c.role = "ui"
            await self.send_to_one(c, {"event": "role_ack", "role": "ui"})
        elif role == "normal":
            c.role = "normal"
            await self.send_to_one(c, {"event": "role_ack", "role": "normal"})

        if c.role != old_role:
            self._recompute_normal_ready()
            await self.broadcast_ui_event("role_changed", {"old_role": old_role, "role": c.role})

    async def _initial_send_after_delay(
        self,
        c: ClientInfo,
        ready_event: Optional[asyncio.Event] = None,
        delay_override_s: Optional[int] = None,
    ) -> int:
        """
        Initial send behavior:
        - When a normal client connects, wait N seconds (default 3)
        - Then send the current value (minutes/seconds depending on config) to THIS client
        - Send only once (no burst)

        NOTE: Some clients (OBS plugins) may not be ready to process messages immediately.
        If ready_event is provided, we wait for the first inbound message (up to N seconds)
        but we still keep the total delay at ~N seconds from scheduling.
        """
        delay = max(0, int(self.config.initial_send_delay_s if delay_override_s is None else delay_override_s))
        try:
            started_at = asyncio.get_running_loop().time()
            if ready_event is not None and delay > 0:
                try:
                    await asyncio.wait_for(ready_event.wait(), timeout=float(delay))
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    pass

            if delay > 0:
                elapsed = asyncio.get_running_loop().time() - started_at
                remaining = max(0.0, float(delay) - float(elapsed))
                if remaining:
                    await asyncio.sleep(remaining)

            if self._stop.is_set() or c.role != "normal":
                return

            payloads = []
            now = datetime.now()
            if self.config.send_minutes:
                payloads.append({"minutes": int(now.minute)})
            if self.config.send_seconds:
                payloads.append({"seconds": int(now.second)})

            if not payloads:
                return

            # Some generic WS clients may drop the first message while initializing.
            # Try a few times (small, bounded) to increase reliability without flooding.
            max_attempts = 3
            attempt_sleep_s = 0.5

            sent = 0
            last_texts = []
            for p in payloads:
                txt = self._payload_to_text(p)
                last_texts.append(txt)
                ok_any = False
                for _ in range(max_attempts):
                    ok = await self._send_text_to_client(c, txt)
                    if ok:
                        ok_any = True
                        break
                    await asyncio.sleep(attempt_sleep_s)

                if ok_any:
                    sent += 1
                    self.last_payload = p

            if sent > 0 and not self._tick_anchor_ready.is_set():
                # Establish the tick anchor (for minutes/seconds loops) only after
                # the first initial push has been sent to a normal client.
                self._tick_anchor_dt = now
                self._tick_anchor_ready.set()

            await self.broadcast_ui_event(
                "init_sent",
                {
                    "client_id": c.id,
                    "delay_s": delay,
                    "sent_msgs": sent,
                    "payloads": payloads,
                    "texts": last_texts,
                },
            )
            return sent
        except Exception:
            return 0

    async def _schedule_initial_send_for_client(self, c: ClientInfo, ready_event: asyncio.Event):
        delay = 0
        async with self._initial_delay_lock:
            if not self._initial_delay_consumed:
                delay = max(0, int(self.config.initial_send_delay_s))

        sent = await self._initial_send_after_delay(c, ready_event=ready_event, delay_override_s=delay)

        if delay > 0 and sent > 0:
            async with self._initial_delay_lock:
                self._initial_delay_consumed = True

    async def conn_handler(self, ws: WebSocketServerProtocol):
        c = ClientInfo(ws)
        self.clients.add(c)

        try:
            role_resolved = asyncio.Event()
            connected_broadcasted = False
            timeout_task: Optional[asyncio.Task] = None
            announce_task: Optional[asyncio.Task] = None
            first_rx_event = asyncio.Event()

            async def resolve_role_on_timeout():
                try:
                    await asyncio.sleep(HELLO_TIMEOUT_S)
                    if self._stop.is_set():
                        return
                    if c.role == "pending":
                        c.role = "normal"
                    role_resolved.set()
                except Exception:
                    role_resolved.set()

            async def announce_connected_when_ready():
                nonlocal connected_broadcasted
                await role_resolved.wait()
                if self._stop.is_set():
                    return
                if connected_broadcasted:
                    return
                connected_broadcasted = True

                self._recompute_normal_ready()
                await self.broadcast_ui_event(
                    "client_connected",
                    {"role": c.role, "client_id": c.id, "remote": c.remote},
                )

                if c.role == "normal":
                    t = asyncio.create_task(self._schedule_initial_send_for_client(c, ready_event=first_rx_event))
                    self._init_tasks[c] = t

            timeout_task = asyncio.create_task(resolve_role_on_timeout())
            announce_task = asyncio.create_task(announce_connected_when_ready())

            async for message in ws:
                if not first_rx_event.is_set():
                    first_rx_event.set()
                if isinstance(message, str):
                    old_role = c.role
                    await self.handle_role_hello(c, message)
                    if c.role != old_role and c.role != "pending":
                        role_resolved.set()

        except Exception:
            pass
        finally:
            try:
                if timeout_task:
                    timeout_task.cancel()
            except Exception:
                pass

            try:
                if announce_task:
                    announce_task.cancel()
            except Exception:
                pass

            try:
                t = self._init_tasks.pop(c, None)
                if t:
                    t.cancel()
            except Exception:
                pass

            try:
                pt = getattr(c, "_pending_send_task", None)
                if pt:
                    pt.cancel()
            except Exception:
                pass

            if c in self.clients:
                self.clients.remove(c)
            self._recompute_normal_ready()
            await self.broadcast_ui_event(
                "client_disconnected",
                {"role": c.role, "client_id": c.id, "remote": c.remote},
            )

    async def _sleep_or_stop(self, seconds: float):
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _sleep_until_dt_with_normal_check(self, target: datetime) -> bool:
        while not self._stop.is_set():
            if not self._normal_ready.is_set():
                return False
            now = datetime.now()
            remaining = (target - now).total_seconds()
            if remaining <= 0:
                return True
            # Finer granularity for short (seconds) schedules, to reduce drift/skips.
            step = 0.5
            if remaining <= 2.0:
                step = 0.05
            await self._sleep_or_stop(min(step, remaining))
        return False

    @staticmethod
    def _next_minute_boundary(after: datetime) -> datetime:
        base = after.replace(second=0, microsecond=0)
        return base + timedelta(minutes=1)

    @staticmethod
    def _next_second_boundary(after: datetime) -> datetime:
        base = after.replace(microsecond=0)
        return base + timedelta(seconds=1)

    async def minute_loop(self):
        """
        Interval ticks (minutes):
        - Waits for a normal client
        - First aligned tick is at the first :00 boundary after the initial push (B)
        - Then every interval minutes from B, always at :00
        """
        while not self._stop.is_set():
            anchor = await self._wait_for_tick_anchor()
            if self._stop.is_set():
                break
            if anchor is None:
                continue

            interval_min = max(1, int(self.config.min_interval_min))

            # Anchor schedule to the initial push time to ensure the first :00 tick is
            # always AFTER INITIAL_SEND_DELAY_S.
            B = self._next_minute_boundary(anchor)
            tick = B

            # If we're already past the first tick (e.g., no clients for a while),
            # skip forward to the next future tick without sending catch-up bursts.
            now0 = datetime.now()
            while tick <= now0:
                tick = tick + timedelta(minutes=interval_min)

            while not self._stop.is_set() and self._normal_ready.is_set():
                self._next_minute_tick = tick
                ok = await self._sleep_until_dt_with_normal_check(tick)
                if not ok:
                    break

                if self.config.send_minutes and self._normal_ready.is_set():
                    await self.broadcast_payload({"minutes": int(tick.minute)}, tick_dt=tick)

                tick = tick + timedelta(minutes=interval_min)
                self._next_minute_tick = tick

                now_check = datetime.now()
                while tick <= now_check:
                    tick = tick + timedelta(minutes=interval_min)
                    self._next_minute_tick = tick

            if not self._normal_ready.is_set():
                self._next_minute_tick = None

    async def seconds_loop(self):
        """
        Interval ticks (seconds):
        - Waits for a normal client
        - First aligned tick is at the first whole-second boundary after the initial push (B)
        - Then every interval seconds from B, always aligned
        """
        while not self._stop.is_set():
            anchor = await self._wait_for_tick_anchor()
            if self._stop.is_set():
                break
            if anchor is None:
                continue

            interval_s = max(1, int(self.config.sec_interval_s))

            # Seconds follow the same rule as minutes:
            # - After initial push, next tick is the first whole-second boundary (B)
            # - Then every interval seconds from B, always aligned
            B = self._next_second_boundary(anchor)
            tick = B

            while not self._stop.is_set() and self._normal_ready.is_set():
                self._next_second_tick = tick
                ok = await self._sleep_until_dt_with_normal_check(tick)
                if not ok:
                    break

                if self.config.send_seconds and self._normal_ready.is_set():
                    # Use the scheduled tick time for deterministic values/alignment.
                    await self.broadcast_payload({"seconds": int(tick.second)}, tick_dt=tick)

                tick = tick + timedelta(seconds=interval_s)
                self._next_second_tick = tick

                now_check = datetime.now()
                while tick <= now_check:
                    tick = tick + timedelta(seconds=interval_s)
                    self._next_second_tick = tick

            if not self._normal_ready.is_set():
                self._next_second_tick = None

    async def status_loop(self):
        while not self._stop.is_set():
            await self.broadcast_ui_event("status")
            await asyncio.sleep(max(1, int(self.config.status_interval_s)))

    def write_lock(self):
        try:
            self.lock_path.write_text(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "host": self.config.host,
                        "port": self.config.port,
                        "started": int(self._start_ts),
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

    def remove_lock(self):
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        except Exception:
            pass

    async def start(self):
        self._start_ts = time.time()
        self._start_dt = datetime.now()
        self.write_lock()

        self.log.info("Starting server on %s:%s", self.config.host, self.config.port)
        self._server = await websockets.serve(
            self.conn_handler,
            self.config.host,
            self.config.port,
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
            max_queue=self.config.max_queue,
            close_timeout=self.config.close_timeout,
        )

        await self.broadcast_ui_event("status")

        try:
            self.log.info(
                "To stop this server (when started without the UI), run '%s' from the Server folder.",
                STOP_HELP_BAT,
            )
        except Exception:
            pass

        tasks = [
            asyncio.create_task(self.minute_loop()),
            asyncio.create_task(self.seconds_loop()),
            asyncio.create_task(self.status_loop()),
        ]

        try:
            await self._stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            for t in list(self._init_tasks.values()):
                try:
                    t.cancel()
                except Exception:
                    pass
            self._init_tasks.clear()

            if self._server:
                self._server.close()
                await self._server.wait_closed()

            self.remove_lock()

    def stop(self):
        self._stop.set()


def setup_logging():
    log_dir = get_appdata_dir()
    log_path = log_dir / "ASS_WS_Server.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def parse_args(argv):
    settings_path = None
    host = None
    port = None

    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--settings", "-s") and i + 1 < len(argv):
            settings_path = Path(argv[i + 1])
            i += 2
            continue
        if a == "--host" and i + 1 < len(argv):
            host = argv[i + 1]
            i += 2
            continue
        if a == "--port" and i + 1 < len(argv):
            try:
                port = int(argv[i + 1])
            except Exception:
                port = None
            i += 2
            continue
        i += 1

    return settings_path, host, port


async def main():
    setup_logging()

    settings_path, host, port = parse_args(sys.argv[1:])
    server = ASSWSServer(settings_path=settings_path)

    if host:
        server.config.host = host
    if port:
        server.config.port = port

    loop = asyncio.get_running_loop()

    if os.name != "nt":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, server.stop)

    try:
        await server.start()
    except OSError as e:
        logging.getLogger("ASS_WS_Server").error("Failed to start WS server: %s", e)
        server.remove_lock()
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
