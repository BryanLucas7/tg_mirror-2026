import os
import sys
import time
import json
import asyncio
import warnings
import logging
import re
import subprocess
import shutil
import random
import math
import io
import heapq
import functools
import inspect
from contextlib import nullcontext
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from hashlib import md5, sha256
from utils import (
    Banner,
    show_banner,
    cache_path,
    authenticate,
    acquire_available_session,
    acquire_runtime_lock,
    release_runtime_lock,
    build_run_id,
    create_run_download_dir,
    cleanup_run_download_dir,
    build_lock_name,
)

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client
from pyrogram import raw
from pyrogram.types import InputMediaAudio, InputMediaDocument, InputMediaPhoto, InputMediaVideo
from pyrogram.errors import CDNFileHashMismatch, FloodWait, TakeoutInitDelay, VolumeLocNotFound
from pyrogram.file_id import FileId, FileType
from pyrogram.session import Auth, Session
from pyrogram.crypto import aes
import pyrogram

""" Global """
session_name = "user"
download_path = "downloads"
MEDIA_CAPTION_LIMIT = 1024
runtime_lock_path = None
task_lock_path = None
run_download_path = None
DOWNLOAD_WORKERS = 3
PREUPLOAD_WORKERS = 4
PUBLISH_WORKERS = 1
MAX_LOCAL_DISK_BYTES_PER_JOB = 3 * 1024 * 1024 * 1024
RESERVE_MARGIN_SINGLE = 8 * 1024 * 1024
RESERVE_MARGIN_ALBUM = 16 * 1024 * 1024
SCHEDULE_LOOKAHEAD = 8
UPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS = PUBLISH_WORKERS + 1
PREUPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS = PREUPLOAD_WORKERS
DOWNLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS = DOWNLOAD_WORKERS
DOWNLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS_TAKEOUT = DOWNLOAD_WORKERS
PREFER_DOWNLOAD_MODE_NORMAL = False
STREAM_RELAY_ENGINE = (os.getenv("FORWARD_STREAM_RELAY_ENGINE", "instrumented").strip().lower() or "instrumented")
if STREAM_RELAY_ENGINE not in {"baseline", "instrumented"}:
    STREAM_RELAY_ENGINE = "instrumented"
PYROGRAM_CLIENT_SLEEP_THRESHOLD_SECONDS = 30
SOURCE_BUDGET_ENABLED = True
ENFORCE_TELEGRAM_QUEUE_CAPS = False
STREAM_RELAY_MIN_BYTES = 20 * 1024 * 1024
STREAM_UPLOAD_SESSIONS = 5  # sessões de upload paralelas por stream (big files)
STREAM_UPLOAD_QUEUE_DEPTH = 8  # profundidade local por sessão para amortecer backpressure
STREAM_RELAY_INCLUDE_SOURCE_THUMB = False  # deixa o servidor gerar preview quando possível
STREAM_RELAY_MAX_ACTIVE = max(1, min(PREUPLOAD_WORKERS, 4))
STREAM_RELAY_LARGE_BYTES = 300 * 1024 * 1024
STREAM_RELAY_HUGE_BYTES = 800 * 1024 * 1024
STREAM_RELAY_MAX_LARGE_ACTIVE = 2
STREAM_RELAY_MAX_LARGE_ACTIVE_BURST = 3  # preset do run 667: libera um terceiro grande quando houver fila real esperando slot
STREAM_RELAY_MAX_HUGE_ACTIVE = 1
SOURCE_READ_ENABLE_CDN = True
SOURCE_READ_LARGE_BYTES = 300 * 1024 * 1024
SOURCE_READ_CUSTOM_MIN_BYTES = 800 * 1024 * 1024
SOURCE_READ_PARALLEL_MIN_BYTES = 800 * 1024 * 1024
SOURCE_READ_PARALLEL_SESSIONS = 1  # run 667 usava apenas 1 sessao por leitura custom/paralela
SOURCE_READ_SESSION_BUDGET = 4  # orçamento por DC observado no run 667
SOURCE_READ_SMALL_MAX_ACTIVE_PER_DC = 2
SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC = 2
SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC_BURST = 3  # preset do run 667: libera um terceiro grande por DC quando houver fila real aguardando leitura
SOURCE_READ_CHUNK_BYTES = 1024 * 1024
SOURCE_READ_SLEEP_THRESHOLD_SECONDS = 20  # absorve waits pequenos no custom, como o path legacy faz internamente
SOURCE_READ_LOCAL_RETRY_MAX_ATTEMPTS = 2  # retries locais evitam escalar limitacoes breves para retry global
SOURCE_READ_LOCAL_RETRY_BACKOFF_BASE_SECONDS = 3
SOURCE_READ_LOCAL_RETRY_BACKOFF_MAX_SECONDS = 10
HEAD_PROTECTED_ITEMS = 3
DISK_HEADROOM_BYTES = 768 * 1024 * 1024
SIZE_AWARE_HORIZON = 4
ETA_HISTORY_SIZE = 6
STATUS_LOG_INTERVAL_SECONDS = 15
PREUPLOAD_TRANSIENT_MAX_ATTEMPTS = 8
PREUPLOAD_TRANSIENT_BACKOFF_BASE_SECONDS = 8
PREUPLOAD_TRANSIENT_BACKOFF_MAX_SECONDS = 120
FAILED_ITEM_RETRY_MAX_ATTEMPTS = 3
FAILED_ITEM_RETRY_BACKOFF_BASE_SECONDS = 20
FAILED_ITEM_RETRY_BACKOFF_MAX_SECONDS = 180
HEAD_PRESSURE_BACKGROUND_PREUPLOAD_LIMIT = 1
HEAD_PRESSURE_READY_BACKLOG_THRESHOLD = 2
LARGE_RELAY_DIAGNOSTIC_BYTES = 300 * 1024 * 1024
LOG_MODE = os.getenv("FORWARD_LOG_MODE", "quiet").strip().lower() or "quiet"
DETAILED_LOG_TO_FILE = os.getenv("FORWARD_DETAILED_LOG", "0").strip().lower() not in {"0", "false", "no", "off"}
DETAILED_LOG_DIR = os.path.abspath(os.getenv("FORWARD_LOG_DIR", os.path.join("forward_task", "logs")))
ANALYTICS_TO_FILE = os.getenv("FORWARD_ANALYTICS", "0").strip().lower() not in {"0", "false", "no", "off"}
PYROGRAM_SESSION_LOGGER_NAME = "pyrogram.session.session"
ETA_FALLBACK_SECONDS_BY_KIND = {
    "video": 90.0,
    "arquivo": 75.0,
    "foto": 12.0,
    "audio": 45.0,
    "sticker": 10.0,
    "animacao": 70.0,
    "album": 120.0,
}
ANALYTICS_DIR = os.path.abspath(os.path.join("forward_task", "analytics"))
ANALYTICS_VERSION = 2
REGRESSION_DELTA_PERCENT = 12.0

SIZE_BUCKET_SMALL_MAX_BYTES = 80 * 1024 * 1024
SIZE_BUCKET_MEDIUM_MAX_BYTES = 300 * 1024 * 1024
SIZE_BUCKET_LARGE_MAX_BYTES = 800 * 1024 * 1024

HOT_ITEM_PREUP_SECONDS = 120.0
HOT_ITEM_PREQ_SECONDS = 90.0
HOT_SMALL_ANOMALY_BYTES = 80 * 1024 * 1024
HOT_SMALL_PRE_SECONDS = 90.0
HIGH_RETRY_DELAY_SECONDS = 15.0

HOL_MODERATE_MIN_STREAK = 3
HOL_SEVERE_MIN_STREAK = 5
HOL_MIN_READY_BEHIND = 2
UNPRODUCTIVE_CONTENTION_MIN_SECONDS = 30.0

OVERSUB_THROUGHPUT_FLOOR_MIB_S = 1.0
SMALL_CONTAMINATION_PREQ_SECONDS = 60.0
SOURCE_BOUND_GETFILE_WAIT_THRESHOLD = 3

LANE_STARVATION_FACTOR = 2.0
LANE_P95_DISCREPANCY_FACTOR = 1.7
WORKER_IMBALANCE_SHARE = 0.35
TAIL_DEGRADATION_FACTOR = 1.5

# Limites oficiais do Telegram para filas de download no mesmo DC.
# getAppConfig retorna em runtime; estes defaults espelham a listagem pública atual.
TG_QUEUE_BYTES_CUTOFF = 20 * 1024 * 1024
TG_QUEUE_CAP_LARGE_DEFAULT = 2
TG_QUEUE_CAP_SMALL_DEFAULT = 5

# Backpressure adaptativo por FLOOD_WAIT em upload.GetFile.
FLOOD_PRESSURE_WINDOW_SECONDS = 90.0
FLOOD_PRESSURE_TIER_LOW_SECONDS = 5.0
FLOOD_PRESSURE_TIER_HIGH_SECONDS = 20.0
FLOOD_HYSTERESIS_CLEAN_SECONDS = 90.0

# Tabela de knobs por tier. Tier 0 = cap do config; tiers > 0 reduzem progressivamente.
# Local cap (SMALL/LARGE/BURST/SESSION_BUDGET) é o piso máximo; invariante tg_large
# separadamente garante que nunca exceda tg_queue_cap_large.
SOURCE_ADAPTIVE_TIERS = {
    0: {"session": None, "small": None, "large": None, "large_burst": None},
    1: {"session": 4, "small": 2, "large": 2, "large_burst": 3},
    2: {"session": 2, "small": 2, "large": 1, "large_burst": 2},
}

CAUSE_QUEUE_BOUND = "queue_bound"
CAUSE_SOURCE_BOUND = "source_bound"
CAUSE_RELAY_SLOT_BOUND = "relay_slot_bound"
CAUSE_RELAY_POLICY_BOUND = "relay_policy_bound"
CAUSE_TELEGRAM_LIMITED = "telegram_limited"
CAUSE_HOL_BOUND = "head_of_line_bound"
CAUSE_SCHEDULER_UNFAIR = "scheduler_unfair"
CAUSE_MIXED = "mixed"

UPLOAD_GETFILE_WAIT_REGEX = re.compile(
    r'Waiting for (\d+) seconds before continuing \(required by "upload\\.GetFile"\)'
)

# Regex do hook live (formato do pyrogram runtime antes do prefixo do handler).
FLOOD_WAIT_LIVE_REGEX = re.compile(
    r'Waiting for (\d+) seconds before continuing \(required by "([^"]+)"\)'
)

@dataclass
class FloodPressureTracker:
    """Acumula eventos de FLOOD_WAIT com janela deslizante + histerese por tier.
    Tier 0 = saudavel (conf do getAppConfig). Tier 1/2 = reducao adaptativa.
    Subida de tier acontece imediatamente; descida exige FLOOD_HYSTERESIS_CLEAN_SECONDS."""
    window_seconds: float = FLOOD_PRESSURE_WINDOW_SECONDS
    events: list = field(default_factory=list)
    last_event_at: float = 0.0
    current_tier: int = 0
    last_tier_change_at: float = 0.0
    total_wait_seconds: float = 0.0
    total_events: int = 0
    transitions_total: int = 0
    peak_pressure_seconds: float = 0.0

    def record(self, wait_seconds):
        now = time.time()
        self.events.append((now, float(wait_seconds)))
        self.last_event_at = now
        self.total_wait_seconds += float(wait_seconds)
        self.total_events += 1
        self._update_tier(now)

    def _prune(self, now):
        cutoff = now - self.window_seconds
        self.events = [(t, s) for (t, s) in self.events if t >= cutoff]

    def pressure(self, now=None):
        now = now if now is not None else time.time()
        self._prune(now)
        value = sum(s for (_, s) in self.events)
        if value > self.peak_pressure_seconds:
            self.peak_pressure_seconds = value
        return value

    def _update_tier(self, now):
        p = self.pressure(now)
        target = 0
        # O primeiro FLOOD_WAIT em upload.GetFile ja indica pressao real do Telegram.
        # Entramos no tier 1 imediatamente para conter a cascata inicial de readers.
        if self.events and p > 0:
            target = 1
        if p >= FLOOD_PRESSURE_TIER_HIGH_SECONDS:
            target = 2
        elif p >= FLOOD_PRESSURE_TIER_LOW_SECONDS:
            target = 1
        if target > self.current_tier:
            self.current_tier = target
            self.last_tier_change_at = now
            self.transitions_total += 1
            return
        if self.current_tier > 0 and target < self.current_tier:
            reference = max(self.last_event_at, self.last_tier_change_at)
            clean_for = now - reference
            if clean_for >= FLOOD_HYSTERESIS_CLEAN_SECONDS:
                self.current_tier -= 1
                self.last_tier_change_at = now
                self.transitions_total += 1

    def tier(self):
        self._update_tier(time.time())
        return self.current_tier

@dataclass
class UploadResumeState:
    file_id: int
    file_total_parts: int
    is_big: bool
    acked_parts: Set[int] = field(default_factory=set)

@dataclass
class WorkItem:
    seq: int
    kind: str
    messages: List
    estimated_bytes: int
    label: str
    media_kind: str
    state: str = "pending"
    reserved_bytes: int = 0
    actual_bytes: int = 0
    local_paths: List[str] = field(default_factory=list)
    aux_paths: List[str] = field(default_factory=list)
    overflow_text: str = ""
    error: str = ""
    download_queued_at: Optional[float] = None
    preupload_queued_at: Optional[float] = None
    download_started_at: Optional[float] = None
    download_finished_at: Optional[float] = None
    preupload_started_at: Optional[float] = None
    preupload_finished_at: Optional[float] = None
    first_preupload_started_at: Optional[float] = None
    publish_started_at: Optional[float] = None
    publish_finished_at: Optional[float] = None
    acked_at: Optional[float] = None
    relay_main_started_at: Optional[float] = None
    relay_main_finished_at: Optional[float] = None
    relay_main_source_read_seconds: float = 0.0
    relay_main_queue_backpressure_seconds: float = 0.0
    relay_main_save_parts_seconds: float = 0.0
    relay_main_chunks: int = 0
    relay_thumb_started_at: Optional[float] = None
    relay_thumb_finished_at: Optional[float] = None
    relay_thumb_source_read_seconds: float = 0.0
    relay_thumb_queue_backpressure_seconds: float = 0.0
    relay_thumb_save_parts_seconds: float = 0.0
    relay_thumb_chunks: int = 0
    relay_finalize_started_at: Optional[float] = None
    relay_finalize_finished_at: Optional[float] = None
    download_queue_depth_at_enqueue: int = 0
    preupload_queue_depth_at_enqueue: int = 0
    download_items_ahead_at_enqueue: int = 0
    preupload_items_ahead_at_enqueue: int = 0
    last_wait_reason: str = ""
    wait_reason_started_at: Optional[float] = None
    send_started_at: Optional[float] = None
    send_finished_at: Optional[float] = None
    remote_media: object = None
    remote_media_group: List[object] = field(default_factory=list)
    upload_resume_states: Dict[Tuple[int, str], UploadResumeState] = field(default_factory=dict)
    caption_text: str = ""
    stream_relay: bool = False
    failure_retry_attempts: int = 0
    source_mode_initial: str = ""
    source_mode_final: str = ""
    source_fallback_count: int = 0
    source_fallback_resume_chunk: Optional[int] = None
    source_dc_id: Optional[int] = None

    @property
    def first_message(self):
        return self.messages[0]

    @property
    def last_message_id(self):
        return self.messages[-1].id

    @property
    def display_message_id(self):
        return self.first_message.id

    @property
    def needs_download(self):
        return self.kind == "album" or not self.first_message.text

class DiskBudgetManager:
    def __init__(self, max_bytes, reserved_headroom_bytes=0):
        self.max_bytes = max_bytes
        self.reserved_headroom_bytes = min(max_bytes, max(0, reserved_headroom_bytes))
        self.reserved_bytes = 0
        self.oversize_owner_seq = None
        self._condition = asyncio.Condition()

    def current_bytes(self):
        return self.reserved_bytes

    def _effective_limit(self, use_reserved_headroom):
        if use_reserved_headroom:
            return self.max_bytes
        return max(1, self.max_bytes - self.reserved_headroom_bytes)

    def _can_reserve(self, item, use_reserved_headroom):
        if self.oversize_owner_seq is not None:
            return False
        if item.estimated_bytes > self.max_bytes:
            return self.reserved_bytes == 0
        effective_limit = self._effective_limit(use_reserved_headroom)
        if item.estimated_bytes > effective_limit:
            return not use_reserved_headroom and self.reserved_bytes == 0
        return (self.reserved_bytes + item.estimated_bytes) <= effective_limit

    async def reserve(self, item, use_reserved_headroom=False):
        waited = False
        async with self._condition:
            while not self._can_reserve(item, use_reserved_headroom):
                waited = True
                item.state = "waiting_budget"
                await self._condition.wait()

            self.reserved_bytes += item.estimated_bytes
            item.reserved_bytes = item.estimated_bytes
            item.state = "reserved"

            if item.estimated_bytes > self.max_bytes:
                self.oversize_owner_seq = item.seq
        return waited

    async def adjust_after_download(self, item, actual_bytes):
        async with self._condition:
            delta = actual_bytes - item.reserved_bytes
            self.reserved_bytes = max(0, self.reserved_bytes + delta)
            item.actual_bytes = actual_bytes
            item.reserved_bytes = actual_bytes
            self._condition.notify_all()

    async def release(self, item):
        async with self._condition:
            self.reserved_bytes = max(0, self.reserved_bytes - item.reserved_bytes)
            if self.oversize_owner_seq == item.seq:
                self.oversize_owner_seq = None
            item.reserved_bytes = 0
            self._condition.notify_all()

@dataclass
class CloneJobContext:
    channel_source: int
    destination: dict
    chat_title: str
    custom_caption: str
    progress_file: str
    work_items: List[WorkItem]
    disk_budget: DiskBudgetManager
    download_queue: asyncio.PriorityQueue
    preupload_queue: asyncio.PriorityQueue
    destination_peer: object = None
    ready_items: dict = field(default_factory=dict)
    source_pinned_message_ids: List[int] = field(default_factory=list)
    source_primary_pinned_message_id: Optional[int] = None
    published_message_ids: dict = field(default_factory=dict)
    total_items: int = 0
    started_at: float = 0.0
    next_seq_to_publish: int = 1
    scheduled_until_seq: int = 0
    success_count: int = 0
    failure_count: int = 0
    log_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    ready_condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    schedule_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    download_slot_condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    preupload_slot_condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    stream_relay_condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    preupload_eta_history: dict = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=ETA_HISTORY_SIZE)))
    media_session_pools: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=lambda: {
        "relay_files": 0,
        "relay_bytes": 0,
        "relay_seconds": 0.0,
        "relay_session_starts": 0,
        "relay_session_reuses": 0,
        "relay_pool_resets": 0,
        "relay_errors": 0,
        "relay_resumes": 0,
        "relay_parts_skipped_on_resume": 0,
        "auth_key_duplicated": 0,
        "source_custom_files": 0,
        "source_parallel_files": 0,
        "source_fallbacks": 0,
        "source_cdn_redirects": 0,
        "source_session_starts": 0,
        "source_cdn_session_starts": 0,
        "flood_waits_by_method": defaultdict(int),
        "flood_wait_seconds_by_method": defaultdict(float),
        "download_phase_seconds": 0.0,
        "download_phase_bytes": 0,
        "preupload_phase_seconds": 0.0,
        "preupload_phase_bytes": 0,
        "publish_phase_seconds": 0.0,
        "publish_items": 0,
    })
    active_background_downloads: int = 0
    active_background_preuploads: int = 0
    active_download_workers: int = 0
    active_preupload_workers: int = 0
    active_stream_relays: int = 0
    active_large_stream_relays: int = 0
    active_huge_stream_relays: int = 0
    waiting_stream_relay_slots: int = 0
    waiting_large_stream_relay_slots: int = 0
    waiting_huge_stream_relay_slots: int = 0
    source_budget_condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    source_budget_by_dc: dict = field(default_factory=dict)
    log_mode: str = "normal"
    detailed_log_path: Optional[str] = None
    detailed_log_stream: Any = None
    pyrogram_wait_handler: Any = None
    pyrogram_wait_logger_propagate: bool = True
    pyrogram_flood_handler: Any = None
    tg_queue_cap_large: int = TG_QUEUE_CAP_LARGE_DEFAULT
    tg_queue_cap_small: int = TG_QUEUE_CAP_SMALL_DEFAULT
    tg_queue_cap_source: str = "default"
    flood_pressure: Any = field(default_factory=FloodPressureTracker)
    analytics_status_snapshots: list = field(default_factory=list)
    analytics_events: list = field(default_factory=list)
    analytics_item_state: dict = field(default_factory=dict)
    analytics_report: dict = field(default_factory=dict)
    source_download_mode: str = "takeout"

@dataclass
class MediaSessionPool:
    dc_id: int
    sessions: List[Session]
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

@dataclass
class SourceReadDcBudgetState:
    active_sessions: int = 0
    active_small_readers: int = 0
    active_large_readers: int = 0
    waiting_large_readers: int = 0
    active_tg_large: int = 0
    active_tg_small: int = 0

def classify_tg_queue(file_size):
    if file_size is None:
        return "tg_large"
    return "tg_small" if int(file_size) < TG_QUEUE_BYTES_CUTOFF else "tg_large"

def get_flood_tier(context):
    tracker = getattr(context, "flood_pressure", None)
    if tracker is None:
        return 0
    return tracker.tier()

def should_enforce_telegram_queue_caps(context):
    if not ENFORCE_TELEGRAM_QUEUE_CAPS:
        return False
    source = str(getattr(context, "tg_queue_cap_source", "") or "")
    if source == "help.getAppConfig":
        return True
    return get_flood_tier(context) >= 1

def get_effective_telegram_queue_cap(context, tg_queue):
    if tg_queue == "tg_large":
        base_cap = max(1, int(getattr(context, "tg_queue_cap_large", TG_QUEUE_CAP_LARGE_DEFAULT)))
        if not ENFORCE_TELEGRAM_QUEUE_CAPS:
            return base_cap
        if str(getattr(context, "source_download_mode", "") or "") == "takeout":
            return min(base_cap, 1)
        return base_cap
    return max(1, int(getattr(context, "tg_queue_cap_small", TG_QUEUE_CAP_SMALL_DEFAULT)))

def _resolve_adaptive_value(context, knob, default_value):
    tier = get_flood_tier(context)
    if tier <= 0:
        return default_value
    override = SOURCE_ADAPTIVE_TIERS.get(tier, {}).get(knob)
    if override is None:
        return default_value
    return min(default_value, override)

def get_adaptive_source_session_budget(context):
    return max(1, _resolve_adaptive_value(context, "session", SOURCE_READ_SESSION_BUDGET))

def get_adaptive_source_small_max(context):
    return max(1, _resolve_adaptive_value(context, "small", SOURCE_READ_SMALL_MAX_ACTIVE_PER_DC))

def get_adaptive_source_large_max(context):
    return max(1, _resolve_adaptive_value(context, "large", SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC))

def get_adaptive_source_large_burst(context):
    return max(
        get_adaptive_source_large_max(context),
        _resolve_adaptive_value(context, "large_burst", SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC_BURST),
    )

def coerce_json_int(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"[-+]?\d+", text):
            try:
                return int(text)
            except ValueError:
                return None
        return None
    inner = getattr(value, "value", None)
    if inner is value:
        return None
    return coerce_json_int(inner)

async def probe_telegram_app_config(client, context):
    """Consulta help.getAppConfig para extrair caps de download por DC.
    Defaults aplicam em caso de erro (o Telegram trata como soft limits de qualquer jeito)."""
    try:
        response = await client.invoke(raw.functions.help.GetAppConfig(hash=0))
    except Exception as error:
        context.tg_queue_cap_source = f"default_fallback:{type(error).__name__}"
        return

    config_root = getattr(response, "config", None)
    if config_root is None:
        context.tg_queue_cap_source = "default_empty_response"
        return

    config_entries = None
    if isinstance(config_root, dict):
        config_entries = list(config_root.items())
    elif isinstance(config_root, (list, tuple)):
        config_entries = list(config_root)
    else:
        object_entries = getattr(config_root, "value", None)
        if isinstance(object_entries, (list, tuple)):
            config_entries = list(object_entries)

    if not config_entries:
        context.tg_queue_cap_source = "default_unexpected_config_shape"
        return

    found_large = None
    found_small = None
    for entry in config_entries:
        if isinstance(entry, tuple) and len(entry) == 2:
            key, value = entry
        elif isinstance(entry, dict):
            key = entry.get("key")
            value = entry.get("value")
        else:
            key = getattr(entry, "key", None)
            value = getattr(entry, "value", None)

        if not isinstance(key, str):
            continue

        numeric_value = coerce_json_int(value)
        if numeric_value is None:
            continue

        if key == "large_queue_max_active_operations_count":
            found_large = numeric_value
        elif key == "small_queue_max_active_operations_count":
            found_small = numeric_value

    if found_large is not None:
        context.tg_queue_cap_large = max(1, found_large)
    if found_small is not None:
        context.tg_queue_cap_small = max(1, found_small)
    context.tg_queue_cap_source = "help.getAppConfig"

class _PyrogramFloodPressureHandler(logging.Handler):
    """Handler inline que observa mensagens 'Waiting for N seconds ... upload.GetFile'
    e alimenta o FloodPressureTracker para o ciclo adaptativo."""
    def __init__(self, context):
        super().__init__(level=logging.WARNING)
        self._context = context
        self._tg_mirror_temp = True

    def emit(self, record):
        try:
            message = record.getMessage()
        except Exception:
            return
        match = FLOOD_WAIT_LIVE_REGEX.search(message)
        if not match:
            return
        method = match.group(2)
        if "upload.GetFile" not in method:
            return
        try:
            wait_seconds = float(match.group(1))
        except (TypeError, ValueError):
            return
        tracker = getattr(self._context, "flood_pressure", None)
        if tracker is None:
            return
        previous_tier = tracker.current_tier
        tracker.record(wait_seconds)
        if tracker.current_tier != previous_tier:
            try:
                record_analytics_event(
                    self._context,
                    "flood_pressure_tier_change",
                    previous_tier=previous_tier,
                    new_tier=tracker.current_tier,
                    pressure_seconds=tracker.pressure(),
                )
            except Exception:
                pass

def install_pyrogram_flood_handler(context):
    pyrogram_logger = logging.getLogger(PYROGRAM_SESSION_LOGGER_NAME)
    handler = _PyrogramFloodPressureHandler(context)
    pyrogram_logger.addHandler(handler)
    context.pyrogram_flood_handler = handler

def uninstall_pyrogram_flood_handler(context):
    if context.pyrogram_flood_handler is None:
        return
    pyrogram_logger = logging.getLogger(PYROGRAM_SESSION_LOGGER_NAME)
    try:
        pyrogram_logger.removeHandler(context.pyrogram_flood_handler)
    except Exception:
        pass
    try:
        context.pyrogram_flood_handler.close()
    except Exception:
        pass
    context.pyrogram_flood_handler = None

def seconds_between(started_at, finished_at):
    if not started_at or not finished_at:
        return 0.0
    return max(0.0, finished_at - started_at)

def safe_div(numerator, denominator):
    if not denominator:
        return 0.0
    return numerator / denominator

def percentile(values, p):
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (p / 100.0)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] + (ordered[high] - ordered[low]) * weight

def average(values):
    values = list(values)
    if not values:
        return 0.0
    return sum(values) / len(values)

def classify_size_bucket(num_bytes):
    size = max(0, int(num_bytes or 0))
    if size < SIZE_BUCKET_SMALL_MAX_BYTES:
        return "lt80mb"
    if size < SIZE_BUCKET_MEDIUM_MAX_BYTES:
        return "80_300mb"
    if size < SIZE_BUCKET_LARGE_MAX_BYTES:
        return "300_800mb"
    return "gt800mb"

def build_run_fingerprint(context):
    thread_id = context.destination.get("thread_id") or "chat"
    raw_name = f"{context.channel_source}_{context.destination['chat_id']}_{thread_id}"
    return clean_filename(raw_name)

def get_run_label():
    if run_download_path:
        return os.path.basename(run_download_path)
    return build_run_id("forward")

def ensure_item_analytics_state(context, item):
    state = context.analytics_item_state.get(item.seq)
    if state is not None:
        return state

    state = {
        "seq": item.seq,
        "label": item.label,
        "display_message_id": item.display_message_id,
        "estimated_bytes": int(item.estimated_bytes or 0),
        "size_bucket": classify_size_bucket(item.estimated_bytes),
        "lane": None,
        "pre_worker": None,
        "download_queue_wait_seconds": 0.0,
        "pre_queue_wait_seconds": 0.0,
        "download_seconds": 0.0,
        "preupload_seconds": 0.0,
        "preupload_wall_e2e_seconds": 0.0,
        "publish_seconds": 0.0,
        "source_wait_seconds": 0.0,
        "source_wait_count": 0,
        "source_budget_wait_seconds": 0.0,
        "source_budget_wait_count": 0,
        "source_api_stall_seconds": 0.0,
        "source_api_stall_count": 0,
        "source_local_retry_seconds": 0.0,
        "source_local_retry_count": 0,
        "source_local_retry_backoff_seconds": 0.0,
        "source_mode_initial": "",
        "source_mode_final": "",
        "source_fallback_count": 0,
        "source_fallback_resume_chunk": None,
        "source_dc": None,
        "retry_phase_counts": {},
        "stream_slot_wait_seconds": 0.0,
        "stream_slot_wait_count": 0,
        "flood_wait_seconds": 0.0,
        "flood_wait_count": 0,
        "transient_retry_count": 0,
        "transient_backoff_seconds": 0.0,
        "fallback_legacy": False,
        "wait_reason_counts": {},
        "hol_wait_seconds": 0.0,
        "source_budget_bound": False,
        "source_budget_severity": None,
        "source_budget_event_count": 0,
        "source_api_bound": False,
        "source_api_severity": None,
        "source_api_event_count": 0,
        "relay_policy_bound": False,
        "relay_policy_severity": None,
        "relay_policy_event_count": 0,
        "dominant_cause": None,
        "dominant_cause_scores": {},
    }
    context.analytics_item_state[item.seq] = state
    return state

def mark_wait_reason(context, item, reason):
    normalized_reason = reason or ""
    if normalized_reason != item.last_wait_reason:
        item.last_wait_reason = normalized_reason
        item.wait_reason_started_at = time.time() if normalized_reason else None
    elif normalized_reason and item.wait_reason_started_at is None:
        item.wait_reason_started_at = time.time()
    if not normalized_reason:
        return
    state = ensure_item_analytics_state(context, item)
    state["wait_reason_counts"][normalized_reason] = state["wait_reason_counts"].get(normalized_reason, 0) + 1

def update_item_source_wait_totals(state):
    state["source_wait_seconds"] = (
        float(state.get("source_budget_wait_seconds", 0.0))
        + float(state.get("source_api_stall_seconds", 0.0))
        + float(state.get("source_local_retry_backoff_seconds", 0.0))
    )
    state["source_wait_count"] = (
        int(state.get("source_budget_wait_count", 0))
        + int(state.get("source_api_stall_count", 0))
        + int(state.get("source_local_retry_count", 0))
    )

def set_item_source_mode(context, item, mode, dc_id=None, final=False):
    state = ensure_item_analytics_state(context, item)
    if mode and not item.source_mode_initial:
        item.source_mode_initial = mode
    if mode and not state.get("source_mode_initial"):
        state["source_mode_initial"] = mode
    if dc_id is not None:
        item.source_dc_id = dc_id
        state["source_dc"] = dc_id
    if final and mode:
        item.source_mode_final = mode
        state["source_mode_final"] = mode
    elif mode and not item.source_mode_final:
        item.source_mode_final = mode
        state["source_mode_final"] = mode

def normalize_retry_phase_name(raw_phase):
    phase = (raw_phase or "").strip().lower()
    if not phase:
        return "unknown"
    if phase.startswith("waiting_for_backoff:"):
        phase = phase.split(":", 1)[1]
    if phase.startswith("source_"):
        return "source_read"
    if phase.startswith("save_parts_"):
        return "save_parts"
    if phase.startswith("queue_backpressure_"):
        return "queue_backpressure"
    if phase == "upload_media_finalize":
        return "finalize"
    if "stream_relay_slot" in phase:
        return "stream_relay_slot"
    if "source_budget" in phase:
        return "source_budget"
    if phase.startswith("stream_upload_"):
        return "stream_upload"
    return phase

def infer_retry_phase(item, error=None):
    wait_reason = item.last_wait_reason or ""
    phase = normalize_retry_phase_name(wait_reason)
    if phase != "unknown":
        return phase
    error_text = str(error or "").upper()
    if "GETFILE" in error_text:
        return "source_read"
    if "FILE_REFERENCE_EXPIRED" in error_text:
        return "refresh_reference"
    if "FLOOD" in error_text:
        return "telegram_limited"
    if is_transport_error(error or ""):
        return "transport"
    return "unknown"

def record_retry_phase(state, phase):
    phase_name = normalize_retry_phase_name(phase)
    phase_counts = state.setdefault("retry_phase_counts", {})
    phase_counts[phase_name] = phase_counts.get(phase_name, 0) + 1

def get_item_source_mode(item):
    return item.source_mode_final or item.source_mode_initial or ("legacy" if not item.stream_relay else "desconhecido")

def get_item_wait_reason_age_seconds(item):
    if not item.last_wait_reason or not item.wait_reason_started_at:
        return 0.0
    return max(0.0, time.time() - item.wait_reason_started_at)

def get_item_head_age_seconds(item):
    state_started_at = None
    if item.state == "preuploading":
        state_started_at = item.first_preupload_started_at or item.preupload_started_at or item.preupload_queued_at
    elif item.state == "downloading":
        state_started_at = item.download_started_at or item.download_queued_at
    elif item.state == "pending":
        state_started_at = item.preupload_queued_at or item.download_queued_at
    elif item.state == "failed":
        state_started_at = item.first_preupload_started_at or item.preupload_started_at or item.download_started_at
    if not state_started_at:
        return 0.0
    return max(0.0, time.time() - state_started_at)

def prime_item_source_plan(context, item, source_ref, file_size, stage_name="main"):
    if not item.stream_relay:
        return
    effective_size = max(int(file_size or 0), int(item.estimated_bytes or 0), int(get_media_size(source_ref) or 0))
    mode, _ = get_source_reader_mode(effective_size, stage_name)
    dc_id = item.source_dc_id
    if dc_id is None:
        try:
            dc_id, _ = build_source_download_location(source_ref)
        except Exception:
            dc_id = None
    set_item_source_mode(context, item, mode, dc_id=dc_id, final=False)

def record_analytics_event(context, event_type, item=None, **payload):
    entry = {
        "ts": time.time(),
        "event": event_type,
        "seq": item.seq if item is not None else None,
        "label": item.label if item is not None else None,
    }
    entry.update(payload)
    context.analytics_events.append(entry)

def compute_delta(current, previous):
    if previous is None:
        return {
            "current": current,
            "previous": None,
            "absolute": None,
            "percent": None,
        }
    absolute = current - previous
    percent = (absolute / previous * 100.0) if previous else None
    return {
        "current": current,
        "previous": previous,
        "absolute": absolute,
        "percent": percent,
    }

def format_delta_percent(delta):
    if delta is None:
        return "N/A"
    return f"{delta:+.1f}%"

def format_seconds_hms(seconds):
    return format_seconds(max(0, int(round(seconds))))

def analytics_file_prefix(fingerprint):
    return f"forward_analytics_{fingerprint}_"

def build_analytics_report_path(fingerprint, run_label):
    if not ANALYTICS_TO_FILE:
        return None
    os.makedirs(ANALYTICS_DIR, exist_ok=True)
    filename = clean_filename(f"{analytics_file_prefix(fingerprint)}{run_label}.json")
    return os.path.join(ANALYTICS_DIR, filename)

def list_analytics_files_for_fingerprint(fingerprint):
    if not ANALYTICS_TO_FILE:
        return []
    if not os.path.isdir(ANALYTICS_DIR):
        return []
    prefix = analytics_file_prefix(fingerprint)
    entries = []
    for name in os.listdir(ANALYTICS_DIR):
        if not name.endswith(".json"):
            continue
        if not name.startswith(prefix):
            continue
        full_path = os.path.join(ANALYTICS_DIR, name)
        entries.append((os.path.getmtime(full_path), full_path))
    entries.sort(key=lambda entry: entry[0], reverse=True)
    return [path for _, path in entries]

def load_previous_analytics_report(fingerprint):
    files = list_analytics_files_for_fingerprint(fingerprint)
    if not files:
        return None
    latest_path = files[0]
    try:
        with open(latest_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            data["_source_file"] = latest_path
            return data
    except Exception:
        return None

def save_analytics_report(report):
    if not ANALYTICS_TO_FILE:
        return
    report_path = report.get("run", {}).get("report_path")
    if not report_path:
        return
    os.makedirs(ANALYTICS_DIR, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)

def resolve_binary(executable_name):
    local_path = os.path.join("tools", "ffmpeg", "bin", executable_name)
    return local_path if os.path.exists(local_path) else shutil.which(executable_name)

FFMPEG_PATH = resolve_binary("ffmpeg.exe")
FFPROBE_PATH = resolve_binary("ffprobe.exe")

def normalize_topic_title(title):
    title = (title or "Topico").strip()
    return title[:128] or "Topico"

def create_forum_topic(client, target_chat_id, topic_title):
    topic_title = normalize_topic_title(topic_title)
    input_channel = client.resolve_peer(target_chat_id)

    client.invoke(
        raw.functions.channels.CreateForumTopic(
            channel=input_channel,
            title=topic_title,
            random_id=random.randrange(1, 1 << 63),
            icon_color=0x6FB9F0,
        )
    )

    forum_topics = client.invoke(
        raw.functions.channels.GetForumTopics(
            channel=input_channel,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100,
            q=topic_title,
        )
    )

    matches = [topic for topic in forum_topics.topics if getattr(topic, "title", None) == topic_title]
    if not matches:
        raise RuntimeError("Nao foi possivel localizar o topico criado no grupo de destino.")

    return max(matches, key=lambda topic: getattr(topic, "id", 0)).id

def extract_migrated_supergroup_id(updates):
    for chat in reversed(getattr(updates, "chats", [])):
        channel_id = getattr(chat, "id", None)
        if channel_id is not None:
            return pyrogram.utils.get_channel_id(channel_id)
    raise RuntimeError("Nao foi possivel identificar o supergrupo criado apos a migracao do grupo.")

def migrate_group_to_supergroup(client, target_chat):
    updates = client.invoke(
        raw.functions.messages.MigrateChat(
            chat_id=abs(target_chat.id),
        )
    )
    migrated_chat_id = extract_migrated_supergroup_id(updates)
    time.sleep(1)
    return client.get_chat(migrated_chat_id)

def resolve_target_destination(client, source_chat, target_chat):
    destination = {
        "chat_id": target_chat.id,
        "thread_id": None,
        "mode_label": "chat",
    }

    if target_chat.type == pyrogram.enums.ChatType.CHANNEL:
        return destination

    if target_chat.type == pyrogram.enums.ChatType.GROUP:
        print("O destino escolhido e um grupo basico.")
        print("1 - Continuar clonando no grupo em si")
        print("2 - Converter para supergrupo, ativar topicos e criar o topico da origem")
        print("3 - Cancelar")
        choice = input("Escolha (1/2/3): ").strip() or "1"
        if choice == "1":
            print("O envio sera feito para o grupo em si, sem topico.")
            return destination
        if choice == "3":
            raise RuntimeError("Envio cancelado pelo usuario.")

        migrated_chat = migrate_group_to_supergroup(client, target_chat)
        print(f"Grupo convertido para supergrupo: {migrated_chat.id}")
        target_chat = migrated_chat
        destination["chat_id"] = migrated_chat.id
        source_title = source_chat.title or "Topico"

        if not getattr(target_chat, "is_forum", False):
            input_channel = client.resolve_peer(target_chat.id)
            client.invoke(
                raw.functions.channels.ToggleForum(
                    channel=input_channel,
                    enabled=True,
                )
            )

        topic_id = create_forum_topic(client, target_chat.id, source_title)
        destination["thread_id"] = topic_id
        destination["mode_label"] = f"topico:{topic_id}"
        print(f"Forum ativado e topico criado: '{normalize_topic_title(source_title)}' (ID {topic_id}).")
        return destination

    if target_chat.type != pyrogram.enums.ChatType.SUPERGROUP:
        print("Destino sem suporte a topicos. O envio sera feito para o chat em si.")
        return destination

    source_title = source_chat.title or "Topico"

    if getattr(target_chat, "is_forum", False):
        topic_id = create_forum_topic(client, target_chat.id, source_title)
        destination["thread_id"] = topic_id
        destination["mode_label"] = f"topico:{topic_id}"
        print(f"Topico criado automaticamente no grupo de destino: '{normalize_topic_title(source_title)}' (ID {topic_id}).")
        return destination

    print("O grupo de destino ainda nao esta usando topicos.")
    print("1 - Clonar no grupo em si")
    print("2 - Ativar topicos e criar o primeiro topico com o nome da origem")
    choice = input("Escolha (1/2): ").strip() or "1"

    if choice == "2":
        input_channel = client.resolve_peer(target_chat.id)
        client.invoke(
            raw.functions.channels.ToggleForum(
                channel=input_channel,
                enabled=True,
            )
        )
        topic_id = create_forum_topic(client, target_chat.id, source_title)
        destination["thread_id"] = topic_id
        destination["mode_label"] = f"topico:{topic_id}"
        print(f"Forum ativado e topico criado: '{normalize_topic_title(source_title)}' (ID {topic_id}).")
        return destination

    print("O envio sera feito para o grupo em si, sem topico.")
    return destination

def get_channels():
    with Client(session_name) as client:
        channel_source = input("Forneça o @username ou ID do canal / grupo de origem: ")
        channel_target = input("Forneça o @username ou ID do canal de destino: ")
        channel_source = parse_channel_input(channel_source)
        channel_target = parse_channel_input(channel_target)
        source_chat = client.get_chat(channel_source)
        target_chat = client.get_chat(channel_target)
        destination = resolve_target_destination(client, source_chat, target_chat)
        return source_chat.id, destination, source_chat.title

def get_batch_job_count():
    while True:
        raw_value = input("Quantos canais deseja clonar nesta sessão? (1-9): ").strip() or "1"
        try:
            count = int(raw_value)
        except ValueError:
            print("Valor inválido. Digite um número inteiro.")
            continue
        if count < 1 or count > 9:
            print("Informe um número entre 1 e 9.")
            continue
        return count

def collect_batch_jobs(count):
    jobs = []
    for index in range(count):
        print(f"\n=== Clonagem {index + 1} de {count} ===")
        channel_source, destination, chat_title = get_channels()
        custom_caption = get_custom_caption()
        jobs.append({
            "channel_source": channel_source,
            "destination": destination,
            "chat_title": chat_title,
            "custom_caption": custom_caption,
        })
    return jobs

def parse_channel_input(channel_input: str):
    """Parse channel input to determine if it's an ID or username."""
    if channel_input.startswith("@"):
        return channel_input
    else:
        try:
            return int(channel_input)
        except ValueError:
            print("Entrada inválida. Por favor, forneça um ID ou nome de usuário válido.")
            exit()

def limpar_nome_arquivo(nome_arquivo):
    nome_limpo = re.sub(r'[^a-zA-Z0-9]', '_', nome_arquivo or "")
    chars_invalidos = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in chars_invalidos:
        nome_limpo = nome_limpo.replace(char, '_')
    return nome_limpo.strip("._") or "arquivo"

def get_cleaned_file_path(media, directory, message_id=None):
    file_name = getattr(media, "file_name", None)
    if file_name and '.' in file_name:
        extension = file_name.split('.')[-1]
    elif hasattr(media, "mime_type") and media.mime_type:
        mime_type = media.mime_type.lower()
        if "jpeg" in mime_type or "jpg" in mime_type:
            extension = "jpg"
        elif "png" in mime_type:
            extension = "png"
        elif "gif" in mime_type:
            extension = "gif"
        elif "mp4" in mime_type:
            extension = "mp4"
        elif "mpeg" in mime_type or "mp3" in mime_type:
            extension = "mp3"
        elif "ogg" in mime_type:
            extension = "ogg"
        elif "pdf" in mime_type:
            extension = "pdf"
        else:
            extension = "bin"
    elif hasattr(media, "width") and hasattr(media, "height"):
        extension = "jpg"
    else:
        extension = "bin"
    base_name = limpar_nome_arquivo(file_name or getattr(media, "file_unique_id", None) or getattr(media, "file_id", None))
    suffix = f"_{message_id}" if message_id is not None else ""
    return os.path.join(directory, f"{base_name}{suffix}.{extension}")

def format_bytes(num_bytes):
    if num_bytes is None:
        return "tamanho desconhecido"
    units = ["B", "KB", "MB", "GB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024

def build_progress_callback(processed, total, message_id, stage):
    state = {
        "started_at": None,
        "last_update": 0.0,
        "last_current": 0,
        "last_line_length": 0,
    }

    def callback(current, total_bytes):
        now = time.time()
        if state["started_at"] is None:
            state["started_at"] = now

        if current < total_bytes and (now - state["last_update"] < 0.5):
            return

        elapsed = max(now - state["started_at"], 0.001)
        delta_bytes = max(current - state["last_current"], 0)
        delta_time = max(now - state["last_update"], 0.001) if state["last_update"] else elapsed
        speed = delta_bytes / delta_time if delta_time > 0 else 0
        percent = (current / total_bytes * 100) if total_bytes else 0
        remaining_bytes = max(total_bytes - current, 0)
        eta = format_seconds(remaining_bytes / speed) if speed > 0 else "--:--"

        line = (
            f"[{processed}/{total}] Mensagem {message_id} | {stage} | "
            f"{percent:5.1f}% | {format_bytes(current)}/{format_bytes(total_bytes)} | "
            f"{format_bytes(speed)}/s | ETA {eta}"
        )
        padded = line.ljust(state["last_line_length"])
        sys.stdout.write("\r" + padded)
        sys.stdout.flush()

        state["last_update"] = now
        state["last_current"] = current
        state["last_line_length"] = len(padded)

        if total_bytes and current >= total_bytes:
            sys.stdout.write("\n")
            sys.stdout.flush()

    return callback

def media_kind_label(message):
    if message.photo:
        return "foto"
    if message.audio:
        return "audio"
    if message.video:
        return "video"
    if message.document:
        return "arquivo"
    if message.sticker:
        return "sticker"
    if message.animation:
        return "animacao"
    if message.text:
        return "texto"
    return "conteudo"

def get_user_choices():
    print("Quais conteudos você deseja processar?:\n")
    options = ["Processar todos os Conteúdos", "Fotos", "Áudios", "Vídeos", "Arquivos", "Texto", "Sticker", "Animação - GIFs"]
    for i, option in enumerate(options):
        print(f"{i} - {option}")
    choices = input("\nInforme os conteúdos que deseja procesar separados por vírgula (ex: 1,3) < 0 para processar todos > : ").split(',')
    choices = [int(choice.strip()) for choice in choices]
    if 0 in choices:
        choices = [1, 2, 3, 4, 5, 6, 7]
    return choices

def extract_links_from_buttons(reply_markup):
    if not reply_markup or not hasattr(reply_markup, 'inline_keyboard') or not reply_markup.inline_keyboard:
        return ''

    link_texts = []
    for row in reply_markup.inline_keyboard:
        for button in row:
            link_texts.append(f"{button.text} ({button.url})")
    return ' '.join(link_texts)

def get_reply_markup(message):
    return getattr(message, "reply_markup", None)

def extract_text_links_from_caption(message):
    if not hasattr(message, 'caption_entities') or not message.caption_entities:
        return ''

    links = []
    for entity in message.caption_entities:
        if entity.type == "text_link":
            links.append(entity.url)
    return ' '.join(links)

def get_custom_caption():
    caption = input("Digite a legenda personalizada (deixe em branco para manter a legenda original): ")
    return caption #Ask user for a custom caption and return it

def get_caption(message, custom_caption=None):
    caption_texts = []
    
    if custom_caption:
        caption_texts.append(custom_caption)
  
    if message.caption:
        caption_texts.append(str(message.caption))
    
    links_from_buttons = extract_links_from_buttons(message.reply_markup)
    if links_from_buttons:
        caption_texts.append(links_from_buttons)# Adicionar links dos botões
    
    links_from_caption = extract_text_links_from_caption(message)
    if links_from_caption:
        caption_texts.append(links_from_caption)# legenda com hiper-link (text_link), adicionamos ao texto final
    
    if message.text and not links_from_buttons and not links_from_caption:
        caption_texts.append(message.text)# mensagem puramente textual
    return ' '.join(caption_texts).strip()

def split_caption_for_media(text):
    text = (text or "").strip()
    if len(text) <= MEDIA_CAPTION_LIMIT:
        return text, ""

    cut = text.rfind(" ", 0, MEDIA_CAPTION_LIMIT)
    if cut <= 0:
        cut = MEDIA_CAPTION_LIMIT
    media_caption = text[:cut].rstrip()
    overflow_text = text[cut:].lstrip()
    return media_caption, overflow_text

def extract_thumbnail(video_path: str) -> str:
    if not FFMPEG_PATH:
        return ""
    thumbnail_path = video_path + ".jpg"
    thumbnail_command = [
        FFMPEG_PATH,
        "-v", "quiet",
        "-stats",
        "-y",
        "-i", video_path,
        "-ss", "00:00:01",
        "-vframes", "1",
        thumbnail_path,
    ]
    try:
        subprocess.run(
            thumbnail_command,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return thumbnail_path if os.path.exists(thumbnail_path) else ""
    except Exception:
        return ""

def collect_video_duration(video_path: str) -> int:
    if not FFPROBE_PATH:
        return 0
    try:
        ffprobe_command = [
            FFPROBE_PATH,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
        ]
        duration = subprocess.check_output(ffprobe_command).decode("utf-8").strip()
        return int(float(duration))
    except Exception:
        return 0

def safe_remove(path, attempts=8, delay=0.5):
    if not path:
        return True
    for attempt in range(attempts):
        try:
            if os.path.exists(path):
                os.remove(path)
            return True
        except PermissionError:
            if attempt == attempts - 1:
                return False
            time.sleep(delay)
    return False

def format_seconds(seconds):
    seconds = max(0, int(seconds))
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"

def print_overall_progress(processed, total, started_at, label, success=True):
    elapsed = time.time() - started_at
    avg = (elapsed / processed) if processed else 0
    remaining = max(0, total - processed)
    eta = format_seconds(avg * remaining) if processed else "--:--"
    status = "OK" if success else "ERRO"
    print(
        f"[{processed}/{total}] {status} | restantes: {remaining} | "
        f"decorrido: {format_seconds(elapsed)} | ETA: {eta} | {label}"
    )

def print_stage(processed, total, message_id, text):
    print(f"[{processed}/{total}] Mensagem {message_id}: {text}")

def simplify_error(error):
    text = str(error)
    upper = text.upper()
    if "FILE_REFERENCE_EXPIRED" in upper:
        return "o Telegram expirou o acesso temporario a esta midia."
    if "PEER ID INVALID" in upper:
        return "o canal de destino nao foi reconhecido pela sessao."
    if "FLOOD" in upper:
        return "o Telegram limitou temporariamente o envio."
    if "WINERROR 32" in upper:
        return "o Windows ainda estava usando o arquivo temporario."
    if "MEDIA_CAPTION_TOO_LONG" in upper:
        return "a legenda ficou grande demais para uma midia."
    if "REPLY_MARKUP_INVALID" in upper:
        return "o Telegram nao aceitou os botoes desta mensagem."
    return text

def is_retryable_failure_error(error_text):
    normalized = (error_text or "").strip().lower()
    if not normalized:
        return False
    retryable_tokens = (
        "temporariamente",
        "temporario",
        "flood",
        "expirou o acesso temporario",
        "getfile",
        "unable to connect",
        "network",
        "timed out",
        "connection reset",
        "winerror 1231",
        "windows ainda estava usando o arquivo",
        "winerror 32",
    )
    return any(token in normalized for token in retryable_tokens)

def is_source_retryable_error(error):
    if isinstance(error, FloodWait):
        return True
    error_text = str(error).upper()
    return (
        "FLOOD" in error_text
        or "GETFILE" in error_text
        or "FILE_REFERENCE_EXPIRED" in error_text
        or is_transport_error(error)
    )

def reset_item_for_retry(item):
    item.state = "pending"
    item.error = ""
    item.last_wait_reason = ""
    item.wait_reason_started_at = None
    item.download_queued_at = None
    item.preupload_queued_at = None
    item.download_started_at = None
    item.download_finished_at = None
    item.preupload_started_at = None
    item.preupload_finished_at = None
    item.publish_started_at = None
    item.publish_finished_at = None
    item.send_started_at = None
    item.send_finished_at = None
    item.acked_at = None
    item.remote_media = None
    item.remote_media_group = []
    item.local_paths = []
    item.aux_paths = []
    item.source_mode_final = ""

def measure_paths_size(paths):
    total = 0
    for path in paths:
        if path and os.path.exists(path):
            total += os.path.getsize(path)
    return total

def get_media_size(message):
    media = get_message_media(message)
    return getattr(media, "file_size", 0) or 0

def estimate_single_message_bytes(message):
    if message.text:
        return 0
    return get_media_size(message) + RESERVE_MARGIN_SINGLE

def estimate_album_bytes(messages):
    return sum(get_media_size(message) for message in messages) + RESERVE_MARGIN_ALBUM

def format_phase_duration(started_at, finished_at):
    if not started_at or not finished_at:
        return "00:00"
    return format_seconds(finished_at - started_at)

def format_absolute_timestamp(timestamp_value):
    if not timestamp_value:
        return "--:--:--"
    return time.strftime("%H:%M:%S", time.localtime(timestamp_value))

def format_rate(num_bytes, started_at, finished_at):
    if not num_bytes or not started_at or not finished_at:
        return "--"
    elapsed = max(0.001, finished_at - started_at)
    mib_per_second = (num_bytes / (1024 * 1024)) / elapsed
    return f"{mib_per_second:.2f} MiB/s"

def queue_size_without_sentinels(queue):
    return sum(1 for _, _, item in queue._queue if item is not None)

def queue_items_ahead(queue, item_seq):
    count = 0
    for _, seq, queued_item in queue._queue:
        if queued_item is None:
            continue
        if seq < item_seq:
            count += 1
    return count

def should_log_large_relay(item, file_size=0):
    return max(item.estimated_bytes or 0, file_size or 0) >= LARGE_RELAY_DIAGNOSTIC_BYTES

def get_source_budget_bucket(file_size):
    if file_size and file_size >= SOURCE_READ_LARGE_BYTES:
        return "large"
    return "small"

def get_source_budget_bucket_label(bucket):
    if bucket == "large":
        return "grande"
    return "pequena"

def get_or_create_source_budget_state(context, dc_id):
    state = context.source_budget_by_dc.get(dc_id)
    if state is None:
        state = SourceReadDcBudgetState()
        context.source_budget_by_dc[dc_id] = state
    return state

def get_effective_source_budget_large_limit(state, context=None):
    base_limit = max(1, get_adaptive_source_large_max(context) if context is not None else SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC)
    adaptive_budget = get_adaptive_source_session_budget(context) if context is not None else SOURCE_READ_SESSION_BUDGET
    adaptive_burst = get_adaptive_source_large_burst(context) if context is not None else SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC_BURST
    burst_limit = max(base_limit, min(adaptive_budget, adaptive_burst))
    current_limit = max(base_limit, min(state.active_large_readers, burst_limit))
    if burst_limit <= base_limit:
        return current_limit
    if state.waiting_large_readers > 0:
        return max(current_limit, burst_limit)
    return current_limit

def can_acquire_source_budget(context, dc_id, session_count, bucket, file_size=None):
    if not SOURCE_BUDGET_ENABLED:
        return True
    state = get_or_create_source_budget_state(context, dc_id)
    adaptive_budget = get_adaptive_source_session_budget(context)
    adaptive_small = get_adaptive_source_small_max(context)

    if session_count > adaptive_budget:
        sessions_ok = state.active_sessions == 0
    else:
        sessions_ok = (state.active_sessions + session_count) <= adaptive_budget
    if not sessions_ok:
        return False

    # Quando help.getAppConfig confirmou os caps da API, tratamos esse limite como
    # fato do servidor e o respeitamos desde o tier 0. Sem esse sinal, continuamos
    # usando o comportamento adaptativo e so endurecemos sob pressao real.
    if should_enforce_telegram_queue_caps(context):
        tg_queue = classify_tg_queue(file_size if file_size is not None else 0)
        if tg_queue == "tg_large" and state.active_tg_large >= get_effective_telegram_queue_cap(context, "tg_large"):
            return False
        if tg_queue == "tg_small" and state.active_tg_small >= get_effective_telegram_queue_cap(context, "tg_small"):
            return False

    if bucket == "large":
        return state.active_large_readers < get_effective_source_budget_large_limit(state, context)
    return state.active_small_readers < adaptive_small

async def acquire_source_budget(context, item, dc_id, session_count, file_size):
    bucket = get_source_budget_bucket(file_size)
    if not SOURCE_BUDGET_ENABLED:
        return bucket
    bucket_label = get_source_budget_bucket_label(bucket)
    tg_queue = classify_tg_queue(file_size)
    waited = False
    wait_started_at = None
    waiter_registered = False
    async with context.source_budget_condition:
        try:
            while not can_acquire_source_budget(context, dc_id, session_count, bucket, file_size):
                state = get_or_create_source_budget_state(context, dc_id)
                if not waited:
                    waited = True
                    wait_started_at = time.time()
                    mark_wait_reason(context, item, "waiting_source_budget")
                    if bucket == "large" and not waiter_registered:
                        state.waiting_large_readers += 1
                        waiter_registered = True
                    record_analytics_event(
                        context,
                        "source_budget_wait_start",
                        item=item,
                        dc_id=dc_id,
                        bucket=bucket,
                        session_count=session_count,
                    )
                    tier = get_flood_tier(context)
                    await log_context(
                        context,
                        f"[SOURCE] {item.label}: aguardando orcamento da origem | dc {dc_id} | faixa {bucket_label} | "
                        f"precisa {session_count} slot(s) | ativos dc: slots {state.active_sessions}/{get_adaptive_source_session_budget(context)}, "
                        f"pequenas {state.active_small_readers}/{get_adaptive_source_small_max(context)}, "
                        f"grandes {state.active_large_readers}/{get_effective_source_budget_large_limit(state, context)}, "
                        f"tg_large {state.active_tg_large}/{get_effective_telegram_queue_cap(context, 'tg_large')}, "
                        f"tg_small {state.active_tg_small}/{get_effective_telegram_queue_cap(context, 'tg_small')}"
                        f"{f', espera_grande {state.waiting_large_readers}' if bucket == 'large' else ''}"
                        f"{f', flood_tier {tier}' if tier > 0 else ''}",
                    )
                    if can_acquire_source_budget(context, dc_id, session_count, bucket, file_size):
                        continue
                await context.source_budget_condition.wait()

            state = get_or_create_source_budget_state(context, dc_id)
            if bucket == "large" and waiter_registered:
                state.waiting_large_readers = max(0, state.waiting_large_readers - 1)
                waiter_registered = False
            state.active_sessions += session_count
            if bucket == "large":
                state.active_large_readers += 1
            else:
                state.active_small_readers += 1
            if tg_queue == "tg_large":
                state.active_tg_large += 1
            else:
                state.active_tg_small += 1

            if waited:
                wait_seconds = max(0.0, time.time() - (wait_started_at or time.time()))
                state_item = ensure_item_analytics_state(context, item)
                state_item["source_budget_wait_seconds"] += wait_seconds
                state_item["source_budget_wait_count"] += 1
                update_item_source_wait_totals(state_item)
                record_analytics_event(
                    context,
                    "source_budget_wait_done",
                    item=item,
                    dc_id=dc_id,
                    bucket=bucket,
                    waited_seconds=wait_seconds,
                )
                await log_context(
                    context,
                    f"[SOURCE] {item.label}: orcamento da origem concedido | dc {dc_id} | faixa {bucket_label} | "
                    f"slots dc {state.active_sessions}/{get_adaptive_source_session_budget(context)}",
                )
        except Exception:
            if bucket == "large" and waiter_registered:
                state = get_or_create_source_budget_state(context, dc_id)
                state.waiting_large_readers = max(0, state.waiting_large_readers - 1)
                context.source_budget_condition.notify_all()
            raise
    return bucket

async def release_source_budget(context, dc_id, session_count, bucket, file_size=None):
    if not SOURCE_BUDGET_ENABLED:
        return
    tg_queue = classify_tg_queue(file_size if file_size is not None else 0)
    async with context.source_budget_condition:
        state = get_or_create_source_budget_state(context, dc_id)
        state.active_sessions = max(0, state.active_sessions - session_count)
        if bucket == "large":
            state.active_large_readers = max(0, state.active_large_readers - 1)
        else:
            state.active_small_readers = max(0, state.active_small_readers - 1)
        if tg_queue == "tg_large":
            state.active_tg_large = max(0, state.active_tg_large - 1)
        else:
            state.active_tg_small = max(0, state.active_tg_small - 1)
        if (
            state.active_sessions == 0
            and state.active_small_readers == 0
            and state.active_large_readers == 0
            and state.waiting_large_readers == 0
            and state.active_tg_large == 0
            and state.active_tg_small == 0
        ):
            context.source_budget_by_dc.pop(dc_id, None)
        context.source_budget_condition.notify_all()

async def get_source_budget_status_snapshot(context):
    async with context.source_budget_condition:
        states = list(context.source_budget_by_dc.values())

    active_dc_count = 0
    active_sessions = 0
    active_small = 0
    active_large = 0
    for state in states:
        if state.active_sessions <= 0 and state.active_small_readers <= 0 and state.active_large_readers <= 0:
            continue
        active_dc_count += 1
        active_sessions += state.active_sessions
        active_small += state.active_small_readers
        active_large += state.active_large_readers

    return (
        f"source_dc={active_dc_count} source_slots={active_sessions} "
        f"(peq {active_small}, grandes {active_large})"
    )

async def get_source_budget_snapshot_data(context):
    async with context.source_budget_condition:
        states = list(context.source_budget_by_dc.values())

    active_dc_count = 0
    active_sessions = 0
    active_small = 0
    active_large = 0
    waiting_large = 0
    for state in states:
        if (
            state.active_sessions <= 0
            and state.active_small_readers <= 0
            and state.active_large_readers <= 0
            and state.waiting_large_readers <= 0
        ):
            continue
        active_dc_count += 1
        active_sessions += state.active_sessions
        active_small += state.active_small_readers
        active_large += state.active_large_readers
        waiting_large += state.waiting_large_readers

    return {
        "source_dc": active_dc_count,
        "source_slots": active_sessions,
        "source_small": active_small,
        "source_large": active_large,
        "source_wait_large": waiting_large,
        "status": (
            f"source_dc={active_dc_count} source_slots={active_sessions} "
            f"(peq {active_small}, grandes {active_large}, espera_grande {waiting_large})"
        ),
    }

def get_stream_relay_snapshot_data(context):
    large_limit = get_effective_stream_relay_large_limit(context)
    return {
        "large_limit": large_limit,
        "slot_waiters": context.waiting_stream_relay_slots,
        "slot_waiters_large": context.waiting_large_stream_relay_slots,
        "slot_waiters_huge": context.waiting_huge_stream_relay_slots,
        "status": (
            f"stream_active={context.active_stream_relays}/{STREAM_RELAY_MAX_ACTIVE} "
            f"(grandes {context.active_large_stream_relays}/{large_limit}, "
            f"gigantes {context.active_huge_stream_relays}/{STREAM_RELAY_MAX_HUGE_ACTIVE}, "
            f"espera_slot {context.waiting_stream_relay_slots})"
        ),
    }

def get_stream_relay_size_bucket(item):
    item_size = item.estimated_bytes or 0
    if item_size >= STREAM_RELAY_HUGE_BYTES:
        return "gigante"
    if item_size >= STREAM_RELAY_LARGE_BYTES:
        return "grande"
    return "normal"

def get_effective_stream_relay_large_limit(context):
    base_limit = max(1, STREAM_RELAY_MAX_LARGE_ACTIVE)
    burst_limit = max(base_limit, min(STREAM_RELAY_MAX_ACTIVE, STREAM_RELAY_MAX_LARGE_ACTIVE_BURST))
    current_limit = max(base_limit, min(context.active_large_stream_relays, burst_limit))
    if burst_limit <= base_limit:
        return current_limit
    if context.active_huge_stream_relays > 0 or context.waiting_huge_stream_relay_slots > 0:
        return current_limit
    if context.waiting_large_stream_relay_slots > 0:
        return max(current_limit, burst_limit)
    return current_limit

def increment_stream_relay_waiter(context, size_bucket):
    context.waiting_stream_relay_slots += 1
    if size_bucket == "grande":
        context.waiting_large_stream_relay_slots += 1
    elif size_bucket == "gigante":
        context.waiting_huge_stream_relay_slots += 1

def decrement_stream_relay_waiter(context, size_bucket):
    context.waiting_stream_relay_slots = max(0, context.waiting_stream_relay_slots - 1)
    if size_bucket == "grande":
        context.waiting_large_stream_relay_slots = max(0, context.waiting_large_stream_relay_slots - 1)
    elif size_bucket == "gigante":
        context.waiting_huge_stream_relay_slots = max(0, context.waiting_huge_stream_relay_slots - 1)

def can_acquire_stream_relay_slot(context, item):
    if context.active_stream_relays >= STREAM_RELAY_MAX_ACTIVE:
        return False

    size_bucket = get_stream_relay_size_bucket(item)
    if size_bucket == "gigante":
        if context.active_huge_stream_relays >= STREAM_RELAY_MAX_HUGE_ACTIVE:
            return False
        if context.active_large_stream_relays >= get_effective_stream_relay_large_limit(context):
            return False
    elif size_bucket == "grande":
        if context.active_large_stream_relays >= get_effective_stream_relay_large_limit(context):
            return False

    return True

def should_reserve_stream_relay_for_head(context, item):
    head_item = get_head_item(context)
    if head_item is None or head_item.seq == item.seq:
        return False
    if not head_item.stream_relay:
        return False
    if is_item_ready_for_publish(head_item):
        return False

    head_reason = (head_item.last_wait_reason or "").lower()
    if "waiting_for_stream_relay_slot" in head_reason:
        return True

    if head_item.state == "failed" and is_retryable_failure_error(head_item.error):
        return True

    return False

async def acquire_stream_relay_slot(context, item):
    wait_started_at = None
    size_bucket = get_stream_relay_size_bucket(item)
    waiter_registered = False
    async with context.stream_relay_condition:
        try:
            while (
                not can_acquire_stream_relay_slot(context, item)
                or should_reserve_stream_relay_for_head(context, item)
            ):
                if wait_started_at is None:
                    mark_wait_reason(context, item, "waiting_for_stream_relay_slot")
                    wait_started_at = time.time()
                    if not waiter_registered:
                        increment_stream_relay_waiter(context, size_bucket)
                        waiter_registered = True
                    record_analytics_event(
                        context,
                        "stream_relay_slot_wait_start",
                        item=item,
                        size_bucket=size_bucket,
                    )
                    await log_context(
                        context,
                        f"[PRE] {item.label}: aguardando slot de stream relay | faixa {size_bucket} | "
                        f"ativos {context.active_stream_relays}/{STREAM_RELAY_MAX_ACTIVE} | "
                        f"grandes {context.active_large_stream_relays}/{get_effective_stream_relay_large_limit(context)} | "
                        f"gigantes {context.active_huge_stream_relays}/{STREAM_RELAY_MAX_HUGE_ACTIVE} | "
                        f"espera_slot {context.waiting_stream_relay_slots}",
                    )
                    if (
                        can_acquire_stream_relay_slot(context, item)
                        and not should_reserve_stream_relay_for_head(context, item)
                    ):
                        continue
                await context.stream_relay_condition.wait()

            if waiter_registered:
                decrement_stream_relay_waiter(context, size_bucket)
                waiter_registered = False

            context.active_stream_relays += 1
            if size_bucket in ("grande", "gigante"):
                context.active_large_stream_relays += 1
            if size_bucket == "gigante":
                context.active_huge_stream_relays += 1
        except Exception:
            if waiter_registered:
                decrement_stream_relay_waiter(context, size_bucket)
                context.stream_relay_condition.notify_all()
            raise

    if wait_started_at is not None:
        wait_seconds = max(0.0, time.time() - wait_started_at)
        state = ensure_item_analytics_state(context, item)
        state["stream_slot_wait_seconds"] += wait_seconds
        state["stream_slot_wait_count"] += 1
        record_analytics_event(
            context,
            "stream_relay_slot_wait_done",
            item=item,
            size_bucket=size_bucket,
            waited_seconds=wait_seconds,
        )
        await log_context(
            context,
            f"[PRE] {item.label}: slot de stream relay liberado apos {format_seconds(time.time() - wait_started_at)} | faixa {size_bucket}",
        )
    mark_wait_reason(context, item, "stream_relay_active")

async def release_stream_relay_slot(context, item):
    size_bucket = get_stream_relay_size_bucket(item)
    async with context.stream_relay_condition:
        context.active_stream_relays = max(0, context.active_stream_relays - 1)
        if size_bucket in ("grande", "gigante"):
            context.active_large_stream_relays = max(0, context.active_large_stream_relays - 1)
        if size_bucket == "gigante":
            context.active_huge_stream_relays = max(0, context.active_huge_stream_relays - 1)
        context.stream_relay_condition.notify_all()

async def invoke_relay_upload_media(upload_client, context, item, rpc):
    item.relay_finalize_started_at = time.time()
    mark_wait_reason(context, item, "upload_media_finalize")
    if should_log_large_relay(item):
        await log_context(
            context,
            f"[RELAY] {item.label}: inicio finalize UploadMedia",
        )

    uploaded = await call_telegram(
        upload_client.invoke,
        rpc,
        _metrics_context=context,
        _trace_item=item,
        _trace_phase="upload_media_finalize",
    )

    item.relay_finalize_finished_at = time.time()
    mark_wait_reason(context, item, "")
    if should_log_large_relay(item):
        await log_context(
            context,
            f"[RELAY] {item.label}: fim finalize UploadMedia | dur "
            f"{format_phase_duration(item.relay_finalize_started_at, item.relay_finalize_finished_at)}",
        )
    return uploaded

def format_budget_usage(context):
    return f"{format_bytes(context.disk_budget.current_bytes())}/{format_bytes(context.disk_budget.max_bytes)}"

def normalize_log_mode(value):
    value = (value or "quiet").strip().lower()
    if value not in {"quiet", "normal", "diag", "trace"}:
        return "quiet"
    return value

def classify_log_message(message):
    if re.match(r"^\[\d+/\d+\]", message):
        return "PROGRESS"

    prefix_match = re.match(r"^\[([^\]]+)\]", message)
    if not prefix_match:
        return "OTHER"

    prefix = prefix_match.group(1)
    if prefix.startswith("DL") and prefix[2:].isdigit():
        return "DL"
    if prefix.startswith("PRE") and prefix[3:].isdigit():
        return "PRE"
    if prefix in {
        "STATUS",
        "WAIT",
        "METRICS",
        "PIN",
        "QUEUE",
        "PUB",
        "RELAY",
        "SOURCE",
        "START",
        "PRE",
        "DL",
    }:
        return prefix
    return "OTHER"

def should_emit_console_message(context, message, category):
    if context is None:
        return True

    if context.log_mode == "trace":
        return True

    lowered = message.lower()
    if "falhou" in lowered or "interrompendo job" in lowered:
        return True

    if context.log_mode == "quiet":
        return category == "PROGRESS"

    if context.log_mode == "diag":
        return category not in {"DL", "PRE", "QUEUE"}

    return category in {"STATUS", "WAIT", "METRICS", "PIN", "PROGRESS", "START", "OTHER"}

def build_detailed_log_path(chat_title, destination):
    if not DETAILED_LOG_TO_FILE:
        return None

    os.makedirs(DETAILED_LOG_DIR, exist_ok=True)
    run_label = os.path.basename(run_download_path) if run_download_path else build_run_id("forward_log")
    base_name = (
        f"tmp_{run_label}_{chat_title}_{destination['chat_id']}_{destination.get('thread_id') or 'chat'}"
    )
    safe_name = re.sub(r"[^a-zA-Z0-9._-]+", "_", base_name).strip("._") or "tmp_forward_log"
    return os.path.abspath(os.path.join(DETAILED_LOG_DIR, f"{safe_name}.log"))

def configure_detailed_logging(context):
    context.log_mode = normalize_log_mode(LOG_MODE)
    context.detailed_log_path = build_detailed_log_path(context.chat_title, context.destination)

    if not context.detailed_log_path:
        pyrogram_logger = logging.getLogger(PYROGRAM_SESSION_LOGGER_NAME)
        context.pyrogram_wait_logger_propagate = pyrogram_logger.propagate
        pyrogram_logger.propagate = False
        return

    context.detailed_log_stream = open(context.detailed_log_path, "a", encoding="utf-8")
    pyrogram_logger = logging.getLogger(PYROGRAM_SESSION_LOGGER_NAME)
    context.pyrogram_wait_logger_propagate = pyrogram_logger.propagate
    pyrogram_logger.propagate = False

    handler = logging.StreamHandler(context.detailed_log_stream)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [PYROGRAM] %(message)s", datefmt="%H:%M:%S"))
    handler._tg_mirror_temp = True
    pyrogram_logger.handlers = [existing for existing in pyrogram_logger.handlers if not getattr(existing, "_tg_mirror_temp", False)]
    pyrogram_logger.addHandler(handler)
    context.pyrogram_wait_handler = handler

def close_detailed_logging(context):
    uninstall_pyrogram_flood_handler(context)
    pyrogram_logger = logging.getLogger(PYROGRAM_SESSION_LOGGER_NAME)
    if context.pyrogram_wait_handler is not None:
        try:
            pyrogram_logger.removeHandler(context.pyrogram_wait_handler)
        except Exception:
            pass
        try:
            context.pyrogram_wait_handler.close()
        except Exception:
            pass
        context.pyrogram_wait_handler = None

    pyrogram_logger.propagate = context.pyrogram_wait_logger_propagate

    if context.detailed_log_stream is not None:
        try:
            context.detailed_log_stream.close()
        except Exception:
            pass
        context.detailed_log_stream = None

def write_detailed_log_line(context, line):
    if context is None or context.detailed_log_stream is None:
        return
    context.detailed_log_stream.write(f"{line}\n")

async def log_context(context, message):
    async with context.log_lock:
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        write_detailed_log_line(context, line)
        category = classify_log_message(message)
        if should_emit_console_message(context, message, category):
            print(line)

def build_work_items(messages, choices):
    work_queue = build_work_queue(messages, choices)
    items = []
    for seq, (kind, payload) in enumerate(work_queue, start=1):
        if kind == "album":
            item = WorkItem(
                seq=seq,
                kind="album",
                messages=list(payload),
                estimated_bytes=estimate_album_bytes(payload),
                label=f"Album {getattr(payload[0], 'media_group_id', 'sem_id')}",
                media_kind="album",
            )
            item.stream_relay = should_use_stream_relay(item)
            items.append(item)
        else:
            message = payload
            item = WorkItem(
                seq=seq,
                kind="message",
                messages=[message],
                estimated_bytes=estimate_single_message_bytes(message),
                label=f"Mensagem {message.id} ({media_kind_label(message)})",
                media_kind=media_kind_label(message),
            )
            item.stream_relay = should_use_stream_relay(item)
            items.append(item)
    return items

def get_head_item(context):
    if 1 <= context.next_seq_to_publish <= context.total_items:
        return context.work_items[context.next_seq_to_publish - 1]
    return None

def is_item_ready_for_publish(item):
    return item.state in ("ready", "failed", "done")

def is_head_blocked(context):
    head_item = get_head_item(context)
    return bool(head_item and not is_item_ready_for_publish(head_item))

def schedule_target_seq(context):
    return min(context.total_items, context.next_seq_to_publish + SCHEDULE_LOOKAHEAD - 1)

def queue_lane_label(context, item):
    if item.seq == context.next_seq_to_publish:
        return "faixa rapida"
    if item.seq <= context.next_seq_to_publish + HEAD_PROTECTED_ITEMS - 1:
        return "prioridade proxima"
    if item.seq <= context.next_seq_to_publish + SIZE_AWARE_HORIZON:
        return "prioridade longa"
    return "fundo"

async def enqueue_download_item(context, item):
    item.download_queued_at = time.time()
    item.download_queue_depth_at_enqueue = queue_size_without_sentinels(context.download_queue)
    item.download_items_ahead_at_enqueue = queue_items_ahead(context.download_queue, item.seq)
    await context.download_queue.put((build_queue_priority(context, item), item.seq, item))

async def enqueue_preupload_item(context, item):
    item.preupload_queued_at = time.time()
    item.preupload_queue_depth_at_enqueue = queue_size_without_sentinels(context.preupload_queue)
    item.preupload_items_ahead_at_enqueue = queue_items_ahead(context.preupload_queue, item.seq)
    await context.preupload_queue.put((build_queue_priority(context, item), item.seq, item))

def is_head_priority_item(context, item):
    return item.seq <= min(context.total_items, context.next_seq_to_publish + HEAD_PROTECTED_ITEMS - 1)

def get_size_band(num_bytes):
    mb = num_bytes / (1024 * 1024)
    if mb < 100:
        return "lt100"
    if mb < 300:
        return "100_300"
    if mb < 700:
        return "300_700"
    return "700_plus"

def get_eta_kind(item):
    if item.kind == "album":
        return "album"
    media_kind = (item.media_kind or "").strip().lower()
    kind_map = {
        "video": "video",
        "arquivo": "arquivo",
        "foto": "foto",
        "audio": "audio",
        "sticker": "sticker",
        "animacao": "animacao",
    }
    return kind_map.get(media_kind, "arquivo")

def build_eta_key(item):
    return (get_eta_kind(item), get_size_band(item.estimated_bytes))

def estimate_preupload_eta_seconds(context, item):
    eta_key = build_eta_key(item)
    history = context.preupload_eta_history.get(eta_key)
    if history:
        return sum(history) / len(history)

    kind_only_values = []
    item_kind = eta_key[0]
    for (kind, _), values in context.preupload_eta_history.items():
        if kind == item_kind:
            kind_only_values.extend(values)
    if kind_only_values:
        return sum(kind_only_values) / len(kind_only_values)

    fallback_seconds = ETA_FALLBACK_SECONDS_BY_KIND.get(item_kind, 60.0)
    estimated_mb = max(1.0, item.estimated_bytes / (1024 * 1024))
    size_factor = {
        "lt100": 0.6,
        "100_300": 1.0,
        "300_700": 1.8,
        "700_plus": 3.0,
    }[get_size_band(item.estimated_bytes)]
    return max(fallback_seconds, fallback_seconds * size_factor, estimated_mb / 4.0)

def record_preupload_eta(context, item):
    if not item.preupload_started_at or not item.preupload_finished_at:
        return
    duration = max(0.0, item.preupload_finished_at - item.preupload_started_at)
    context.preupload_eta_history[build_eta_key(item)].append(duration)

def build_queue_priority(context, item):
    distance = max(0, item.seq - context.next_seq_to_publish)
    if distance == 0:
        return (0, 0, item.seq)
    if distance <= HEAD_PROTECTED_ITEMS - 1:
        return (1, distance, item.seq)
    if distance <= SIZE_AWARE_HORIZON:
        return (2, -estimate_preupload_eta_seconds(context, item), item.seq)
    return (3, distance, item.seq)

async def notify_pipeline_slots(context):
    async with context.download_slot_condition:
        context.download_slot_condition.notify_all()
    async with context.preupload_slot_condition:
        context.preupload_slot_condition.notify_all()

def refresh_pending_queue_priorities(context):
    for queue in (context.download_queue, context.preupload_queue):
        refreshed_entries = []
        for _, seq, item in queue._queue:
            if item is None:
                refreshed_entries.append(((float("inf"), float("inf"), float("inf")), seq, item))
                continue
            refreshed_entries.append((build_queue_priority(context, item), seq, item))
        queue._queue[:] = refreshed_entries
        heapq.heapify(queue._queue)

def describe_head_block(context):
    head_item = get_head_item(context)
    if not head_item:
        return "sem_head"
    ready_behind = sum(1 for seq, item in context.ready_items.items() if seq > head_item.seq and item.state == "ready")
    wait_reason = head_item.last_wait_reason or head_item.state
    head_mode = get_item_source_mode(head_item)
    head_dc = head_item.source_dc_id if head_item.source_dc_id is not None else "--"
    head_age = format_seconds(get_item_head_age_seconds(head_item))
    phase_age = format_seconds(get_item_wait_reason_age_seconds(head_item))
    return (
        f"head={head_item.label} estado={head_item.state} modo={head_mode} dc={head_dc} "
        f"idade={head_age} fase={phase_age} motivo={wait_reason} prontos_atras={ready_behind}"
    )

def get_head_block_data(context):
    head_item = get_head_item(context)
    if not head_item:
        return {
            "head_seq": None,
            "head_label": None,
            "head_state": None,
            "head_mode": None,
            "head_dc": None,
            "head_age_seconds": 0.0,
            "head_phase_age_seconds": 0.0,
            "head_reason": None,
            "prontos_atras": 0,
            "text": "sem_head",
        }
    ready_behind = sum(1 for seq, item in context.ready_items.items() if seq > head_item.seq and item.state == "ready")
    wait_reason = head_item.last_wait_reason or head_item.state
    head_mode = get_item_source_mode(head_item)
    head_dc = head_item.source_dc_id if head_item.source_dc_id is not None else None
    head_age_seconds = get_item_head_age_seconds(head_item)
    head_phase_age_seconds = get_item_wait_reason_age_seconds(head_item)
    return {
        "head_seq": head_item.seq,
        "head_label": head_item.label,
        "head_state": head_item.state,
        "head_mode": head_mode,
        "head_dc": head_dc,
        "head_age_seconds": head_age_seconds,
        "head_phase_age_seconds": head_phase_age_seconds,
        "head_reason": wait_reason,
        "prontos_atras": ready_behind,
        "text": (
            f"head={head_item.label} estado={head_item.state} modo={head_mode} "
            f"dc={head_dc if head_dc is not None else '--'} idade={format_seconds(head_age_seconds)} "
            f"fase={format_seconds(head_phase_age_seconds)} "
            f"motivo={wait_reason} prontos_atras={ready_behind}"
        ),
    }

async def pipeline_status_loop(context):
    try:
        while context.next_seq_to_publish <= context.total_items:
            await asyncio.sleep(STATUS_LOG_INTERVAL_SECONDS)
            if context.next_seq_to_publish > context.total_items:
                break
            download_q = queue_size_without_sentinels(context.download_queue)
            preupload_q = queue_size_without_sentinels(context.preupload_queue)
            source_budget_data = await get_source_budget_snapshot_data(context)
            stream_relay_data = get_stream_relay_snapshot_data(context)
            head_data = get_head_block_data(context)
            context.analytics_status_snapshots.append(
                {
                    "ts": time.time(),
                    "download_q": download_q,
                    "preupload_q": preupload_q,
                    "ready": len(context.ready_items),
                    "dl_active": context.active_download_workers,
                    "dl_max": DOWNLOAD_WORKERS,
                    "pre_active": context.active_preupload_workers,
                    "pre_max": PREUPLOAD_WORKERS,
                    "stream_active": context.active_stream_relays,
                    "stream_max": STREAM_RELAY_MAX_ACTIVE,
                    "large_active": context.active_large_stream_relays,
                    "large_max": stream_relay_data["large_limit"],
                    "huge_active": context.active_huge_stream_relays,
                    "huge_max": STREAM_RELAY_MAX_HUGE_ACTIVE,
                    "slot_waiters": stream_relay_data["slot_waiters"],
                    "slot_waiters_large": stream_relay_data["slot_waiters_large"],
                    "slot_waiters_huge": stream_relay_data["slot_waiters_huge"],
                    "source_dc": source_budget_data["source_dc"],
                    "source_slots": source_budget_data["source_slots"],
                    "source_small": source_budget_data["source_small"],
                    "source_large": source_budget_data["source_large"],
                    "source_wait_large": source_budget_data["source_wait_large"],
                    "head_seq": head_data["head_seq"],
                    "head_label": head_data["head_label"],
                    "head_state": head_data["head_state"],
                    "head_mode": head_data["head_mode"],
                    "head_dc": head_data["head_dc"],
                    "head_age_seconds": head_data["head_age_seconds"],
                    "head_phase_age_seconds": head_data["head_phase_age_seconds"],
                    "head_reason": head_data["head_reason"],
                    "prontos_atras": head_data["prontos_atras"],
                }
            )
            await log_context(
                context,
                f"[STATUS] dl_q={download_q} pre_q={preupload_q} ready={len(context.ready_items)} "
                f"dl_active={context.active_download_workers}/{DOWNLOAD_WORKERS} "
                f"pre_active={context.active_preupload_workers}/{PREUPLOAD_WORKERS} "
                f"stream_active={context.active_stream_relays}/{STREAM_RELAY_MAX_ACTIVE} "
                f"(grandes {context.active_large_stream_relays}/{stream_relay_data['large_limit']}, "
                f"gigantes {context.active_huge_stream_relays}/{STREAM_RELAY_MAX_HUGE_ACTIVE}, "
                f"espera_slot {stream_relay_data['slot_waiters']}) "
                f"{source_budget_data['status']} | "
                f"{head_data['text']}",
            )
    except asyncio.CancelledError:
        raise

async def acquire_background_slot(context, stage_name, item):
    if item.seq == context.next_seq_to_publish:
        return False

    if stage_name == "download":
        condition = context.download_slot_condition
        attr_name = "active_background_downloads"
        limit = max(0, DOWNLOAD_WORKERS - 1)
    else:
        condition = context.preupload_slot_condition
        attr_name = "active_background_preuploads"
        limit = max(0, PREUPLOAD_WORKERS - 1)

        head_item = get_head_item(context)
        if head_item is not None:
            ready_behind = sum(
                1
                for seq, queued_item in context.ready_items.items()
                if seq > head_item.seq and queued_item.state == "ready"
            )
            head_reason = (head_item.last_wait_reason or "").lower()
            pressure_reasons = (
                "waiting_source_budget",
                "waiting_for_stream_relay_slot",
                "source_read",
                "queue_backpressure",
            )
            if ready_behind >= HEAD_PRESSURE_READY_BACKLOG_THRESHOLD and any(
                token in head_reason for token in pressure_reasons
            ):
                # Under sustained source/relay pressure, keep fewer background preuploads
                # to protect head progress and reduce HOL amplification.
                limit = min(limit, HEAD_PRESSURE_BACKGROUND_PREUPLOAD_LIMIT)

    async with condition:
        while is_head_blocked(context) and getattr(context, attr_name) >= limit:
            mark_wait_reason(context, item, f"waiting_for_{stage_name}_slot")
            await condition.wait()
        setattr(context, attr_name, getattr(context, attr_name) + 1)
    return True

async def release_background_slot(context, stage_name, acquired_slot):
    if not acquired_slot:
        return

    if stage_name == "download":
        condition = context.download_slot_condition
        attr_name = "active_background_downloads"
    else:
        condition = context.preupload_slot_condition
        attr_name = "active_background_preuploads"

    async with condition:
        setattr(context, attr_name, max(0, getattr(context, attr_name) - 1))
        condition.notify_all()

async def schedule_more_items(context):
    async with context.schedule_lock:
        target_seq = schedule_target_seq(context)
        while context.scheduled_until_seq < target_seq:
            item = context.work_items[context.scheduled_until_seq]
            context.scheduled_until_seq += 1
            if item.needs_download:
                await enqueue_download_item(context, item)
            else:
                item.state = "ready"
                async with context.ready_condition:
                    context.ready_items[item.seq] = item
                    context.ready_condition.notify_all()
        refresh_pending_queue_priorities(context)

async def call_telegram(operation, *args, _metrics_context=None, _trace_item=None, _trace_phase=None, **kwargs):
    method_name = getattr(operation, "__qualname__", getattr(operation, "__name__", operation.__class__.__name__))
    if method_name.endswith("invoke") and args:
        rpc_name = getattr(args[0].__class__, "__name__", None)
        if rpc_name:
            method_name = f"{method_name}:{rpc_name}"
    while True:
        try:
            return await operation(*args, **kwargs)
        except FloodWait as error:
            if _metrics_context is not None:
                _metrics_context.metrics["flood_waits_by_method"][method_name] += 1
                _metrics_context.metrics["flood_wait_seconds_by_method"][method_name] += error.value
            if _trace_item is not None:
                if _metrics_context is not None:
                    mark_wait_reason(_metrics_context, _trace_item, f"waiting_for_backoff:{_trace_phase or method_name}")
                else:
                    _trace_item.last_wait_reason = f"waiting_for_backoff:{_trace_phase or method_name}"
            if _metrics_context is not None and _trace_item is not None:
                item_state = ensure_item_analytics_state(_metrics_context, _trace_item)
                item_state["flood_wait_count"] += 1
                item_state["flood_wait_seconds"] += float(error.value)
                if (_trace_phase or "").startswith("source_"):
                    item_state["source_api_stall_seconds"] += float(error.value)
                    item_state["source_api_stall_count"] += 1
                    update_item_source_wait_totals(item_state)
                    record_analytics_event(
                        _metrics_context,
                        "source_api_stall",
                        item=_trace_item,
                        seconds=float(error.value),
                        phase=_trace_phase or "--",
                        method=method_name,
                    )
                record_analytics_event(
                    _metrics_context,
                    "flood_wait",
                    item=_trace_item,
                    seconds=float(error.value),
                    phase=_trace_phase or "--",
                    method=method_name,
                )
                await log_context(
                    _metrics_context,
                    f"[WAIT] {_trace_item.label}: FloodWait {error.value}s | fase {_trace_phase or '--'} | metodo {method_name}",
                )
            await asyncio.sleep(error.value)

async def start_client_with_retry(client_or_factory, label, context=None):
    client_factory = client_or_factory if callable(client_or_factory) else None
    client = client_factory() if client_factory else client_or_factory
    while True:
        try:
            await asyncio.wait_for(client.start(), timeout=45)
            return client
        except FloodWait as error:
            message = f"[START] {label}: FloodWait {error.value}s durante inicializacao"
            if context is not None:
                await log_context(context, message)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] {message}")
            try:
                await client.stop()
            except Exception:
                pass
            await asyncio.sleep(error.value)
            if client_factory:
                client = client_factory()
        except ConnectionError as error:
            if "already connected" in str(error).lower():
                try:
                    await client.stop()
                except Exception:
                    pass
                await asyncio.sleep(1)
                if client_factory:
                    client = client_factory()
                continue
            raise
        except asyncio.TimeoutError:
            message = f"[START] {label}: timeout durante inicializacao, recriando client"
            if context is not None:
                await log_context(context, message)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] {message}")
            try:
                await client.stop()
            except Exception:
                pass
            await asyncio.sleep(1)
            if client_factory:
                client = client_factory()

async def connect_client_with_retry(client_or_factory, label, shared_me=None, context=None):
    client_factory = client_or_factory if callable(client_or_factory) else None
    client = client_factory() if client_factory else client_or_factory
    while True:
        try:
            is_authorized = await asyncio.wait_for(client.connect(), timeout=30)
            if not is_authorized:
                raise RuntimeError(f"{label} nao autorizado ao conectar sessao auxiliar.")
            if shared_me is not None:
                client.me = shared_me
            return client
        except FloodWait as error:
            message = f"[CONNECT] {label}: FloodWait {error.value}s durante conexao"
            if context is not None:
                await log_context(context, message)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] {message}")
            try:
                if getattr(client, "is_connected", False):
                    await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(error.value)
            if client_factory:
                client = client_factory()
        except ConnectionError as error:
            error_text = str(error).lower()
            if "already connected" in error_text:
                try:
                    if getattr(client, "is_connected", False):
                        await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(1)
                if client_factory:
                    client = client_factory()
                continue
            raise
        except asyncio.TimeoutError:
            message = f"[CONNECT] {label}: timeout durante conexao, recriando client"
            if context is not None:
                await log_context(context, message)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] {message}")
            try:
                if getattr(client, "is_connected", False):
                    await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(1)
            if client_factory:
                client = client_factory()

async def close_client_quietly(client):
    if client is None:
        return
    try:
        if getattr(client, "is_initialized", False):
            await client.stop()
        elif getattr(client, "is_connected", False):
            await client.disconnect()
    except Exception:
        pass

async def refresh_message(client, message):
    refreshed = await call_telegram(client.get_messages, message.chat.id, message.id)
    return refreshed or message

def get_message_media(message):
    return (
        message.photo
        or message.audio
        or message.video
        or message.document
        or message.sticker
        or message.animation
    )

def get_streamable_thumbnail(message):
    media = get_message_media(message)
    thumbs = getattr(media, "thumbnails", None) or getattr(media, "thumbs", None) or []
    return thumbs[-1] if thumbs else None

def get_media_upload_name(message, fallback_ext="bin"):
    media = get_message_media(message)
    if not media:
        return f"message_{message.id}.{fallback_ext}"
    file_name = getattr(media, "file_name", None)
    if file_name:
        return os.path.basename(file_name)
    if message.photo:
        return f"photo_{message.id}.jpg"
    if message.video:
        return f"video_{message.id}.mp4"
    if message.audio:
        return f"audio_{message.id}.mp3"
    if message.document:
        return f"document_{message.id}.{fallback_ext}"
    if message.animation:
        return f"animation_{message.id}.mp4"
    if message.sticker:
        extension = "webp"
        sticker_name = getattr(media, "file_name", None)
        if sticker_name and "." in sticker_name:
            extension = sticker_name.rsplit(".", 1)[-1]
        return f"sticker_{message.id}.{extension}"
    return f"message_{message.id}.{fallback_ext}"

def is_stream_relay_message(message):
    media = get_message_media(message)
    if not media:
        return False
    if get_media_size(message) < STREAM_RELAY_MIN_BYTES:
        return False
    return bool(
        message.photo
        or message.audio
        or message.video
        or message.document
        or message.animation
        or message.sticker
    )

def should_use_stream_relay(item):
    if item.kind == "album":
        return all(is_stream_relay_message(message) for message in item.messages)
    return is_stream_relay_message(item.first_message)

def build_raw_input_document(uploaded_document):
    return raw.types.InputDocument(
        id=uploaded_document.id,
        access_hash=uploaded_document.access_hash,
        file_reference=uploaded_document.file_reference,
    )

def build_raw_input_photo(uploaded_photo):
    return raw.types.InputPhoto(
        id=uploaded_photo.id,
        access_hash=uploaded_photo.access_hash,
        file_reference=uploaded_photo.file_reference,
    )

async def get_media_session_pool(context, upload_client, dc_id, session_count):
    pool_key = (id(upload_client), dc_id, session_count)
    existing_pool = context.media_session_pools.get(pool_key)
    if existing_pool is not None:
        context.metrics["relay_session_reuses"] += 1
        return existing_pool

    auth_key = await upload_client.storage.auth_key()
    test_mode = await upload_client.storage.test_mode()
    sessions = [
        Session(upload_client, dc_id, auth_key, test_mode, is_media=True)
        for _ in range(session_count)
    ]
    for session in sessions:
        await session.start()
        context.metrics["relay_session_starts"] += 1

    pool = MediaSessionPool(dc_id=dc_id, sessions=sessions)
    context.media_session_pools[pool_key] = pool
    return pool

async def close_media_session_pools(context):
    for pool in context.media_session_pools.values():
        for session in pool.sessions:
            try:
                await session.stop()
            except Exception:
                pass
    context.media_session_pools.clear()

async def invalidate_media_session_pool(context, upload_client, dc_id, session_count):
    pool_key = (id(upload_client), dc_id, session_count)
    pool = context.media_session_pools.pop(pool_key, None)
    if pool is None:
        return
    for session in pool.sessions:
        try:
            await session.stop()
        except Exception:
            pass
    context.metrics["relay_pool_resets"] += 1

def is_transport_error(error):
    error_text = str(error).upper()
    return (
        "WINERROR 1231" in error_text
        or "UNABLE TO CONNECT" in error_text
        or "NETWORK ISSUES" in error_text
        or "TIMED OUT" in error_text
        or "CONNECTION RESET" in error_text
    )

def format_metrics_summary(context):
    relay_files = context.metrics["relay_files"]
    relay_bytes = context.metrics["relay_bytes"]
    relay_seconds = context.metrics["relay_seconds"]
    mib_sent = relay_bytes / (1024 * 1024) if relay_bytes else 0.0
    mib_per_second = (mib_sent / relay_seconds) if relay_seconds > 0 else 0.0
    avg_seconds_per_file = (relay_seconds / relay_files) if relay_files else 0.0

    flood_parts = []
    flood_waits = context.metrics["flood_waits_by_method"]
    flood_seconds = context.metrics["flood_wait_seconds_by_method"]
    for method_name in sorted(flood_waits.keys()):
        flood_parts.append(f"{method_name}:{flood_waits[method_name]}x/{flood_seconds[method_name]:.0f}s")
    flood_summary = ", ".join(flood_parts) if flood_parts else "nenhum"
    download_rate = "--"
    if context.metrics["download_phase_seconds"] > 0:
        download_rate = f"{(context.metrics['download_phase_bytes'] / (1024 * 1024)) / context.metrics['download_phase_seconds']:.2f} MiB/s"
    preupload_rate = "--"
    if context.metrics["preupload_phase_seconds"] > 0:
        preupload_rate = f"{(context.metrics['preupload_phase_bytes'] / (1024 * 1024)) / context.metrics['preupload_phase_seconds']:.2f} MiB/s"
    publish_avg = "--"
    if context.metrics["publish_items"] > 0:
        publish_avg = f"{context.metrics['publish_phase_seconds'] / context.metrics['publish_items']:.2f}s/item"

    return (
        f"[METRICS] relay arquivos: {relay_files} | relay volume: {mib_sent:.1f} MiB | "
        f"relay taxa media: {mib_per_second:.2f} MiB/s | relay tempo medio/arquivo: {avg_seconds_per_file:.1f}s | "
        f"source custom: {context.metrics['source_custom_files']} | source paralelo: {context.metrics['source_parallel_files']} | "
        f"source fallback: {context.metrics['source_fallbacks']} | source cdn: {context.metrics['source_cdn_redirects']} | "
        f"download taxa media: {download_rate} | preupload taxa media: {preupload_rate} | publish medio: {publish_avg} | "
        f"sessoes abertas: {context.metrics['relay_session_starts']} | reusos de pool: {context.metrics['relay_session_reuses']} | "
        f"resets de pool: {context.metrics['relay_pool_resets']} | source sessoes: {context.metrics['source_session_starts']} | "
        f"cdn sessoes: {context.metrics['source_cdn_session_starts']} | "
        f"erros relay: {context.metrics['relay_errors']} | auth duplicated: {context.metrics['auth_key_duplicated']} | "
        f"resumes: {context.metrics['relay_resumes']} | parts skipped: {context.metrics['relay_parts_skipped_on_resume']} | "
        f"flood waits: {flood_summary}"
    )

def build_hol_windows(status_snapshots):
    windows = []
    current = None

    def flush_window(window):
        if not window:
            return
        window["duration_seconds"] = max(0.0, window["end_ts"] - window["start_ts"])
        classification = None
        if window["max_prontos_atras"] >= HOL_MIN_READY_BEHIND:
            if window["count"] >= HOL_SEVERE_MIN_STREAK:
                classification = "hol_severe"
            elif window["count"] >= HOL_MODERATE_MIN_STREAK:
                classification = "hol_moderate"
        window["classification"] = classification
        window["moderate"] = classification == "hol_moderate"
        window["severe"] = classification == "hol_severe"
        windows.append(window)

    for snapshot in status_snapshots:
        head_seq = snapshot.get("head_seq")
        if head_seq is None:
            flush_window(current)
            current = None
            continue

        if current and current["head_seq"] == head_seq:
            current["end_ts"] = snapshot["ts"]
            current["count"] += 1
            current["max_prontos_atras"] = max(current["max_prontos_atras"], snapshot.get("prontos_atras", 0))
            current["max_pre_active"] = max(current["max_pre_active"], snapshot.get("pre_active", 0))
            current["max_large_active"] = max(current["max_large_active"], snapshot.get("large_active", 0))
            current["max_large_limit"] = max(current["max_large_limit"], snapshot.get("large_max", STREAM_RELAY_MAX_LARGE_ACTIVE))
            current["head_reason"] = snapshot.get("head_reason")
        else:
            flush_window(current)
            current = {
                "head_seq": head_seq,
                "head_label": snapshot.get("head_label"),
                "head_reason": snapshot.get("head_reason"),
                "start_ts": snapshot["ts"],
                "end_ts": snapshot["ts"],
                "count": 1,
                "max_prontos_atras": snapshot.get("prontos_atras", 0),
                "max_pre_active": snapshot.get("pre_active", 0),
                "max_large_active": snapshot.get("large_active", 0),
                "max_large_limit": snapshot.get("large_max", STREAM_RELAY_MAX_LARGE_ACTIVE),
            }

    flush_window(current)
    return windows

def compute_saturation_seconds(status_snapshots):
    result = {
        "pre_active_max_seconds": 0.0,
        "large_limit_seconds": 0.0,
        "ready_stalled_seconds": 0.0,
    }
    if not status_snapshots:
        return result

    for index, snapshot in enumerate(status_snapshots):
        if index + 1 < len(status_snapshots):
            next_ts = status_snapshots[index + 1]["ts"]
        else:
            next_ts = snapshot["ts"] + STATUS_LOG_INTERVAL_SECONDS
        dt = max(0.0, next_ts - snapshot["ts"])

        if snapshot.get("pre_active", 0) >= PREUPLOAD_WORKERS:
            result["pre_active_max_seconds"] += dt

        if (
            snapshot.get("large_active", 0) >= snapshot.get("large_max", STREAM_RELAY_MAX_LARGE_ACTIVE)
            or snapshot.get("huge_active", 0) >= STREAM_RELAY_MAX_HUGE_ACTIVE
        ):
            result["large_limit_seconds"] += dt

        if snapshot.get("ready", 0) > 0 and snapshot.get("head_seq") is not None:
            head_state = (snapshot.get("head_state") or "").strip().lower()
            if head_state not in {"ready", "done", "failed"}:
                result["ready_stalled_seconds"] += dt

    return result

def build_unproductive_contention_windows(status_snapshots):
    windows = []
    current = None

    def estimate_dt(index, snapshot):
        if index + 1 < len(status_snapshots):
            return max(0.0, status_snapshots[index + 1]["ts"] - snapshot["ts"])
        return float(STATUS_LOG_INTERVAL_SECONDS)

    def flush_window(window):
        if not window:
            return
        if window["duration_seconds"] >= UNPRODUCTIVE_CONTENTION_MIN_SECONDS:
            windows.append(window)

    for index, snapshot in enumerate(status_snapshots):
        head_seq = snapshot.get("head_seq")
        head_state = (snapshot.get("head_state") or "").strip().lower()
        is_unproductive = (
            head_seq is not None
            and snapshot.get("pre_active", 0) >= PREUPLOAD_WORKERS
            and snapshot.get("stream_active", 0) >= max(1, STREAM_RELAY_MAX_ACTIVE - 1)
            and snapshot.get("ready", 0) > 0
            and head_state not in {"ready", "done", "failed"}
        )

        dt = estimate_dt(index, snapshot)
        if is_unproductive:
            if current and current["head_seq"] == head_seq:
                current["duration_seconds"] += dt
                current["end_ts"] = snapshot["ts"] + dt
                current["max_ready"] = max(current["max_ready"], snapshot.get("ready", 0))
                current["max_prontos_atras"] = max(current["max_prontos_atras"], snapshot.get("prontos_atras", 0))
                current["max_pre_active"] = max(current["max_pre_active"], snapshot.get("pre_active", 0))
                current["max_stream_active"] = max(current["max_stream_active"], snapshot.get("stream_active", 0))
            else:
                flush_window(current)
                current = {
                    "head_seq": head_seq,
                    "head_label": snapshot.get("head_label"),
                    "start_ts": snapshot["ts"],
                    "end_ts": snapshot["ts"] + dt,
                    "duration_seconds": dt,
                    "max_ready": snapshot.get("ready", 0),
                    "max_prontos_atras": snapshot.get("prontos_atras", 0),
                    "max_pre_active": snapshot.get("pre_active", 0),
                    "max_stream_active": snapshot.get("stream_active", 0),
                }
        else:
            flush_window(current)
            current = None

    flush_window(current)
    return windows

def interval_hits_relay_policy_limit(status_snapshots, started_at, finished_at):
    if not started_at or not finished_at or finished_at <= started_at:
        return False
    for snapshot in status_snapshots:
        ts = snapshot.get("ts")
        if ts is None or ts < started_at or ts > finished_at:
            continue
        if (
            snapshot.get("large_active", 0) >= snapshot.get("large_max", STREAM_RELAY_MAX_LARGE_ACTIVE)
            or snapshot.get("huge_active", 0) >= STREAM_RELAY_MAX_HUGE_ACTIVE
        ):
            return True
    return False

def classify_bound_severity(event_count):
    if event_count <= 0:
        return None
    if event_count == 1:
        return "leve"
    if event_count <= 3:
        return "moderada"
    return "severa"

def count_upload_getfile_waits(context):
    # Prefer explicit Pyrogram wait lines from detailed log when available.
    if context.detailed_log_stream is not None:
        try:
            context.detailed_log_stream.flush()
        except Exception:
            pass

    if context.detailed_log_path and os.path.isfile(context.detailed_log_path):
        wait_count = 0
        total_wait_seconds = 0.0
        try:
            with open(context.detailed_log_path, "r", encoding="utf-8") as log_file:
                for line in log_file:
                    match = UPLOAD_GETFILE_WAIT_REGEX.search(line)
                    if not match:
                        continue
                    wait_count += 1
                    total_wait_seconds += float(int(match.group(1)))
            return {
                "count": wait_count,
                "seconds": total_wait_seconds,
            }
        except Exception:
            pass

    wait_count = 0
    total_wait_seconds = 0.0
    for method_name, count in context.metrics["flood_waits_by_method"].items():
        if "GetFile" not in method_name:
            continue
        wait_count += int(count)
        total_wait_seconds += float(context.metrics["flood_wait_seconds_by_method"].get(method_name, 0.0))
    return {
        "count": wait_count,
        "seconds": total_wait_seconds,
    }

def interval_has_large_active(status_snapshots, started_at, finished_at):
    if not started_at or not finished_at or finished_at <= started_at:
        return False
    for snapshot in status_snapshots:
        ts = snapshot.get("ts")
        if ts is None or ts < started_at or ts > finished_at:
            continue
        if snapshot.get("large_active", 0) > 0 or snapshot.get("huge_active", 0) > 0:
            return True
    return False

def cause_label(cause_key):
    labels = {
        CAUSE_QUEUE_BOUND: "Queue Bound",
        CAUSE_SOURCE_BOUND: "Source Bound",
        CAUSE_RELAY_SLOT_BOUND: "Relay Slot Bound",
        CAUSE_RELAY_POLICY_BOUND: "Relay Policy Bound",
        CAUSE_TELEGRAM_LIMITED: "Telegram Limited",
        CAUSE_HOL_BOUND: "Head-of-Line Bound",
        CAUSE_SCHEDULER_UNFAIR: "Scheduler Unfair",
        CAUSE_MIXED: "Mixed",
    }
    return labels.get(cause_key, cause_key)

def classify_dominant_cause(row, starvation_lanes):
    scores = {}
    source_budget_pressure = row["source_budget_wait_seconds"] + row["source_budget_wait_count"] * 5.0
    source_api_pressure = (
        row["source_api_stall_seconds"]
        + row["source_local_retry_backoff_seconds"]
        + row["source_api_stall_count"] * 5.0
        + row["source_local_retry_count"] * 5.0
        + (35.0 if row["fallback_legacy"] else 0.0)
    )

    if (
        row["pre_queue_wait_seconds"] >= HOT_ITEM_PREQ_SECONDS
        and (source_budget_pressure + source_api_pressure) < 10.0
        and row["stream_slot_wait_seconds"] < 10.0
        and row["retry_count"] == 0
    ):
        scores[CAUSE_QUEUE_BOUND] = row["pre_queue_wait_seconds"]

    if (
        source_budget_pressure >= 20.0
        or source_api_pressure >= 20.0
        or row["fallback_legacy"]
        or row["source_local_retry_count"] >= 1
        or row["source_api_stall_count"] >= 1
        or row["source_budget_wait_count"] >= 2
    ):
        scores[CAUSE_SOURCE_BOUND] = source_budget_pressure + source_api_pressure

    if row["stream_slot_wait_seconds"] >= 15.0:
        scores[CAUSE_RELAY_SLOT_BOUND] = row["stream_slot_wait_seconds"] + row["stream_slot_wait_count"] * 5.0

    if row["retry_count"] > 0 or row["flood_wait_count"] > 0:
        scores[CAUSE_TELEGRAM_LIMITED] = row["retry_delay_seconds"] + row["retry_count"] * 10.0

    if row["hol_wait_seconds"] >= 20.0:
        scores[CAUSE_HOL_BOUND] = row["hol_wait_seconds"] + row["hol_window_hits"] * 5.0

    if row["lane"] in starvation_lanes and row["pre_queue_wait_seconds"] >= 20.0:
        scores[CAUSE_SCHEDULER_UNFAIR] = row["pre_queue_wait_seconds"] + 15.0

    if not scores:
        fallback_scores = {
            CAUSE_QUEUE_BOUND: row["pre_queue_wait_seconds"],
            CAUSE_SOURCE_BOUND: row["source_wait_seconds"],
            CAUSE_RELAY_SLOT_BOUND: row["stream_slot_wait_seconds"],
            CAUSE_TELEGRAM_LIMITED: row["retry_delay_seconds"],
            CAUSE_HOL_BOUND: row["hol_wait_seconds"],
        }
        fallback_cause = max(fallback_scores.items(), key=lambda entry: entry[1])[0]
        if fallback_scores[fallback_cause] > 0:
            scores[fallback_cause] = fallback_scores[fallback_cause]
        else:
            scores[CAUSE_QUEUE_BOUND] = row["pre_queue_wait_seconds"]

    ordered = sorted(scores.items(), key=lambda entry: entry[1], reverse=True)
    if len(ordered) >= 2:
        top_value = ordered[0][1]
        second_value = ordered[1][1]
        if top_value >= 20.0 and second_value >= 20.0 and safe_div(second_value, top_value) >= 0.85:
            return CAUSE_MIXED, scores
    return ordered[0][0], scores

def derive_lane_stats(item_rows):
    lane_stats = {}
    for row in item_rows:
        lane = row["lane"]
        lane_stats.setdefault(lane, []).append(row["pre_queue_wait_seconds"])

    lane_summary = {}
    for lane, waits in lane_stats.items():
        lane_summary[lane] = {
            "count": len(waits),
            "avg_wait_seconds": average(waits),
            "p95_wait_seconds": percentile(waits, 95),
        }
    return lane_summary

def find_starvation_lanes(lane_summary, global_waits):
    starvation = set()
    global_avg = average(global_waits)
    global_p95 = percentile(global_waits, 95)
    for lane, summary in lane_summary.items():
        if global_avg > 0 and summary["avg_wait_seconds"] > (global_avg * LANE_STARVATION_FACTOR):
            starvation.add(lane)
            continue
        if global_p95 > 0 and summary["p95_wait_seconds"] > (global_p95 * LANE_P95_DISCREPANCY_FACTOR):
            starvation.add(lane)
    return starvation

def infer_window_causes(window):
    reason = (window.get("head_reason") or "").lower()
    causes = []
    if "stream_relay" in reason or "slot" in reason:
        causes.append(CAUSE_RELAY_SLOT_BOUND)
    if "backoff" in reason or "flood" in reason:
        causes.append(CAUSE_TELEGRAM_LIMITED)
    if "source" in reason or "orcamento" in reason:
        causes.append(CAUSE_SOURCE_BOUND)
    if not causes:
        causes.append(CAUSE_HOL_BOUND)
    return causes

def build_run_analytics_report(context, previous_report=None):
    ended_at = time.time()
    total_time_seconds = max(0.0, ended_at - context.started_at)
    fingerprint = build_run_fingerprint(context)
    run_label = get_run_label()

    hol_windows = build_hol_windows(context.analytics_status_snapshots)
    saturation = compute_saturation_seconds(context.analytics_status_snapshots)
    unproductive_windows = build_unproductive_contention_windows(context.analytics_status_snapshots)
    upload_getfile_wait_stats = count_upload_getfile_waits(context)
    items_by_seq = {item.seq: item for item in context.work_items}

    item_rows = []
    for item in context.work_items:
        state = ensure_item_analytics_state(context, item)
        row = {
            "seq": item.seq,
            "label": item.label,
            "display_message_id": item.display_message_id,
            "estimated_bytes": int(item.estimated_bytes or 0),
            "size_bucket": classify_size_bucket(item.estimated_bytes),
            "lane": state.get("lane") or "desconhecida",
            "pre_worker": state.get("pre_worker") or "N/A",
            "download_queue_wait_seconds": seconds_between(item.download_queued_at, item.download_started_at),
            "download_seconds": seconds_between(item.download_started_at, item.download_finished_at),
            "pre_queue_wait_seconds": seconds_between(item.preupload_queued_at, item.preupload_started_at),
            "preupload_seconds": seconds_between(item.preupload_started_at, item.preupload_finished_at),
            "preupload_wall_e2e_seconds": seconds_between(item.first_preupload_started_at, item.preupload_finished_at),
            "publish_seconds": seconds_between(item.publish_started_at, item.publish_finished_at),
            "source_wait_seconds": float(state.get("source_wait_seconds", 0.0)),
            "source_wait_count": int(state.get("source_wait_count", 0)),
            "source_budget_wait_seconds": float(state.get("source_budget_wait_seconds", 0.0)),
            "source_budget_wait_count": int(state.get("source_budget_wait_count", 0)),
            "source_api_stall_seconds": float(state.get("source_api_stall_seconds", 0.0)),
            "source_api_stall_count": int(state.get("source_api_stall_count", 0)),
            "source_local_retry_seconds": float(state.get("source_local_retry_seconds", 0.0)),
            "source_local_retry_count": int(state.get("source_local_retry_count", 0)),
            "source_local_retry_backoff_seconds": float(state.get("source_local_retry_backoff_seconds", 0.0)),
            "source_mode_initial": state.get("source_mode_initial") or item.source_mode_initial or "",
            "source_mode_final": state.get("source_mode_final") or item.source_mode_final or "",
            "source_fallback_count": int(state.get("source_fallback_count", item.source_fallback_count)),
            "source_fallback_resume_chunk": state.get("source_fallback_resume_chunk", item.source_fallback_resume_chunk),
            "source_dc": state.get("source_dc", item.source_dc_id),
            "retry_phase_counts": dict(state.get("retry_phase_counts", {})),
            "stream_slot_wait_seconds": float(state.get("stream_slot_wait_seconds", 0.0)),
            "stream_slot_wait_count": int(state.get("stream_slot_wait_count", 0)),
            "flood_wait_seconds": float(state.get("flood_wait_seconds", 0.0)),
            "flood_wait_count": int(state.get("flood_wait_count", 0)),
            "transient_retry_count": int(state.get("transient_retry_count", 0)),
            "transient_backoff_seconds": float(state.get("transient_backoff_seconds", 0.0)),
            "retry_count": int(state.get("transient_retry_count", 0)) + int(state.get("flood_wait_count", 0)),
            "retry_delay_seconds": float(state.get("transient_backoff_seconds", 0.0)) + float(state.get("flood_wait_seconds", 0.0)),
            "fallback_legacy": bool(state.get("fallback_legacy", False)),
            "wait_reason_counts": dict(state.get("wait_reason_counts", {})),
            "relay_source_read_seconds": float(item.relay_main_source_read_seconds + item.relay_thumb_source_read_seconds),
            "relay_save_parts_seconds": float(item.relay_main_save_parts_seconds + item.relay_thumb_save_parts_seconds),
            "source_budget_bound": False,
            "source_budget_severity": None,
            "source_budget_event_count": 0,
            "source_api_bound": False,
            "source_api_severity": None,
            "source_api_event_count": 0,
            "relay_policy_bound": False,
            "relay_policy_severity": None,
            "relay_policy_event_count": 0,
            "success": item.state in {"done", "ready"},
            "error": item.error or "",
            "ready_at": item.preupload_finished_at or item.download_finished_at or item.preupload_started_at or item.download_started_at,
            "publish_started_at": item.publish_started_at or item.send_started_at or item.acked_at,
            "hol_wait_seconds": 0.0,
            "hol_window_hits": 0,
            "dominant_cause": None,
            "dominant_cause_scores": {},
        }
        item_rows.append(row)

    for row in item_rows:
        ready_at = row.get("ready_at")
        publish_started_at = row.get("publish_started_at")
        if not ready_at or not publish_started_at:
            continue
        for window in hol_windows:
            head_seq = window.get("head_seq")
            if head_seq is None or row["seq"] <= head_seq:
                continue
            overlap_start = max(ready_at, window["start_ts"])
            overlap_end = min(publish_started_at, window["end_ts"])
            if overlap_end <= overlap_start:
                continue
            row["hol_wait_seconds"] += (overlap_end - overlap_start)
            row["hol_window_hits"] += 1

    for row in item_rows:
        source_budget_event_count = int(row["source_budget_wait_count"])
        row["source_budget_event_count"] = source_budget_event_count
        row["source_budget_bound"] = source_budget_event_count > 0
        row["source_budget_severity"] = classify_bound_severity(source_budget_event_count)
        source_api_event_count = (
            int(row["source_api_stall_count"])
            + int(row["source_local_retry_count"])
            + int(row["source_fallback_count"])
        )
        row["source_api_event_count"] = source_api_event_count
        row["source_api_bound"] = source_api_event_count > 0
        row["source_api_severity"] = classify_bound_severity(source_api_event_count)

        relay_event_count = int(row["stream_slot_wait_count"])
        item_ref = items_by_seq.get(row["seq"])
        queue_started_at = (
            getattr(item_ref, "preupload_queued_at", None)
            or row.get("ready_at")
            or context.started_at
        )
        queue_released_at = (
            getattr(item_ref, "preupload_started_at", None)
            or row.get("publish_started_at")
            or ended_at
        )
        if relay_event_count <= 0 and interval_hits_relay_policy_limit(
            context.analytics_status_snapshots,
            queue_started_at,
            queue_released_at,
        ):
            relay_event_count = 1
        row["relay_policy_event_count"] = relay_event_count
        row["relay_policy_bound"] = relay_event_count > 0
        row["relay_policy_severity"] = classify_bound_severity(relay_event_count)

    pre_queue_waits = [row["pre_queue_wait_seconds"] for row in item_rows if row["pre_queue_wait_seconds"] > 0]
    preupload_times = [row["preupload_seconds"] for row in item_rows if row["preupload_seconds"] > 0]
    download_times = [row["download_seconds"] for row in item_rows if row["download_seconds"] > 0]

    lane_summary = derive_lane_stats(item_rows)
    starvation_lanes = find_starvation_lanes(lane_summary, pre_queue_waits)

    cause_counter = defaultdict(int)
    for row in item_rows:
        cause, scores = classify_dominant_cause(row, starvation_lanes)
        row["dominant_cause"] = cause
        row["dominant_cause_scores"] = scores
        cause_counter[cause] += 1
        state = context.analytics_item_state.get(row["seq"])
        if state is not None:
            state["dominant_cause"] = cause
            state["dominant_cause_scores"] = scores
            state["hol_wait_seconds"] = row["hol_wait_seconds"]
            state["source_budget_bound"] = row["source_budget_bound"]
            state["source_budget_severity"] = row["source_budget_severity"]
            state["source_budget_event_count"] = row["source_budget_event_count"]
            state["source_api_bound"] = row["source_api_bound"]
            state["source_api_severity"] = row["source_api_severity"]
            state["source_api_event_count"] = row["source_api_event_count"]
            state["relay_policy_bound"] = row["relay_policy_bound"]
            state["relay_policy_severity"] = row["relay_policy_severity"]
            state["relay_policy_event_count"] = row["relay_policy_event_count"]

    size_bucket_summary = {}
    for bucket in ["lt80mb", "80_300mb", "300_800mb", "gt800mb"]:
        bucket_rows = [row for row in item_rows if row["size_bucket"] == bucket]
        preq_values = [row["pre_queue_wait_seconds"] for row in bucket_rows if row["pre_queue_wait_seconds"] > 0]
        preup_values = [row["preupload_seconds"] for row in bucket_rows if row["preupload_seconds"] > 0]
        fallback_count = sum(1 for row in bucket_rows if row["fallback_legacy"])
        size_bucket_summary[bucket] = {
            "count": len(bucket_rows),
            "pre_queue_wait_avg_seconds": average(preq_values),
            "pre_queue_wait_p95_seconds": percentile(preq_values, 95),
            "preupload_avg_seconds": average(preup_values),
            "preupload_p95_seconds": percentile(preup_values, 95),
            "fallback_rate": safe_div(fallback_count, len(bucket_rows)),
        }

    worker_totals = defaultdict(float)
    for row in item_rows:
        if row["preupload_seconds"] > 0 and row["pre_worker"] != "N/A":
            worker_totals[row["pre_worker"]] += row["preupload_seconds"]
    total_worker_seconds = sum(worker_totals.values())
    worker_shares = {
        worker: safe_div(seconds, total_worker_seconds)
        for worker, seconds in worker_totals.items()
    }

    sorted_rows = sorted(item_rows, key=lambda row: row["seq"])
    quarter_count = max(1, len(sorted_rows) // 4)
    first_quarter = sorted_rows[:quarter_count]
    last_quarter = sorted_rows[-quarter_count:]
    first_quarter_wait = average(row["pre_queue_wait_seconds"] for row in first_quarter)
    last_quarter_wait = average(row["pre_queue_wait_seconds"] for row in last_quarter)

    top_causes = [cause for cause, _ in sorted(cause_counter.items(), key=lambda entry: entry[1], reverse=True)[:3]]
    top_causes_human = [cause_label(cause) for cause in top_causes]

    total_retries = sum(row["retry_count"] for row in item_rows)
    total_fallbacks = sum(1 for row in item_rows if row["fallback_legacy"])
    total_hol_events = sum(1 for window in hol_windows if window.get("classification") in {"hol_moderate", "hol_severe"})
    total_hol_moderate_events = sum(1 for window in hol_windows if window.get("classification") == "hol_moderate")
    total_hol_severe_events = sum(1 for window in hol_windows if window.get("classification") == "hol_severe")
    sum_pre_queue_wait_seconds = sum(row["pre_queue_wait_seconds"] for row in item_rows)
    sum_preupload_seconds = sum(row["preupload_seconds"] for row in item_rows)
    sum_preupload_wall_e2e_seconds = sum(row["preupload_wall_e2e_seconds"] for row in item_rows)
    stream_slot_wait_seconds_total = sum(row["stream_slot_wait_seconds"] for row in item_rows)
    source_budget_wait_seconds_total = sum(row["source_budget_wait_seconds"] for row in item_rows)
    source_api_stall_seconds_total = sum(row["source_api_stall_seconds"] for row in item_rows)
    source_local_retry_seconds_total = sum(row["source_local_retry_backoff_seconds"] for row in item_rows)
    source_budget_events_total = sum(row["source_budget_event_count"] for row in item_rows)
    source_api_events_total = sum(row["source_api_event_count"] for row in item_rows)
    relay_policy_events_total = sum(row["relay_policy_event_count"] for row in item_rows)
    source_budget_bound_items = sum(1 for row in item_rows if row["source_budget_bound"])
    source_api_bound_items = sum(1 for row in item_rows if row["source_api_event_count"] > 0)
    relay_policy_bound_items = sum(1 for row in item_rows if row["relay_policy_bound"])
    unproductive_contention_seconds = sum(window["duration_seconds"] for window in unproductive_windows)
    items_per_min = safe_div(context.success_count, total_time_seconds / 60.0) if total_time_seconds > 0 else 0.0
    custom_source_rows = [row for row in item_rows if row["source_mode_initial"] in {"custom", "parallel"}]
    custom_to_legacy_rows = [row for row in custom_source_rows if row["source_mode_final"] == "legacy"]

    alerts = []
    alert_codes = set()

    def add_alert(code, message, evidence):
        if code in alert_codes:
            return
        alert_codes.add(code)
        alerts.append({"code": code, "message": message, "evidence": evidence})

    hot_items = [
        row for row in item_rows
        if (
            row["preupload_seconds"] > HOT_ITEM_PREUP_SECONDS
            or row["preupload_wall_e2e_seconds"] > HOT_ITEM_PREUP_SECONDS
            or row["pre_queue_wait_seconds"] > HOT_ITEM_PREQ_SECONDS
            or row["retry_count"] > 0
            or row["fallback_legacy"]
        )
    ]
    if hot_items:
        add_alert(
            "item_hot",
            f"[Alerta] Item quente detectado em {len(hot_items)} item(ns).",
            {"count": len(hot_items)},
        )

    high_retry_items = [
        row for row in item_rows
        if row["retry_count"] > 0 and (
            row["retry_delay_seconds"] > HIGH_RETRY_DELAY_SECONDS
            or row["retry_delay_seconds"] > (row["preupload_seconds"] * 0.10 if row["preupload_seconds"] > 0 else 0)
        )
    ]
    if high_retry_items:
        add_alert(
            "retry_high_cost",
            f"[Alerta] Custo de retry alto detectado em {len(high_retry_items)} item(ns).",
            {"count": len(high_retry_items)},
        )

    small_anomalies = [
        row for row in item_rows
        if row["estimated_bytes"] < HOT_SMALL_ANOMALY_BYTES and (
            row["preupload_seconds"] > HOT_SMALL_PRE_SECONDS
            or row["pre_queue_wait_seconds"] > HOT_SMALL_PRE_SECONDS
        )
    ]
    if small_anomalies:
        add_alert(
            "small_file_anomaly",
            f"[Alerta] Anomalia de arquivo pequeno detectada em {len(small_anomalies)} item(ns).",
            {"count": len(small_anomalies)},
        )

    moderate_hol = [window for window in hol_windows if window.get("classification") == "hol_moderate"]
    if moderate_hol:
        add_alert(
            "hol_moderate",
            "[Alerta] HOL moderado detectado.",
            {"windows": len(moderate_hol)},
        )

    severe_hol = [window for window in hol_windows if window.get("classification") == "hol_severe"]
    if severe_hol:
        add_alert(
            "hol_severe",
            "[Alerta] HOL severo detectado (Lane possivelmente penalizada).",
            {"windows": len(severe_hol)},
        )

    if unproductive_windows:
        add_alert(
            "contention_unproductive",
            "[Alerta] Contencao improdutiva detectada (sistema cheio com head parado).",
            {
                "windows": len(unproductive_windows),
                "seconds": unproductive_contention_seconds,
            },
        )

    if relay_policy_bound_items > 0:
        add_alert(
            "relay_policy_bound",
            f"[Alerta] Relay policy bound detectado em {relay_policy_bound_items} item(ns).",
            {
                "items": relay_policy_bound_items,
                "events": relay_policy_events_total,
            },
        )

    if source_budget_bound_items > 0:
        add_alert(
            "source_budget_bound",
            f"[Alerta] Source budget bound detectado em {source_budget_bound_items} item(ns).",
            {
                "items": source_budget_bound_items,
                "events": source_budget_events_total,
                "seconds": source_budget_wait_seconds_total,
            },
        )

    if source_api_bound_items > 0:
        add_alert(
            "source_api_bound",
            f"[Alerta] Source API bound detectado em {source_api_bound_items} item(ns).",
            {
                "items": source_api_bound_items,
                "events": source_api_events_total,
                "seconds": source_api_stall_seconds_total + source_local_retry_seconds_total,
            },
        )

    source_wait_repeats = sum(1 for event in context.analytics_events if event.get("event") == "source_budget_wait_done")
    avg_preupload_mib_s = safe_div(
        context.metrics["preupload_phase_bytes"] / (1024 * 1024),
        context.metrics["preupload_phase_seconds"],
    )
    oversub_detected = (
        any(
            snapshot.get("pre_active", 0) >= PREUPLOAD_WORKERS and snapshot.get("ready", 0) > 0
            for snapshot in context.analytics_status_snapshots
        )
        and source_wait_repeats >= 3
        and avg_preupload_mib_s < OVERSUB_THROUGHPUT_FLOOR_MIB_S
    )
    if oversub_detected:
        add_alert(
            "oversubscription",
            "[Alerta] Oversubscription provavel detectada.",
            {
                "source_wait_repeats": source_wait_repeats,
                "avg_preupload_mib_s": avg_preupload_mib_s,
            },
        )

    contaminated_small = []
    for row in item_rows:
        if row["estimated_bytes"] >= HOT_SMALL_ANOMALY_BYTES:
            continue
        if row["pre_queue_wait_seconds"] <= SMALL_CONTAMINATION_PREQ_SECONDS:
            continue
        if interval_has_large_active(
            context.analytics_status_snapshots,
            row.get("ready_at") or row.get("publish_started_at") or context.started_at,
            row.get("publish_started_at") or ended_at,
        ):
            contaminated_small.append(row)
    if contaminated_small:
        add_alert(
            "large_contamination",
            "[Alerta] Contaminacao por grandes punindo lote <80MB.",
            {"count": len(contaminated_small)},
        )

    if starvation_lanes:
        add_alert(
            "lane_starvation",
            f"[Alerta] Starvation de lane detectada: {', '.join(sorted(starvation_lanes))}.",
            {"lanes": sorted(starvation_lanes)},
        )

    if worker_shares:
        worst_worker, worst_share = max(worker_shares.items(), key=lambda entry: entry[1])
        if worst_share > WORKER_IMBALANCE_SHARE:
            add_alert(
                "worker_imbalance",
                f"[Alerta] Worker imbalance detectado ({worst_worker} com {worst_share * 100:.1f}% do tempo pre-up).",
                {"worker": worst_worker, "share": worst_share},
            )

    tail_ratio = safe_div(last_quarter_wait, first_quarter_wait) if first_quarter_wait > 0 else 0.0
    if first_quarter_wait > 0 and tail_ratio > TAIL_DEGRADATION_FACTOR:
        add_alert(
            "tail_degradation",
            "[Alerta] Degradacao final (cauda longa) detectada.",
            {
                "first_quarter_wait_seconds": first_quarter_wait,
                "last_quarter_wait_seconds": last_quarter_wait,
                "ratio": tail_ratio,
            },
        )

    saturation_metrics = dict(saturation)
    saturation_metrics["unproductive_contention_seconds"] = unproductive_contention_seconds

    metrics = {
        "macro": {
            "total_time_seconds": total_time_seconds,
            "items_total": context.total_items,
            "items_per_min": items_per_min,
            "sum_pre_queue_wait_seconds": sum_pre_queue_wait_seconds,
            "sum_preupload_seconds": sum_preupload_seconds,
            "sum_preupload_wall_e2e_seconds": sum_preupload_wall_e2e_seconds,
            "total_retries": total_retries,
            "total_fallbacks": total_fallbacks,
            "total_hol_events": total_hol_events,
            "total_hol_moderate_events": total_hol_moderate_events,
            "total_hol_severe_events": total_hol_severe_events,
            "upload_getfile_wait_count": upload_getfile_wait_stats["count"],
            "upload_getfile_wait_seconds": upload_getfile_wait_stats["seconds"],
            "flood_pressure_events_total": int(getattr(context.flood_pressure, "total_events", 0)) if hasattr(context, "flood_pressure") else 0,
            "flood_pressure_wait_seconds_total": float(getattr(context.flood_pressure, "total_wait_seconds", 0.0)) if hasattr(context, "flood_pressure") else 0.0,
            "flood_pressure_peak_seconds": float(getattr(context.flood_pressure, "peak_pressure_seconds", 0.0)) if hasattr(context, "flood_pressure") else 0.0,
            "flood_tier_transitions_total": int(getattr(context.flood_pressure, "transitions_total", 0)) if hasattr(context, "flood_pressure") else 0,
            "tg_queue_cap_large": int(getattr(context, "tg_queue_cap_large", TG_QUEUE_CAP_LARGE_DEFAULT)),
            "tg_queue_cap_small": int(getattr(context, "tg_queue_cap_small", TG_QUEUE_CAP_SMALL_DEFAULT)),
            "tg_queue_cap_source": str(getattr(context, "tg_queue_cap_source", "default")),
            "stream_slot_wait_seconds_total": stream_slot_wait_seconds_total,
            "source_budget_wait_seconds_total": source_budget_wait_seconds_total,
            "source_api_stall_seconds_total": source_api_stall_seconds_total,
            "source_local_retry_seconds_total": source_local_retry_seconds_total,
            "source_budget_events_total": source_budget_events_total,
            "source_budget_bound_items": source_budget_bound_items,
            "source_api_events_total": source_api_events_total,
            "source_api_bound_items": source_api_bound_items,
            "relay_policy_events_total": relay_policy_events_total,
            "relay_policy_bound_items": relay_policy_bound_items,
            "unproductive_contention_events": len(unproductive_windows),
            "unproductive_contention_seconds": unproductive_contention_seconds,
        },
        "percentiles": {
            "pre_queue_wait_seconds": {
                "p50": percentile(pre_queue_waits, 50),
                "p95": percentile(pre_queue_waits, 95),
                "p99": percentile(pre_queue_waits, 99),
            },
            "preupload_seconds": {
                "p50": percentile(preupload_times, 50),
                "p95": percentile(preupload_times, 95),
                "p99": percentile(preupload_times, 99),
            },
            "download_seconds": {
                "p50": percentile(download_times, 50),
                "p95": percentile(download_times, 95),
                "p99": percentile(download_times, 99),
            },
        },
        "size_buckets": size_bucket_summary,
        "lanes": lane_summary,
        "saturation": saturation_metrics,
    }

    comparison = {
        "available": False,
        "macro": {},
        "percentiles": {},
        "saturation": {},
    }

    if previous_report is not None:
        previous_metrics = previous_report.get("metrics", {})
        previous_macro = previous_metrics.get("macro", {})
        previous_percentiles = previous_metrics.get("percentiles", {})
        previous_saturation = previous_metrics.get("saturation", {})
        comparison = {
            "available": True,
            "macro": {
                "total_time_seconds": compute_delta(metrics["macro"]["total_time_seconds"], previous_macro.get("total_time_seconds")),
                "items_total": compute_delta(metrics["macro"]["items_total"], previous_macro.get("items_total")),
                "items_per_min": compute_delta(metrics["macro"]["items_per_min"], previous_macro.get("items_per_min")),
                "sum_pre_queue_wait_seconds": compute_delta(metrics["macro"]["sum_pre_queue_wait_seconds"], previous_macro.get("sum_pre_queue_wait_seconds")),
                "sum_preupload_seconds": compute_delta(metrics["macro"]["sum_preupload_seconds"], previous_macro.get("sum_preupload_seconds")),
                "sum_preupload_wall_e2e_seconds": compute_delta(metrics["macro"]["sum_preupload_wall_e2e_seconds"], previous_macro.get("sum_preupload_wall_e2e_seconds")),
                "total_retries": compute_delta(metrics["macro"]["total_retries"], previous_macro.get("total_retries")),
                "total_fallbacks": compute_delta(metrics["macro"]["total_fallbacks"], previous_macro.get("total_fallbacks")),
                "total_hol_events": compute_delta(metrics["macro"]["total_hol_events"], previous_macro.get("total_hol_events")),
                "upload_getfile_wait_count": compute_delta(metrics["macro"]["upload_getfile_wait_count"], previous_macro.get("upload_getfile_wait_count")),
                "stream_slot_wait_seconds_total": compute_delta(metrics["macro"]["stream_slot_wait_seconds_total"], previous_macro.get("stream_slot_wait_seconds_total")),
                "source_budget_wait_seconds_total": compute_delta(metrics["macro"]["source_budget_wait_seconds_total"], previous_macro.get("source_budget_wait_seconds_total")),
                "source_api_stall_seconds_total": compute_delta(metrics["macro"]["source_api_stall_seconds_total"], previous_macro.get("source_api_stall_seconds_total")),
                "source_budget_events_total": compute_delta(metrics["macro"]["source_budget_events_total"], previous_macro.get("source_budget_events_total")),
                "source_api_events_total": compute_delta(metrics["macro"]["source_api_events_total"], previous_macro.get("source_api_events_total")),
                "relay_policy_events_total": compute_delta(metrics["macro"]["relay_policy_events_total"], previous_macro.get("relay_policy_events_total")),
                "unproductive_contention_seconds": compute_delta(metrics["macro"]["unproductive_contention_seconds"], previous_macro.get("unproductive_contention_seconds")),
            },
            "percentiles": {
                "pre_queue_wait_p95": compute_delta(
                    metrics["percentiles"]["pre_queue_wait_seconds"]["p95"],
                    (((previous_percentiles.get("pre_queue_wait_seconds") or {}).get("p95")) if previous_percentiles else None),
                ),
                "preupload_p95": compute_delta(
                    metrics["percentiles"]["preupload_seconds"]["p95"],
                    (((previous_percentiles.get("preupload_seconds") or {}).get("p95")) if previous_percentiles else None),
                ),
                "download_p95": compute_delta(
                    metrics["percentiles"]["download_seconds"]["p95"],
                    (((previous_percentiles.get("download_seconds") or {}).get("p95")) if previous_percentiles else None),
                ),
            },
            "saturation": {
                "pre_active_max_seconds": compute_delta(
                    metrics["saturation"]["pre_active_max_seconds"],
                    previous_saturation.get("pre_active_max_seconds"),
                ),
                "large_limit_seconds": compute_delta(
                    metrics["saturation"]["large_limit_seconds"],
                    previous_saturation.get("large_limit_seconds"),
                ),
                "ready_stalled_seconds": compute_delta(
                    metrics["saturation"]["ready_stalled_seconds"],
                    previous_saturation.get("ready_stalled_seconds"),
                ),
                "unproductive_contention_seconds": compute_delta(
                    metrics["saturation"]["unproductive_contention_seconds"],
                    previous_saturation.get("unproductive_contention_seconds"),
                ),
            },
        }

        regressions = 0
        total_time_delta = comparison["macro"]["total_time_seconds"]["percent"]
        preq_p95_delta = comparison["percentiles"]["pre_queue_wait_p95"]["percent"]
        preup_p95_delta = comparison["percentiles"]["preupload_p95"]["percent"]
        down_p95_delta = comparison["percentiles"]["download_p95"]["percent"]
        items_per_min_delta = comparison["macro"]["items_per_min"]["percent"]

        if total_time_delta is not None and total_time_delta > REGRESSION_DELTA_PERCENT:
            regressions += 1
        if preq_p95_delta is not None and preq_p95_delta > REGRESSION_DELTA_PERCENT:
            regressions += 1
        if preup_p95_delta is not None and preup_p95_delta > REGRESSION_DELTA_PERCENT:
            regressions += 1
        if down_p95_delta is not None and down_p95_delta > REGRESSION_DELTA_PERCENT:
            regressions += 1
        if items_per_min_delta is not None and items_per_min_delta < -REGRESSION_DELTA_PERCENT:
            regressions += 1

        if regressions >= 2:
            add_alert(
                "config_regression",
                "[Alerta] Regressao de configuracao detectada em comparacao ao run anterior.",
                {"regression_metrics": regressions},
            )

    hot_items_sorted = sorted(
        hot_items,
        key=lambda row: (
            row["preupload_wall_e2e_seconds"],
            row["preupload_seconds"],
            row["pre_queue_wait_seconds"],
            row["retry_delay_seconds"],
        ),
        reverse=True,
    )[:10]

    def hol_window_priority(window):
        if window.get("classification") == "hol_severe":
            return 2
        if window.get("classification") == "hol_moderate":
            return 1
        return 0

    critical_windows = sorted(
        hol_windows,
        key=lambda window: (
            hol_window_priority(window),
            window.get("duration_seconds", 0.0),
            window.get("max_prontos_atras", 0),
        ),
        reverse=True,
    )[:3]

    optimization_hypotheses = []
    top_cause_set = set(top_causes)
    if custom_source_rows and custom_to_legacy_rows:
        optimization_hypotheses.append(
            f"[Acao] Custom caiu para legacy em {len(custom_to_legacy_rows)}/{len(custom_source_rows)} item(ns); "
            f"se isso subir, reduza o escopo do custom ou aumente o sleep_threshold/local retry da origem."
        )
    if source_api_stall_seconds_total > max(20.0, source_budget_wait_seconds_total * 1.25):
        optimization_hypotheses.append(
            f"[Acao] A origem esta limitando mais pela API do que pelo budget interno "
            f"({format_seconds_hms(source_api_stall_seconds_total)} vs {format_seconds_hms(source_budget_wait_seconds_total)}); "
            "o foco deve ser reduzir retry/fallback do custom, nao apertar mais o orcamento por DC."
        )
    if source_budget_wait_seconds_total > max(20.0, source_api_stall_seconds_total * 1.25):
        optimization_hypotheses.append(
            f"[Acao] O budget da origem esta pesando mais que a API "
            f"({format_seconds_hms(source_budget_wait_seconds_total)} vs {format_seconds_hms(source_api_stall_seconds_total)}); "
            "avalie subir slots/DC pequenos ou aliviar a competicao entre grandes."
        )
    if stream_slot_wait_seconds_total >= 30.0 or CAUSE_RELAY_SLOT_BOUND in top_cause_set:
        optimization_hypotheses.append(
            f"[Acao] Stream relay perdeu {format_seconds_hms(stream_slot_wait_seconds_total)} esperando slot; "
            "se o head ainda sofre com grandes=1/1, teste prioridade do head mais agressiva antes de reabrir concorrencia."
        )
    if sum_preupload_wall_e2e_seconds > (sum_preupload_seconds * 1.20) and sum_preupload_seconds > 0:
        optimization_hypotheses.append(
            "[Acao] O tempo fim-a-fim de pre-upload esta bem acima do tempo util; "
            "isso indica churn de retry/reset e deve ser atacado antes de aumentar workers."
        )
    if CAUSE_HOL_BOUND in top_cause_set:
        optimization_hypotheses.append("[Acao] O head segurou itens prontos atras; reduza backlog lateral antes de aumentar throughput bruto.")
    if CAUSE_SCHEDULER_UNFAIR in top_cause_set:
        optimization_hypotheses.append("[Acao] Rebalancear pesos de lane para proteger a fila rapida de itens pequenos.")
    if not optimization_hypotheses:
        optimization_hypotheses.append("[Acao] Ajustar limites de concorrencia gradualmente e comparar p95 de pre_q_wait, pre-up wall_e2e e source_api_stall entre runs.")

    report = {
        "version": ANALYTICS_VERSION,
        "run": {
            "label": run_label,
            "fingerprint": fingerprint,
            "started_at": context.started_at,
            "ended_at": ended_at,
            "channel_source": context.channel_source,
            "destination_chat_id": context.destination["chat_id"],
            "destination_thread_id": context.destination.get("thread_id") or "chat",
            "report_path": build_analytics_report_path(fingerprint, run_label),
            "previous_report_path": previous_report.get("_source_file") if previous_report else None,
        },
        "summary": {
            "total_time_seconds": total_time_seconds,
            "items_per_min": items_per_min,
            "top_causes": top_causes,
            "top_causes_human": top_causes_human,
            "success_count": context.success_count,
            "failure_count": context.failure_count,
            "total_items": context.total_items,
        },
        "metrics": metrics,
        "comparison": comparison,
        "alerts": alerts,
        "critical_windows": critical_windows,
        "hot_items": hot_items_sorted,
        "optimization_hypotheses": optimization_hypotheses,
        "items": item_rows,
        "hol_windows": hol_windows,
        "unproductive_contention_windows": unproductive_windows,
        "events": context.analytics_events,
        "status_snapshots": context.analytics_status_snapshots,
    }
    return report

def build_executive_summary_lines(report):
    lines = []
    summary = report["summary"]
    comparison = report.get("comparison", {})
    metric_total_time_delta = None
    if comparison.get("available"):
        metric_total_time_delta = (comparison.get("macro", {}).get("total_time_seconds", {}) or {}).get("percent")

    lines.append("Resumo Executivo do Run")
    lines.append(
        f"Duracao total: {format_seconds_hms(summary['total_time_seconds'])} "
        f"(Delta vs Run Anterior: {format_delta_percent(metric_total_time_delta)})"
    )
    lines.append(f"Items/min: {summary['items_per_min']:.2f}")
    lines.append(
        "Top 3 causas dominantes: "
        + (", ".join(summary["top_causes_human"]) if summary["top_causes_human"] else "--")
    )

    lines.append("Alertas Automaticos Disparados")
    if report["alerts"]:
        for alert in report["alerts"]:
            lines.append(alert["message"])
    else:
        lines.append("[Alerta] Nenhuma anomalia critica disparada neste run.")

    lines.append("Top 10 Itens Quentes")
    if report["hot_items"]:
        for row in report["hot_items"]:
            lines.append(
                f"Item [Mensagem {row['display_message_id']}]: pre-up = {format_seconds_hms(row['preupload_seconds'])}, "
                f"wall = {format_seconds_hms(row['preupload_wall_e2e_seconds'])}, "
                f"pre_q_wait = {format_seconds_hms(row['pre_queue_wait_seconds'])}, "
                f"source={row['source_mode_initial'] or '--'}->{row['source_mode_final'] or '--'}, "
                f"Causa: {cause_label(row['dominant_cause'])}"
            )
    else:
        lines.append("Item [--]: pre-up = 00:00, pre_q_wait = 00:00, Causa: --")

    lines.append("Janelas de Contencao Criticas")
    if report["critical_windows"]:
        for window in report["critical_windows"]:
            inferred = infer_window_causes(window)
            inferred_text = " + ".join(inferred)
            hol_classification = window.get("classification") or "hol_light"
            lines.append(
                f"Janela [{format_absolute_timestamp(window['start_ts'])} - {format_absolute_timestamp(window['end_ts'])}]"
            )
            lines.append(
                f"Status: pre_active={window['max_pre_active']}/{PREUPLOAD_WORKERS}, "
                f"grandes={window['max_large_active']}/{window.get('max_large_limit', STREAM_RELAY_MAX_LARGE_ACTIVE)} | classe={hol_classification}"
            )
            lines.append(
                f"Gargalo: Head={window['head_label']} segurando {window['max_prontos_atras']} prontos_atras. "
                f"Causa provavel: {inferred_text}."
            )
    else:
        lines.append("Janela [--:--:-- - --:--:--]")
        lines.append("Status: pre_active=0/0, grandes=0/0")
        lines.append("Gargalo: sem contenção critica registrada.")

    lines.append("Hipoteses de Otimizacao (Acao)")
    for hint in report["optimization_hypotheses"]:
        lines.append(hint)

    return lines

def emit_executive_summary(context, report):
    lines = build_executive_summary_lines(report)
    quiet = getattr(context, "log_mode", "normal") == "quiet"
    for line in lines:
        if not quiet:
            print(line)
        write_detailed_log_line(context, f"[{time.strftime('%H:%M:%S')}] [SUMMARY] {line}")

def get_source_reader_mode(file_size, stage_name):
    if stage_name != "main" or not file_size or file_size < SOURCE_READ_CUSTOM_MIN_BYTES:
        return "legacy", 1
    if file_size >= SOURCE_READ_PARALLEL_MIN_BYTES and SOURCE_READ_PARALLEL_SESSIONS > 1:
        return "parallel", SOURCE_READ_PARALLEL_SESSIONS
    return "custom", 1

def get_source_file_id(source_ref):
    if isinstance(source_ref, str):
        return source_ref
    file_id = getattr(source_ref, "file_id", None)
    if file_id:
        return file_id
    media = get_message_media(source_ref)
    file_id = getattr(media, "file_id", None) if media else None
    if file_id:
        return file_id
    raise RuntimeError("Nao foi possivel resolver file_id para leitura da origem.")

def build_source_download_location(source_ref):
    file_id_obj = FileId.decode(get_source_file_id(source_ref))
    if file_id_obj.file_type == FileType.PHOTO:
        return (
            file_id_obj.dc_id,
            raw.types.InputPhotoFileLocation(
                id=file_id_obj.media_id,
                access_hash=file_id_obj.access_hash,
                file_reference=file_id_obj.file_reference,
                thumb_size=file_id_obj.thumbnail_size,
            ),
        )
    return (
        file_id_obj.dc_id,
        raw.types.InputDocumentFileLocation(
            id=file_id_obj.media_id,
            access_hash=file_id_obj.access_hash,
            file_reference=file_id_obj.file_reference,
            thumb_size=file_id_obj.thumbnail_size,
        ),
    )

async def open_source_read_workers(source_client, dc_id, session_count, context, item, stage_name):
    home_dc_id = await source_client.storage.dc_id()
    test_mode = await source_client.storage.test_mode()
    exported_auth = None
    shared_auth_key = None
    worker_states = []

    if dc_id != home_dc_id:
        exported_auth = await call_telegram(
            source_client.invoke,
            raw.functions.auth.ExportAuthorization(dc_id=dc_id),
            sleep_threshold=SOURCE_READ_SLEEP_THRESHOLD_SECONDS,
            _metrics_context=context,
            _trace_item=item,
            _trace_phase=f"source_export_auth_{stage_name}",
        )
    else:
        shared_auth_key = await source_client.storage.auth_key()

    try:
        for _ in range(session_count):
            auth_key = shared_auth_key
            if dc_id != home_dc_id:
                auth_key = await Auth(source_client, dc_id, test_mode).create()
            session = Session(source_client, dc_id, auth_key, test_mode, is_media=True)
            await session.start()
            context.metrics["source_session_starts"] += 1
            if exported_auth is not None:
                await call_telegram(
                    session.invoke,
                    raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes),
                    sleep_threshold=SOURCE_READ_SLEEP_THRESHOLD_SECONDS,
                    _metrics_context=context,
                    _trace_item=item,
                    _trace_phase=f"source_import_auth_{stage_name}",
                )
            worker_states.append({"session": session, "cdn_session": None, "cdn_redirect": None})
        return worker_states
    except Exception:
        await close_source_read_workers(worker_states)
        raise

async def close_source_read_workers(worker_states):
    for worker_state in worker_states:
        cdn_session = worker_state.get("cdn_session")
        if cdn_session is not None:
            try:
                await cdn_session.stop()
            except Exception:
                pass
        session = worker_state.get("session")
        if session is not None:
            try:
                await session.stop()
            except Exception:
                pass

async def fetch_cdn_source_chunk(source_client, worker_state, offset_bytes, context, item, stage_name):
    redirect = worker_state["cdn_redirect"]
    master_session = worker_state["session"]
    cdn_session = worker_state.get("cdn_session")
    if cdn_session is None:
        test_mode = await source_client.storage.test_mode()
        cdn_auth_key = await Auth(source_client, redirect.dc_id, test_mode).create()
        cdn_session = Session(source_client, redirect.dc_id, cdn_auth_key, test_mode, is_media=True, is_cdn=True)
        await cdn_session.start()
        worker_state["cdn_session"] = cdn_session
        context.metrics["source_cdn_session_starts"] += 1

    while True:
        response = await call_telegram(
            cdn_session.invoke,
            raw.functions.upload.GetCdnFile(
                file_token=redirect.file_token,
                offset=offset_bytes,
                limit=SOURCE_READ_CHUNK_BYTES,
            ),
            sleep_threshold=SOURCE_READ_SLEEP_THRESHOLD_SECONDS,
            _metrics_context=context,
            _trace_item=item,
            _trace_phase=f"source_read_cdn_{stage_name}",
        )

        if isinstance(response, raw.types.upload.CdnFileReuploadNeeded):
            await call_telegram(
                master_session.invoke,
                raw.functions.upload.ReuploadCdnFile(
                    file_token=redirect.file_token,
                    request_token=response.request_token,
                ),
                sleep_threshold=SOURCE_READ_SLEEP_THRESHOLD_SECONDS,
                _metrics_context=context,
                _trace_item=item,
                _trace_phase=f"source_cdn_reupload_{stage_name}",
            )
            continue

        encrypted_chunk = response.bytes
        decrypted_chunk = aes.ctr256_decrypt(
            encrypted_chunk,
            redirect.encryption_key,
            bytearray(
                redirect.encryption_iv[:-4]
                + (offset_bytes // 16).to_bytes(4, "big")
            ),
        )
        hashes = await call_telegram(
            master_session.invoke,
            raw.functions.upload.GetCdnFileHashes(file_token=redirect.file_token, offset=offset_bytes),
            sleep_threshold=SOURCE_READ_SLEEP_THRESHOLD_SECONDS,
            _metrics_context=context,
            _trace_item=item,
            _trace_phase=f"source_cdn_hashes_{stage_name}",
        )
        for index, file_hash in enumerate(hashes):
            cdn_chunk = decrypted_chunk[file_hash.limit * index:file_hash.limit * (index + 1)]
            CDNFileHashMismatch.check(
                file_hash.hash == sha256(cdn_chunk).digest(),
                "file_hash.hash == sha256(cdn_chunk).digest()",
            )
        return decrypted_chunk

async def fetch_source_offset_chunk(source_client, worker_state, location, offset_bytes, context, item, stage_name, cdn_logged):
    if worker_state["cdn_redirect"] is None:
        response = await call_telegram(
            worker_state["session"].invoke,
            raw.functions.upload.GetFile(
                location=location,
                offset=offset_bytes,
                limit=SOURCE_READ_CHUNK_BYTES,
                cdn_supported=SOURCE_READ_ENABLE_CDN,
            ),
            sleep_threshold=SOURCE_READ_SLEEP_THRESHOLD_SECONDS,
            _metrics_context=context,
            _trace_item=item,
            _trace_phase=f"source_read_{stage_name}",
        )
        if isinstance(response, raw.types.upload.File):
            return response.bytes
        if isinstance(response, raw.types.upload.FileCdnRedirect):
            worker_state["cdn_redirect"] = response
            context.metrics["source_cdn_redirects"] += 1
            if not cdn_logged["value"]:
                cdn_logged["value"] = True
                await log_context(
                    context,
                    f"[SOURCE] {item.label}: CDN redirect ativo | dc {response.dc_id} | fase {stage_name}",
                )
            return await fetch_cdn_source_chunk(source_client, worker_state, offset_bytes, context, item, stage_name)
        raise RuntimeError(f"Resposta inesperada em upload.GetFile: {type(response).__name__}")
    return await fetch_cdn_source_chunk(source_client, worker_state, offset_bytes, context, item, stage_name)

async def fetch_source_offset_chunk_with_retry(source_client, worker_state, location, offset_bytes, context, item, stage_name, cdn_logged):
    attempt = 0
    while True:
        try:
            return await fetch_source_offset_chunk(
                source_client,
                worker_state,
                location,
                offset_bytes,
                context,
                item,
                stage_name,
                cdn_logged,
            )
        except Exception as error:
            if not is_source_retryable_error(error):
                raise
            attempt += 1
            if attempt > SOURCE_READ_LOCAL_RETRY_MAX_ATTEMPTS:
                raise
            backoff = min(
                SOURCE_READ_LOCAL_RETRY_BACKOFF_MAX_SECONDS,
                SOURCE_READ_LOCAL_RETRY_BACKOFF_BASE_SECONDS * attempt,
            )
            item_state = ensure_item_analytics_state(context, item)
            item_state["source_local_retry_count"] += 1
            item_state["source_local_retry_seconds"] += float(backoff)
            item_state["source_local_retry_backoff_seconds"] += float(backoff)
            update_item_source_wait_totals(item_state)
            record_analytics_event(
                context,
                "source_local_retry",
                item=item,
                stage_name=stage_name,
                offset_bytes=offset_bytes,
                attempt=attempt,
                backoff_seconds=float(backoff),
                error=simplify_error(error),
            )
            await log_context(
                context,
                f"[SOURCE] {item.label}: retry local da origem ({simplify_error(error)}) "
                f"offset {format_bytes(offset_bytes)} | tentativa {attempt}/{SOURCE_READ_LOCAL_RETRY_MAX_ATTEMPTS} | "
                f"backoff {backoff}s",
            )
            await asyncio.sleep(backoff)

async def stream_source_media_from_legacy_offset(source_client, source_ref, start_chunk_index):
    async for chunk in source_client.stream_media(source_ref, offset=start_chunk_index):
        yield chunk

async def stream_source_media(source_client, context, item, source_ref, file_size, stage_name):
    mode, session_count = get_source_reader_mode(file_size, stage_name)
    dc_id, location = build_source_download_location(source_ref)
    if stage_name == "main":
        set_item_source_mode(context, item, mode, dc_id=dc_id, final=(mode == "legacy"))
    source_bucket = get_source_budget_bucket(file_size)
    budget_bucket = None
    budget_slots_needed = session_count if mode in ("parallel", "custom") else 1

    if mode != "legacy":
        context.metrics["source_custom_files"] += 1
        if mode == "parallel":
            context.metrics["source_parallel_files"] += 1

    total_chunks = int(math.ceil(file_size / SOURCE_READ_CHUNK_BYTES)) if file_size else 0
    worker_states = []
    worker_tasks = []
    result_queue = asyncio.Queue()
    next_chunk_lock = asyncio.Lock()
    next_chunk_index = 0
    next_expected_chunk = 0
    cdn_logged = {"value": False}
    yielded_any_chunk = False

    async def cleanup_custom_source_pipeline(release_budget=True):
        nonlocal budget_bucket, worker_tasks, worker_states
        for worker_task in worker_tasks:
            worker_task.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        worker_tasks = []
        await close_source_read_workers(worker_states)
        worker_states = []
        if release_budget and budget_bucket is not None:
            await release_source_budget(context, dc_id, budget_slots_needed, budget_bucket, file_size)
            budget_bucket = None

    if should_log_large_relay(item, file_size) or mode == "parallel":
        await log_context(
            context,
            f"[SOURCE] {item.label}: leitura origem {mode} | sessoes {session_count} | dc {dc_id} | "
            f"faixa {get_source_budget_bucket_label(source_bucket)} | chunk {format_bytes(SOURCE_READ_CHUNK_BYTES)} | "
            f"cdn {'on' if SOURCE_READ_ENABLE_CDN else 'off'}",
        )

    async def source_worker(worker_state):
        nonlocal next_chunk_index
        try:
            while True:
                async with next_chunk_lock:
                    chunk_index = next_chunk_index
                    next_chunk_index += 1
                if chunk_index >= total_chunks:
                    break
                offset_bytes = chunk_index * SOURCE_READ_CHUNK_BYTES
                chunk = await fetch_source_offset_chunk_with_retry(
                    source_client,
                    worker_state,
                    location,
                    offset_bytes,
                    context,
                    item,
                    stage_name,
                    cdn_logged,
                )
                await result_queue.put(("chunk", chunk_index, chunk))
        except Exception as error:
            await result_queue.put(("error", error, None))
        finally:
            await result_queue.put(("done", None, None))

    try:
        budget_bucket = await acquire_source_budget(context, item, dc_id, budget_slots_needed, file_size)

        if mode == "legacy":
            async for chunk in source_client.stream_media(source_ref):
                yielded_any_chunk = True
                yield chunk
            return

        worker_states = await open_source_read_workers(
            source_client,
            dc_id,
            session_count,
            context,
            item,
            stage_name,
        )
        worker_tasks = [asyncio.create_task(source_worker(worker_state)) for worker_state in worker_states]

        pending_chunks = {}
        finished_workers = 0

        while next_expected_chunk < total_chunks:
            while next_expected_chunk not in pending_chunks:
                event_type, payload_a, payload_b = await result_queue.get()
                if event_type == "chunk":
                    pending_chunks[payload_a] = payload_b
                    continue
                if event_type == "error":
                    raise payload_a
                finished_workers += 1
                if finished_workers >= session_count and next_expected_chunk not in pending_chunks:
                    raise RuntimeError("Leitura paralela da origem terminou antes de entregar todos os chunks.")
            yielded_any_chunk = True
            yield pending_chunks.pop(next_expected_chunk)
            next_expected_chunk += 1
    except Exception as error:
        if mode != "legacy" and is_source_retryable_error(error):
            context.metrics["source_fallbacks"] += 1
            item_state = ensure_item_analytics_state(context, item)
            item_state["fallback_legacy"] = True
            item_state["source_fallback_count"] += 1
            item_state["source_fallback_resume_chunk"] = next_expected_chunk
            item.source_fallback_count += 1
            item.source_fallback_resume_chunk = next_expected_chunk
            set_item_source_mode(context, item, "legacy", dc_id=dc_id, final=True)
            record_analytics_event(
                context,
                "source_fallback_legacy",
                item=item,
                mode=mode,
                stage_name=stage_name,
                resume_chunk=next_expected_chunk,
                reason=simplify_error(error),
            )
            await cleanup_custom_source_pipeline(release_budget=False)
            await log_context(
                context,
                f"[SOURCE] {item.label}: leitura {mode} recuou para legacy apos limitacao da origem | "
                f"motivo {simplify_error(error)} | retomando do chunk {next_expected_chunk}",
            )
            async for chunk in stream_source_media_from_legacy_offset(source_client, source_ref, next_expected_chunk):
                yield chunk
            return
        raise
    finally:
        await cleanup_custom_source_pipeline()

    if stage_name == "main":
        set_item_source_mode(context, item, mode, dc_id=dc_id, final=True)
    if should_log_large_relay(item, file_size) or mode == "parallel":
        await log_context(
            context,
            f"[SOURCE] {item.label}: leitura origem concluida | modo {mode} | sessoes {session_count} | "
            f"chunks {total_chunks} | cdn {'sim' if cdn_logged['value'] else 'nao'}",
        )

async def stream_relay_input_file_baseline(source_client, upload_client, context, item, source_ref, file_name, file_size, stage_name, stage_key=None):
    async with upload_client.save_file_semaphore:
        part_size = 512 * 1024
        file_total_parts = int(math.ceil((file_size or 0) / part_size)) or 1
        is_big = file_size > 10 * 1024 * 1024
        n_sessions = STREAM_UPLOAD_SESSIONS if is_big else 1
        relay_started_at = time.time()
        source_read_seconds = 0.0
        queue_backpressure_seconds = 0.0
        save_parts_seconds = 0.0
        chunk_count = 0
        errors = []
        sessions = []
        queues = []
        workers = []

        source_dc_id = None
        try:
            source_dc_id, _ = build_source_download_location(source_ref)
        except Exception:
            source_dc_id = None

        if stage_name == "main":
            item.relay_main_started_at = relay_started_at
            set_item_source_mode(context, item, "legacy", dc_id=source_dc_id, final=True)
            mark_wait_reason(context, item, "stream_upload_main")
        else:
            item.relay_thumb_started_at = relay_started_at
            mark_wait_reason(context, item, "stream_upload_thumb")

        if should_log_large_relay(item, file_size):
            await log_context(
                context,
                f"[RELAY] {item.label}: inicio {stage_name} | engine baseline | tam {format_bytes(file_size)} | "
                f"sessoes {n_sessions} | fila/sessao {STREAM_UPLOAD_QUEUE_DEPTH} | chunk 512.0 KB",
            )

        try:
            dc_id = await upload_client.storage.dc_id()
            auth_key = await upload_client.storage.auth_key()
            test_mode = await upload_client.storage.test_mode()
            file_id = upload_client.rnd_id()
            md5_sum = md5() if not is_big else None
            sessions = [
                Session(upload_client, dc_id, auth_key, test_mode, is_media=True)
                for _ in range(n_sessions)
            ]
            queues = [asyncio.Queue(maxsize=STREAM_UPLOAD_QUEUE_DEPTH) for _ in range(n_sessions)]

            for session in sessions:
                await session.start()
                context.metrics["relay_session_starts"] += 1

            async def session_worker(session, queue):
                nonlocal save_parts_seconds
                while True:
                    queue_item = await queue.get()
                    if queue_item is None:
                        queue.task_done()
                        return

                    idx, data = queue_item
                    try:
                        if is_big:
                            rpc = raw.functions.upload.SaveBigFilePart(
                                file_id=file_id,
                                file_part=idx,
                                file_total_parts=file_total_parts,
                                bytes=data,
                            )
                        else:
                            rpc = raw.functions.upload.SaveFilePart(
                                file_id=file_id,
                                file_part=idx,
                                bytes=data,
                            )

                        save_started_at = time.time()
                        mark_wait_reason(context, item, f"save_parts_{stage_name}")
                        await session.invoke(rpc)
                        save_parts_seconds += time.time() - save_started_at
                    except Exception as error:
                        context.metrics["relay_errors"] += 1
                        if "AUTH_KEY_DUPLICATED" in str(error).upper():
                            context.metrics["auth_key_duplicated"] += 1
                        errors.append(error)
                    finally:
                        queue.task_done()

            workers = [
                asyncio.create_task(session_worker(session, queue))
                for session, queue in zip(sessions, queues)
            ]

            file_part = 0
            pending = bytearray()
            source_stream = source_client.stream_media(source_ref).__aiter__()

            while True:
                source_read_started_at = time.time()
                mark_wait_reason(context, item, f"source_read_{stage_name}")
                try:
                    chunk = await source_stream.__anext__()
                except StopAsyncIteration:
                    break

                source_read_seconds += time.time() - source_read_started_at
                pending.extend(chunk)

                while len(pending) >= part_size:
                    sub_chunk = bytes(pending[:part_size])
                    del pending[:part_size]
                    if md5_sum is not None:
                        md5_sum.update(sub_chunk)
                    if errors:
                        raise errors[0]

                    enqueue_started_at = time.time()
                    mark_wait_reason(context, item, f"queue_backpressure_{stage_name}")
                    await queues[file_part % n_sessions].put((file_part, sub_chunk))
                    queue_backpressure_seconds += time.time() - enqueue_started_at
                    file_part += 1
                    chunk_count += 1

                if errors:
                    raise errors[0]

            if pending:
                sub_chunk = bytes(pending)
                if md5_sum is not None:
                    md5_sum.update(sub_chunk)
                if errors:
                    raise errors[0]

                enqueue_started_at = time.time()
                mark_wait_reason(context, item, f"queue_backpressure_{stage_name}")
                await queues[file_part % n_sessions].put((file_part, sub_chunk))
                queue_backpressure_seconds += time.time() - enqueue_started_at
                file_part += 1
                chunk_count += 1

            if errors:
                raise errors[0]

            await asyncio.gather(*(queue.join() for queue in queues))

            if errors:
                raise errors[0]

            context.metrics["relay_files"] += 1
            context.metrics["relay_bytes"] += file_size or 0
            context.metrics["relay_seconds"] += max(0.0, time.time() - relay_started_at)
        finally:
            for queue in queues:
                await queue.put(None)
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
            for session in sessions:
                try:
                    await session.stop()
                except Exception:
                    pass

            if stage_name == "main":
                item.relay_main_source_read_seconds = source_read_seconds
                item.relay_main_queue_backpressure_seconds = queue_backpressure_seconds
                item.relay_main_save_parts_seconds = save_parts_seconds
                item.relay_main_chunks = chunk_count
                item.relay_main_finished_at = time.time()
            else:
                item.relay_thumb_source_read_seconds = source_read_seconds
                item.relay_thumb_queue_backpressure_seconds = queue_backpressure_seconds
                item.relay_thumb_save_parts_seconds = save_parts_seconds
                item.relay_thumb_chunks = chunk_count
                item.relay_thumb_finished_at = time.time()

        if should_log_large_relay(item, file_size):
            finished_at = item.relay_main_finished_at if stage_name == "main" else item.relay_thumb_finished_at
            await log_context(
                context,
                f"[RELAY] {item.label}: fim {stage_name} | engine baseline | "
                f"dur {format_phase_duration(relay_started_at, finished_at)} | "
                f"taxa {format_rate(file_size, relay_started_at, finished_at)} | "
                f"source_read {format_seconds(source_read_seconds)} | "
                f"queue_backpressure {format_seconds(queue_backpressure_seconds)} | "
                f"save_parts {format_seconds(save_parts_seconds)} | chunks {chunk_count}",
            )

        if is_big:
            return raw.types.InputFileBig(
                id=file_id,
                parts=file_total_parts,
                name=file_name,
            )

        md5_hex = "".join(hex(i)[2:].zfill(2) for i in md5_sum.digest())
        return raw.types.InputFile(
            id=file_id,
            parts=file_total_parts,
            name=file_name,
            md5_checksum=md5_hex,
        )

async def stream_relay_input_file_instrumented(source_client, upload_client, context, item, source_ref, file_name, file_size, stage_name, stage_key=None):
    part_size = 512 * 1024
    file_total_parts = int(math.ceil(file_size / part_size)) if file_size else 1
    is_big = file_size > 10 * 1024 * 1024

    resume_state = None
    if stage_key is not None and is_big:
        existing = item.upload_resume_states.get(stage_key)
        if existing is not None and existing.file_total_parts == file_total_parts and existing.is_big == is_big:
            resume_state = existing

    if resume_state is not None:
        file_id = resume_state.file_id
        acked_parts = resume_state.acked_parts
        md5_sum = None
        if acked_parts:
            context.metrics["relay_resumes"] += 1
            await log_context(
                context,
                f"[RELAY] {item.label}: retomando {stage_name} | file_id {file_id} | "
                f"parts_ja_acked {len(acked_parts)}/{file_total_parts}",
            )
    else:
        file_id = upload_client.rnd_id()
        acked_parts = set()
        md5_sum = md5() if not is_big else None
        if stage_key is not None:
            resume_state = UploadResumeState(
                file_id=file_id,
                file_total_parts=file_total_parts,
                is_big=is_big,
                acked_parts=acked_parts,
            )
            item.upload_resume_states[stage_key] = resume_state

    # Para big files: N sessões paralelas, cada uma com socket próprio.
    # Chunks distribuídos round-robin → N chunks em voo sem conflito de recv().
    n_sessions = STREAM_UPLOAD_SESSIONS if is_big else 1
    dc_id = await upload_client.storage.dc_id()
    session_pool = await get_media_session_pool(context, upload_client, dc_id, n_sessions)
    relay_started_at = time.time()
    if stage_name == "main":
        item.relay_main_started_at = relay_started_at
        mark_wait_reason(context, item, "stream_upload_main")
    else:
        item.relay_thumb_started_at = relay_started_at
        mark_wait_reason(context, item, "stream_upload_thumb")
    if should_log_large_relay(item, file_size):
        await log_context(
            context,
            f"[RELAY] {item.label}: inicio {stage_name} | tam {format_bytes(file_size)} | sessoes {n_sessions} | "
            f"fila/sessao {STREAM_UPLOAD_QUEUE_DEPTH} | chunk 512.0 KB",
        )
    transport_error_detected = {"value": False}
    source_read_seconds = 0.0
    queue_backpressure_seconds = 0.0
    save_parts_seconds = 0.0
    chunk_count = 0
    errors = []
    try:
        async with session_pool.lock:
            queues = [asyncio.Queue(maxsize=STREAM_UPLOAD_QUEUE_DEPTH) for _ in session_pool.sessions]

            async def session_worker(session, queue):
                nonlocal save_parts_seconds
                while True:
                    queue_item = await queue.get()
                    if queue_item is None:
                        return
                    idx, data = queue_item
                    try:
                        if is_big:
                            rpc = raw.functions.upload.SaveBigFilePart(
                                file_id=file_id, file_part=idx,
                                file_total_parts=file_total_parts, bytes=data,
                            )
                        else:
                            rpc = raw.functions.upload.SaveFilePart(
                                file_id=file_id, file_part=idx, bytes=data,
                            )
                        save_started_at = time.time()
                        mark_wait_reason(context, item, f"save_parts_{stage_name}")
                        await call_telegram(
                            session.invoke,
                            rpc,
                            _metrics_context=context,
                            _trace_item=item,
                            _trace_phase=f"save_parts_{stage_name}",
                        )
                        save_parts_seconds += time.time() - save_started_at
                        acked_parts.add(idx)
                    except Exception as e:
                        context.metrics["relay_errors"] += 1
                        if "AUTH_KEY_DUPLICATED" in str(e).upper():
                            context.metrics["auth_key_duplicated"] += 1
                        if is_transport_error(e):
                            transport_error_detected["value"] = True
                        errors.append(e)

            workers = [
                asyncio.create_task(session_worker(s, q))
                for s, q in zip(session_pool.sessions, queues)
            ]

            try:
                file_part = 0
                pending = bytearray()
                source_stream = stream_source_media(
                    source_client,
                    context,
                    item,
                    source_ref,
                    file_size,
                    stage_name,
                ).__aiter__()
                while True:
                    source_read_started_at = time.time()
                    mark_wait_reason(context, item, f"source_read_{stage_name}")
                    try:
                        chunk = await source_stream.__anext__()
                    except StopAsyncIteration:
                        break
                    source_read_seconds += time.time() - source_read_started_at
                    pending.extend(chunk)
                    while len(pending) >= part_size:
                        sub_chunk = bytes(pending[:part_size])
                        del pending[:part_size]
                        if md5_sum is not None:
                            md5_sum.update(sub_chunk)
                        if errors:
                            raise errors[0]
                        if file_part in acked_parts:
                            context.metrics["relay_parts_skipped_on_resume"] += 1
                            file_part += 1
                            chunk_count += 1
                            continue
                        enqueue_started_at = time.time()
                        mark_wait_reason(context, item, f"queue_backpressure_{stage_name}")
                        await queues[file_part % n_sessions].put((file_part, sub_chunk))
                        queue_backpressure_seconds += time.time() - enqueue_started_at
                        file_part += 1
                        chunk_count += 1
                if pending:
                    sub_chunk = bytes(pending)
                    if md5_sum is not None:
                        md5_sum.update(sub_chunk)
                    if errors:
                        raise errors[0]
                    if file_part in acked_parts:
                        context.metrics["relay_parts_skipped_on_resume"] += 1
                        file_part += 1
                        chunk_count += 1
                    else:
                        enqueue_started_at = time.time()
                        mark_wait_reason(context, item, f"queue_backpressure_{stage_name}")
                        await queues[file_part % n_sessions].put((file_part, sub_chunk))
                        queue_backpressure_seconds += time.time() - enqueue_started_at
                        file_part += 1
                        chunk_count += 1
            finally:
                for q in queues:
                    await q.put(None)
                await asyncio.gather(*workers, return_exceptions=True)

        context.metrics["relay_files"] += 1
        context.metrics["relay_bytes"] += file_size or 0
        context.metrics["relay_seconds"] += max(0.0, time.time() - relay_started_at)
    finally:
        if stage_name == "main":
            item.relay_main_source_read_seconds = source_read_seconds
            item.relay_main_queue_backpressure_seconds = queue_backpressure_seconds
            item.relay_main_save_parts_seconds = save_parts_seconds
            item.relay_main_chunks = chunk_count
        else:
            item.relay_thumb_source_read_seconds = source_read_seconds
            item.relay_thumb_queue_backpressure_seconds = queue_backpressure_seconds
            item.relay_thumb_save_parts_seconds = save_parts_seconds
            item.relay_thumb_chunks = chunk_count
        if stage_name == "main":
            item.relay_main_finished_at = time.time()
        else:
            item.relay_thumb_finished_at = time.time()
        if transport_error_detected["value"]:
            await invalidate_media_session_pool(context, upload_client, dc_id, n_sessions)

    if should_log_large_relay(item, file_size):
        finished_at = item.relay_main_finished_at if stage_name == "main" else item.relay_thumb_finished_at
        await log_context(
            context,
            f"[RELAY] {item.label}: fim {stage_name} | dur {format_phase_duration(relay_started_at, finished_at)} | "
            f"taxa {format_rate(file_size, relay_started_at, finished_at)} | "
            f"source_read {format_seconds(source_read_seconds)} | queue_backpressure {format_seconds(queue_backpressure_seconds)} | "
            f"save_parts {format_seconds(save_parts_seconds)} | chunks {chunk_count}",
        )

    if errors:
        raise errors[0]

    if is_big:
        return raw.types.InputFileBig(
            id=file_id, parts=file_total_parts, name=file_name,
        )
    md5_hex = "".join(hex(i)[2:].zfill(2) for i in md5_sum.digest())
    return raw.types.InputFile(
        id=file_id, parts=file_total_parts, name=file_name, md5_checksum=md5_hex,
    )

async def stream_relay_input_file(source_client, upload_client, context, item, source_ref, file_name, file_size, stage_name, stage_key=None):
    if STREAM_RELAY_ENGINE == "instrumented":
        return await stream_relay_input_file_instrumented(
            source_client,
            upload_client,
            context,
            item,
            source_ref,
            file_name,
            file_size,
            stage_name,
            stage_key=stage_key,
        )
    return await stream_relay_input_file_baseline(
        source_client,
        upload_client,
        context,
        item,
        source_ref,
        file_name,
        file_size,
        stage_name,
        stage_key=stage_key,
    )

async def relay_upload_media_reference(source_client, upload_client, peer, context, item, message):
    media = get_message_media(message)
    if not media:
        raise RuntimeError("Mensagem sem midia para relay em streaming.")

    input_file = await stream_relay_input_file(
        source_client,
        upload_client,
        context,
        item,
        message,
        get_media_upload_name(message),
        get_media_size(message),
        "main",
        stage_key=(message.id, "main"),
    )

    thumb_input = None
    thumbnail = get_streamable_thumbnail(message) if STREAM_RELAY_INCLUDE_SOURCE_THUMB else None
    if thumbnail and getattr(thumbnail, "file_id", None) and getattr(thumbnail, "file_size", 0):
        thumb_input = await stream_relay_input_file(
            source_client,
            upload_client,
            context,
            item,
            thumbnail.file_id,
            f"thumb_{message.id}.jpg",
            thumbnail.file_size,
            "thumb",
            stage_key=(message.id, "thumb"),
        )

    if message.photo:
        uploaded = await invoke_relay_upload_media(
            upload_client,
            context,
            item,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedPhoto(
                    file=input_file,
                ),
            ),
        )
        return raw.types.InputMediaPhoto(id=build_raw_input_photo(uploaded.photo))

    if message.video:
        uploaded = await invoke_relay_upload_media(
            upload_client,
            context,
            item,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(get_media_upload_name(message)) or "video/mp4",
                    file=input_file,
                    thumb=thumb_input,
                    attributes=[
                        raw.types.DocumentAttributeVideo(
                            supports_streaming=True,
                            duration=getattr(message.video, "duration", 0) or 0,
                            w=getattr(message.video, "width", 0) or 0,
                            h=getattr(message.video, "height", 0) or 0,
                        ),
                        raw.types.DocumentAttributeFilename(file_name=get_media_upload_name(message)),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.audio:
        uploaded = await invoke_relay_upload_media(
            upload_client,
            context,
            item,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(get_media_upload_name(message)) or "audio/mpeg",
                    file=input_file,
                    attributes=[
                        raw.types.DocumentAttributeAudio(
                            duration=getattr(message.audio, "duration", 0) or 0,
                            performer=getattr(message.audio, "performer", None),
                            title=getattr(message.audio, "title", None),
                        ),
                        raw.types.DocumentAttributeFilename(file_name=get_media_upload_name(message)),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.document:
        uploaded = await invoke_relay_upload_media(
            upload_client,
            context,
            item,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(get_media_upload_name(message)) or "application/octet-stream",
                    file=input_file,
                    attributes=[
                        raw.types.DocumentAttributeFilename(file_name=get_media_upload_name(message)),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.animation:
        uploaded = await invoke_relay_upload_media(
            upload_client,
            context,
            item,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(get_media_upload_name(message)) or "video/mp4",
                    file=input_file,
                    thumb=thumb_input,
                    attributes=[
                        raw.types.DocumentAttributeVideo(
                            supports_streaming=True,
                            duration=getattr(message.animation, "duration", 0) or 0,
                            w=getattr(message.animation, "width", 0) or 0,
                            h=getattr(message.animation, "height", 0) or 0,
                        ),
                        raw.types.DocumentAttributeFilename(file_name=get_media_upload_name(message)),
                        raw.types.DocumentAttributeAnimated(),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.sticker:
        uploaded = await invoke_relay_upload_media(
            upload_client,
            context,
            item,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(get_media_upload_name(message)) or "image/webp",
                    file=input_file,
                    attributes=[
                        raw.types.DocumentAttributeFilename(file_name=get_media_upload_name(message)),
                        raw.types.DocumentAttributeSticker(
                            alt=getattr(message.sticker, "emoji", "") or "",
                            stickerset=raw.types.InputStickerSetEmpty(),
                        ),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    raise RuntimeError("Tipo de mídia não suportado para relay em streaming.")

async def relay_upload_media_reference_with_retry(source_client, upload_client, peer, context, item, message, attempts=2):
    current_message = message
    for attempt in range(attempts):
        try:
            remote_media = await relay_upload_media_reference(source_client, upload_client, peer, context, item, current_message)
            return remote_media, current_message
        except Exception as error:
            if "FILE_REFERENCE_EXPIRED" in str(error).upper() and attempt < attempts - 1:
                current_message = await refresh_message(source_client, current_message)
                await asyncio.sleep(1)
                continue
            raise

async def upload_media_reference(upload_client, peer, message, file_name, thumb_path=None):
    if message.photo:
        uploaded = await call_telegram(
            upload_client.invoke,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedPhoto(
                    file=await upload_client.save_file(file_name),
                ),
            ),
        )
        return raw.types.InputMediaPhoto(id=build_raw_input_photo(uploaded.photo))

    if message.video:
        thumb = await upload_client.save_file(thumb_path) if thumb_path else None
        uploaded = await call_telegram(
            upload_client.invoke,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(file_name) or "video/mp4",
                    file=await upload_client.save_file(file_name),
                    thumb=thumb,
                    attributes=[
                        raw.types.DocumentAttributeVideo(
                            supports_streaming=True,
                            duration=await asyncio.to_thread(collect_video_duration, file_name) or getattr(message.video, "duration", 0) or 0,
                            w=getattr(message.video, "width", 0) or 0,
                            h=getattr(message.video, "height", 0) or 0,
                        ),
                        raw.types.DocumentAttributeFilename(file_name=os.path.basename(file_name)),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.audio:
        uploaded = await call_telegram(
            upload_client.invoke,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(file_name) or "audio/mpeg",
                    file=await upload_client.save_file(file_name),
                    attributes=[
                        raw.types.DocumentAttributeAudio(
                            duration=getattr(message.audio, "duration", 0) or 0,
                            performer=getattr(message.audio, "performer", None),
                            title=getattr(message.audio, "title", None),
                        ),
                        raw.types.DocumentAttributeFilename(file_name=os.path.basename(file_name)),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.document:
        uploaded = await call_telegram(
            upload_client.invoke,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(file_name) or "application/octet-stream",
                    file=await upload_client.save_file(file_name),
                    attributes=[
                        raw.types.DocumentAttributeFilename(file_name=os.path.basename(file_name)),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.animation:
        thumb = await upload_client.save_file(thumb_path) if thumb_path else None
        uploaded = await call_telegram(
            upload_client.invoke,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(file_name) or "video/mp4",
                    file=await upload_client.save_file(file_name),
                    thumb=thumb,
                    attributes=[
                        raw.types.DocumentAttributeVideo(
                            supports_streaming=True,
                            duration=getattr(message.animation, "duration", 0) or 0,
                            w=getattr(message.animation, "width", 0) or 0,
                            h=getattr(message.animation, "height", 0) or 0,
                        ),
                        raw.types.DocumentAttributeFilename(file_name=os.path.basename(file_name)),
                        raw.types.DocumentAttributeAnimated(),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    if message.sticker:
        uploaded = await call_telegram(
            upload_client.invoke,
            raw.functions.messages.UploadMedia(
                peer=peer,
                media=raw.types.InputMediaUploadedDocument(
                    mime_type=upload_client.guess_mime_type(file_name) or "image/webp",
                    file=await upload_client.save_file(file_name),
                    attributes=[
                        raw.types.DocumentAttributeFilename(file_name=os.path.basename(file_name)),
                        raw.types.DocumentAttributeSticker(
                            alt=getattr(message.sticker, "emoji", "") or "",
                            stickerset=raw.types.InputStickerSetEmpty(),
                        ),
                    ],
                ),
            ),
        )
        return raw.types.InputMediaDocument(id=build_raw_input_document(uploaded.document))

    raise RuntimeError("Tipo de mídia não suportado para pre-upload.")

async def prepare_single_item_for_publish(source_client, preupload_client, destination_peer, context, item):
    message = item.first_message
    final_caption = get_caption(message, context.custom_caption)
    media_caption, overflow_text = split_caption_for_media(final_caption)
    item.caption_text = media_caption
    item.overflow_text = overflow_text

    if message.text:
        item.state = "ready"
        return

    if item.stream_relay:
        prime_item_source_plan(context, item, message, item.estimated_bytes, stage_name="main")
        await acquire_stream_relay_slot(context, item)
        try:
            item.remote_media, refreshed_message = await relay_upload_media_reference_with_retry(
                source_client,
                preupload_client,
                destination_peer,
                context,
                item,
                message,
            )
        finally:
            await release_stream_relay_slot(context, item)
        item.messages = [refreshed_message]
    else:
        file_name = item.local_paths[0]
        thumb_path = None
        if message.video or message.animation:
            thumb_path = await asyncio.to_thread(extract_thumbnail, file_name)
            if thumb_path:
                item.aux_paths.append(thumb_path)

        item.remote_media = await upload_media_reference(preupload_client, destination_peer, message, file_name, thumb_path)
    item.state = "ready"

async def prepare_album_for_publish(source_client, preupload_client, destination_peer, context, item):
    item.remote_media_group = []
    first_caption, overflow_text = split_caption_for_media(get_caption(item.first_message, context.custom_caption))
    item.caption_text = first_caption
    item.overflow_text = overflow_text

    refreshed_messages = []
    for index, message in enumerate(item.messages):
        if item.stream_relay:
            prime_item_source_plan(context, item, message, get_media_size(message), stage_name="main")
            await acquire_stream_relay_slot(context, item)
            try:
                remote_media, refreshed_message = await relay_upload_media_reference_with_retry(
                    source_client,
                    preupload_client,
                    destination_peer,
                    context,
                    item,
                    message,
                )
            finally:
                await release_stream_relay_slot(context, item)
            refreshed_messages.append(refreshed_message)
        else:
            file_name = item.local_paths[index]
            thumb_path = None
            if message.video:
                thumb_path = await asyncio.to_thread(extract_thumbnail, file_name)
                if thumb_path:
                    item.aux_paths.append(thumb_path)
            remote_media = await upload_media_reference(preupload_client, destination_peer, message, file_name, thumb_path)
        item.remote_media_group.append(remote_media)

    if refreshed_messages:
        item.messages = refreshed_messages
    item.state = "ready"

async def preupload_item(source_client, preupload_client, destination_peer, context, item, worker_id):
    item.preupload_started_at = time.time()
    if item.first_preupload_started_at is None:
        item.first_preupload_started_at = item.preupload_started_at
    mark_wait_reason(context, item, "")
    item.state = "preuploading"
    max_transient_attempts = PREUPLOAD_TRANSIENT_MAX_ATTEMPTS
    transient_attempt = 0
    try:
        while True:
            try:
                if item.kind == "album":
                    await prepare_album_for_publish(source_client, preupload_client, destination_peer, context, item)
                else:
                    await prepare_single_item_for_publish(source_client, preupload_client, destination_peer, context, item)
                break
            except FloodWait as error:
                wait_seconds = max(1, int(getattr(error, "value", 5)))
                state = ensure_item_analytics_state(context, item)
                retry_phase = infer_retry_phase(item, error)
                state["flood_wait_count"] += 1
                state["flood_wait_seconds"] += float(wait_seconds)
                record_retry_phase(state, retry_phase)
                record_analytics_event(
                    context,
                    "preupload_flood_wait_retry",
                    item=item,
                    worker_id=worker_id,
                    wait_seconds=float(wait_seconds),
                    phase=retry_phase,
                )
                await log_context(
                    context,
                    f"[PRE{worker_id}] {item.label}: FloodWait {wait_seconds}s, aguardando para retry (fase {retry_phase})",
                )
                cleanup_paths(item.local_paths)
                cleanup_paths(item.aux_paths)
                item.local_paths = []
                item.aux_paths = []
                await asyncio.sleep(wait_seconds)
            except Exception as error:
                error_text = str(error).upper()
                is_transient = (
                    "FLOOD" in error_text
                    or "GETFILE" in error_text
                    or "FILE_REFERENCE_EXPIRED" in error_text
                    or is_transport_error(error)
                )
                if not is_transient:
                    raise
                transient_attempt += 1
                if transient_attempt >= max_transient_attempts:
                    raise
                backoff = min(
                    PREUPLOAD_TRANSIENT_BACKOFF_MAX_SECONDS,
                    PREUPLOAD_TRANSIENT_BACKOFF_BASE_SECONDS * transient_attempt,
                )
                state = ensure_item_analytics_state(context, item)
                retry_phase = infer_retry_phase(item, error)
                state["transient_retry_count"] += 1
                state["transient_backoff_seconds"] += float(backoff)
                record_retry_phase(state, retry_phase)
                record_analytics_event(
                    context,
                    "preupload_transient_retry",
                    item=item,
                    worker_id=worker_id,
                    backoff_seconds=float(backoff),
                    attempt=transient_attempt,
                    phase=retry_phase,
                    error=simplify_error(error),
                )
                await log_context(
                    context,
                    f"[PRE{worker_id}] {item.label}: erro transitorio ({simplify_error(error)}) "
                    f"fase {retry_phase} | tentativa {transient_attempt}/{max_transient_attempts}, retry em {backoff}s",
                )
                cleanup_paths(item.local_paths)
                cleanup_paths(item.aux_paths)
                item.local_paths = []
                item.aux_paths = []
                await asyncio.sleep(backoff)
        item.preupload_finished_at = time.time()
        item.acked_at = item.preupload_finished_at
        context.metrics["preupload_phase_seconds"] += max(0.0, item.preupload_finished_at - item.preupload_started_at)
        context.metrics["preupload_phase_bytes"] += item.actual_bytes or item.estimated_bytes or 0
        record_preupload_eta(context, item)
        relay_breakdown = ""
        if item.stream_relay and should_log_large_relay(item):
            relay_breakdown = (
                f" | chunks {format_phase_duration(item.relay_main_started_at, item.relay_main_finished_at)}"
                f" | thumb {format_phase_duration(item.relay_thumb_started_at, item.relay_thumb_finished_at)}"
                f" | finalize {format_phase_duration(item.relay_finalize_started_at, item.relay_finalize_finished_at)}"
            )
        await log_context(
            context,
            f"[PRE{worker_id}] {item.label}: pre-upload concluido em {format_phase_duration(item.preupload_started_at, item.preupload_finished_at)} "
            f"| taxa {format_rate(item.actual_bytes or item.estimated_bytes, item.preupload_started_at, item.preupload_finished_at)} "
            f"| inicio_abs {format_absolute_timestamp(item.preupload_started_at)} fim_abs {format_absolute_timestamp(item.preupload_finished_at)}"
            f"{relay_breakdown}",
        )
        async with context.ready_condition:
            context.ready_items[item.seq] = item
            context.ready_condition.notify_all()
        await schedule_more_items(context)
        await notify_pipeline_slots(context)
    except Exception as error:
        item.error = simplify_error(error)
        item.state = "failed"
        item.preupload_finished_at = time.time()
        await log_context(context, f"[PRE{worker_id}] {item.label}: falhou no pre-upload porque {item.error}")
        async with context.ready_condition:
            context.ready_items[item.seq] = item
            context.ready_condition.notify_all()
        await schedule_more_items(context)
        await notify_pipeline_slots(context)
    finally:
        cleanup_paths(item.local_paths)
        cleanup_paths(item.aux_paths)
        item.local_paths = []
        item.aux_paths = []
        if not item.stream_relay:
            await context.disk_budget.release(item)

async def download_media_with_retry(client, context, item, message, file_name, attempts=2):
    current_message = message
    for attempt in range(attempts):
        budget_bucket = None
        dc_id = None
        file_size = 0
        try:
            media = get_message_media(current_message)
            if not media:
                raise RuntimeError("Mensagem sem midia para baixar.")
            file_size = max(0, int(getattr(media, "file_size", 0) or 0))
            dc_id, _ = build_source_download_location(media)
            budget_bucket = await acquire_source_budget(context, item, dc_id, 1, file_size)
            return await call_telegram(
                client.download_media,
                media,
                file_name=file_name,
                _metrics_context=context,
                _trace_item=item,
                _trace_phase="download_media",
            ), current_message
        except Exception as error:
            if "FILE_REFERENCE_EXPIRED" in str(error).upper() and attempt < attempts - 1:
                current_message = await refresh_message(client, current_message)
                await asyncio.sleep(1)
                continue
            raise
        finally:
            if budget_bucket is not None and dc_id is not None:
                await release_source_budget(context, dc_id, 1, budget_bucket, file_size)

async def send_video_with_metadata(client, destination, message, final_caption, file_name):
    duration = await asyncio.to_thread(collect_video_duration, file_name)
    duration = duration or getattr(message.video, "duration", 0) or 0
    width = getattr(message.video, "width", None)
    height = getattr(message.video, "height", None)
    thumbnail_path = await asyncio.to_thread(extract_thumbnail, file_name)

    kwargs = {
        "chat_id": destination["chat_id"],
        "video": file_name,
        "caption": final_caption,
        "duration": duration,
        "supports_streaming": True,
    }
    if destination.get("thread_id"):
        kwargs["message_thread_id"] = destination["thread_id"]
    reply_markup = get_reply_markup(message)
    if reply_markup:
        kwargs["reply_markup"] = reply_markup
    if width:
        kwargs["width"] = width
    if height:
        kwargs["height"] = height
    if thumbnail_path:
        kwargs["thumb"] = thumbnail_path

    try:
        await safe_send_with_buttons(
            client.send_video,
            fallback_func=client.send_video,
            **kwargs,
        )
    finally:
        if thumbnail_path and os.path.exists(thumbnail_path):
            safe_remove(thumbnail_path)

def message_matches_choices(message, choices):
    return (
        (1 in choices and message.photo) or
        (2 in choices and message.audio) or
        (3 in choices and message.video) or
        (4 in choices and message.document) or
        (5 in choices and message.text) or
        (6 in choices and message.sticker) or
        (7 in choices and message.animation)
    )

def is_album_compatible(message):
    return bool(message.photo or message.video or message.audio or message.document)

def build_input_media(message, file_name, caption_text):
    caption_text, _ = split_caption_for_media(caption_text)
    if message.photo:
        return InputMediaPhoto(media=file_name, caption=caption_text or "")
    if message.video:
        thumb = extract_thumbnail(file_name)
        return InputMediaVideo(
            media=file_name,
            caption=caption_text or "",
            duration=collect_video_duration(file_name) or getattr(message.video, "duration", 0) or 0,
            width=getattr(message.video, "width", None),
            height=getattr(message.video, "height", None),
            supports_streaming=True,
            thumb=thumb or None,
        )
    if message.audio:
        return InputMediaAudio(
            media=file_name,
            caption=caption_text or "",
            duration=getattr(message.audio, "duration", None),
            performer=getattr(message.audio, "performer", None),
            title=getattr(message.audio, "title", None),
        )
    if message.document:
        return InputMediaDocument(media=file_name, caption=caption_text or "")
    raise RuntimeError("Tipo de mídia não suportado em álbum.")

async def build_input_media_async(message, file_name, caption_text):
    caption_text, _ = split_caption_for_media(caption_text)
    if message.photo:
        return InputMediaPhoto(media=file_name, caption=caption_text or ""), None
    if message.video:
        thumb = await asyncio.to_thread(extract_thumbnail, file_name)
        duration = await asyncio.to_thread(collect_video_duration, file_name)
        return InputMediaVideo(
            media=file_name,
            caption=caption_text or "",
            duration=duration or getattr(message.video, "duration", 0) or 0,
            width=getattr(message.video, "width", None),
            height=getattr(message.video, "height", None),
            supports_streaming=True,
            thumb=thumb or None,
        ), thumb
    if message.audio:
        return InputMediaAudio(
            media=file_name,
            caption=caption_text or "",
            duration=getattr(message.audio, "duration", None),
            performer=getattr(message.audio, "performer", None),
            title=getattr(message.audio, "title", None),
        ), None
    if message.document:
        return InputMediaDocument(media=file_name, caption=caption_text or ""), None
    raise RuntimeError("Tipo de mídia não suportado em álbum.")

async def send_overflow_text(client, destination, overflow_text):
    if not overflow_text:
        return
    kwargs = {
        "chat_id": destination["chat_id"],
        "text": overflow_text,
    }
    if destination.get("thread_id"):
        kwargs["message_thread_id"] = destination["thread_id"]
    await call_telegram(client.send_message, **kwargs)

async def safe_send_with_buttons(send_func, fallback_func=None, **kwargs):
    try:
        return await call_telegram(send_func, **kwargs)
    except Exception as error:
        if "REPLY_MARKUP_INVALID" in str(error).upper() and fallback_func:
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("reply_markup", None)
            return await call_telegram(fallback_func, **fallback_kwargs)
        raise

def cleanup_paths(paths):
    for path in paths:
        if path and os.path.exists(path):
            safe_remove(path)

async def build_reply_to(client, destination):
    return await pyrogram.utils.get_reply_to(
        client=client,
        chat_id=destination["chat_id"],
        message_thread_id=destination.get("thread_id"),
    )

def extract_sent_message_ids(send_result):
    if send_result is None:
        return []

    if isinstance(send_result, list):
        return [getattr(message, "id", None) for message in send_result if getattr(message, "id", None) is not None]

    direct_id = getattr(send_result, "id", None)
    if direct_id is not None:
        return [direct_id]

    message_ids = []

    for message in getattr(send_result, "messages", []) or []:
        message_id = getattr(message, "id", None)
        if message_id is not None:
            message_ids.append(message_id)

    for update in getattr(send_result, "updates", []) or []:
        update_message = getattr(update, "message", None)
        message_id = getattr(update_message, "id", None)
        if message_id is None:
            message_id = getattr(update, "id", None)
        if message_id is not None:
            message_ids.append(message_id)

    deduped_ids = []
    seen_ids = set()
    for message_id in message_ids:
        if message_id in seen_ids:
            continue
        seen_ids.add(message_id)
        deduped_ids.append(message_id)
    return deduped_ids

def record_published_message_ids(context, item, sent_message_ids):
    if not sent_message_ids:
        return

    for message, sent_message_id in zip(item.messages, sent_message_ids):
        source_message_id = getattr(message, "id", None)
        if source_message_id is not None:
            context.published_message_ids[source_message_id] = sent_message_id

async def send_preuploaded_single_media(client, destination, item):
    reply_markup = get_reply_markup(item.first_message)
    rpc = raw.functions.messages.SendMedia(
        peer=await client.resolve_peer(destination["chat_id"]),
        media=item.remote_media,
        reply_to=await build_reply_to(client, destination),
        random_id=client.rnd_id(),
        reply_markup=await reply_markup.write(client) if reply_markup else None,
        message=item.caption_text or "",
        entities=None,
    )

    try:
        send_result = await call_telegram(client.invoke, rpc)
    except Exception as error:
        if "REPLY_MARKUP_INVALID" in str(error).upper() and reply_markup:
            rpc.reply_markup = None
            send_result = await call_telegram(client.invoke, rpc)
        else:
            raise
    return extract_sent_message_ids(send_result)

async def send_preuploaded_album(client, destination, item):
    multi_media = []
    for index, media in enumerate(item.remote_media_group):
        multi_media.append(
            raw.types.InputSingleMedia(
                media=media,
                random_id=client.rnd_id(),
                message=item.caption_text if index == 0 else "",
                entities=None,
            )
        )

    rpc = raw.functions.messages.SendMultiMedia(
        peer=await client.resolve_peer(destination["chat_id"]),
        multi_media=multi_media,
        reply_to=await build_reply_to(client, destination),
    )
    send_result = await call_telegram(client.invoke, rpc)
    return extract_sent_message_ids(send_result)

async def send_downloaded_message(client, item, destination, custom_caption):
    message = item.first_message
    reply_markup = get_reply_markup(message)

    if message.text:
        links_from_buttons = extract_links_from_buttons(message.reply_markup)
        text_with_links = (message.text + ' ' + links_from_buttons).strip()
        if custom_caption:
            text_with_links = f"{custom_caption} {text_with_links}".strip()
        sent_message = await safe_send_with_buttons(
            client.send_message,
            fallback_func=client.send_message,
            chat_id=destination["chat_id"],
            text=text_with_links,
            reply_markup=reply_markup,
            message_thread_id=destination.get("thread_id"),
        )
        return extract_sent_message_ids(sent_message)

    sent_message_ids = await send_preuploaded_single_media(client, destination, item)
    await send_overflow_text(client, destination, item.overflow_text)
    return sent_message_ids

async def download_item(download_client, context, item, worker_id):
    item.download_started_at = time.time()
    mark_wait_reason(context, item, "")
    item.state = "downloading"

    prefix = f"[DL{worker_id}]"
    if not item.stream_relay and item.estimated_bytes > context.disk_budget.max_bytes:
        await log_context(
            context,
            f"{prefix} {item.label}: aguardou exclusividade por exceder {format_bytes(context.disk_budget.max_bytes)}.",
        )

    budget_status = f"disco local {format_budget_usage(context)}"
    if item.stream_relay:
        queue_status = f"estimado {format_bytes(item.estimated_bytes)} | {budget_status}"
    else:
        queue_status = f"reserva {format_bytes(item.estimated_bytes)} | {budget_status}"

    await log_context(
        context,
        f"{prefix} {item.label}: baixando {item.media_kind} | {queue_status}",
    )

    try:
        if item.stream_relay:
            item.download_finished_at = time.time()
            context.metrics["download_phase_seconds"] += max(0.0, item.download_finished_at - item.download_started_at)
            context.metrics["download_phase_bytes"] += item.estimated_bytes or 0
            await log_context(
                context,
                f"{prefix} {item.label}: relay em streaming habilitado, sem download em disco | disco local {format_budget_usage(context)} "
                f"| taxa {format_rate(item.estimated_bytes, item.download_started_at, item.download_finished_at)}",
            )
            item.state = "downloaded"
            await log_context(
                context,
                f"[QUEUE] {item.label}: entrou na fila de pre-upload | relay em streaming | lane {queue_lane_label(context, item)} | disco local {format_budget_usage(context)}",
            )
            await enqueue_preupload_item(context, item)
            return

        if item.kind == "album":
            refreshed_messages = []
            local_paths = []
            for message in item.messages:
                file_name, refreshed_message = await download_media_with_retry(
                    download_client,
                    context,
                    item,
                    message,
                    get_cleaned_file_path(get_message_media(message), download_path, message.id),
                )
                refreshed_messages.append(refreshed_message)
                local_paths.append(file_name)
            item.messages = refreshed_messages
            item.local_paths = local_paths
        else:
            message = item.first_message
            if not message.text:
                file_name, refreshed_message = await download_media_with_retry(
                    download_client,
                    context,
                    item,
                    message,
                    get_cleaned_file_path(get_message_media(message), download_path, message.id),
                )
                item.messages = [refreshed_message]
                item.local_paths = [file_name]

        item.download_finished_at = time.time()
        actual_bytes = measure_paths_size(item.local_paths)
        await context.disk_budget.adjust_after_download(item, actual_bytes)
        context.metrics["download_phase_seconds"] += max(0.0, item.download_finished_at - item.download_started_at)
        context.metrics["download_phase_bytes"] += actual_bytes
        await log_context(
            context,
            f"{prefix} {item.label}: download concluido em {format_phase_duration(item.download_started_at, item.download_finished_at)} | "
            f"disco local {format_budget_usage(context)} | taxa {format_rate(actual_bytes, item.download_started_at, item.download_finished_at)} | "
            f"inicio_abs {format_absolute_timestamp(item.download_started_at)} fim_abs {format_absolute_timestamp(item.download_finished_at)}",
        )
        item.state = "downloaded"
        await log_context(
            context,
            f"[QUEUE] {item.label}: entrou na fila de pre-upload | lane {queue_lane_label(context, item)} | disco local {format_budget_usage(context)}",
        )
        await enqueue_preupload_item(context, item)
    except Exception as error:
        cleanup_paths(item.local_paths)
        cleanup_paths(item.aux_paths)
        item.local_paths = []
        item.aux_paths = []
        item.error = simplify_error(error)
        item.state = "failed"
        item.download_finished_at = time.time()
        if not item.stream_relay:
            await context.disk_budget.release(item)
        await log_context(context, f"{prefix} {item.label}: falhou no download porque {item.error}")
    finally:
        if item.state == "failed":
            async with context.ready_condition:
                context.ready_items[item.seq] = item
                context.ready_condition.notify_all()
            await schedule_more_items(context)
            await notify_pipeline_slots(context)

async def downloader_loop(download_client, context, worker_id):
    while True:
        _, _, item = await context.download_queue.get()
        acquired_background_slot = False
        try:
            if item is None:
                return
            queue_wait = max(0.0, time.time() - (item.download_queued_at or time.time()))
            context.active_download_workers += 1
            await log_context(
                context,
                f"[DL{worker_id}] {item.label}: saiu da fila | esperou {format_seconds(queue_wait)} | "
                f"prof_fila_enfileirar {item.download_queue_depth_at_enqueue} | itens_frente {item.download_items_ahead_at_enqueue}",
            )
            acquired_background_slot = await acquire_background_slot(context, "download", item)
            if not item.stream_relay:
                waited_for_budget = await context.disk_budget.reserve(
                    item,
                    use_reserved_headroom=is_head_priority_item(context, item),
                )
                if waited_for_budget:
                    mark_wait_reason(context, item, "waiting_for_disk_budget")
                    await log_context(
                        context,
                        f"[DL{worker_id}] {item.label}: orcamento local liberado, retomando download | disco local {format_budget_usage(context)}",
                    )
            await download_item(download_client, context, item, worker_id)
        finally:
            if item is not None:
                context.active_download_workers = max(0, context.active_download_workers - 1)
            await release_background_slot(context, "download", acquired_background_slot)
            context.download_queue.task_done()

async def preuploader_loop(source_client, preupload_client, context, worker_id):
    destination_peer = await preupload_client.resolve_peer(context.destination["chat_id"])
    while True:
        _, _, item = await context.preupload_queue.get()
        acquired_background_slot = False
        try:
            if item is None:
                return
            queue_wait = max(0.0, time.time() - (item.preupload_queued_at or time.time()))
            lane = queue_lane_label(context, item)
            item_state = ensure_item_analytics_state(context, item)
            item_state["lane"] = lane
            item_state["pre_worker"] = f"PRE{worker_id}"
            item_state["pre_queue_wait_seconds"] = queue_wait
            item_state["download_queue_wait_seconds"] = max(0.0, time.time() - (item.download_queued_at or time.time())) if item.download_queued_at else 0.0
            context.active_preupload_workers += 1
            acquired_background_slot = await acquire_background_slot(context, "preupload", item)
            await log_context(
                context,
                f"[PRE{worker_id}] {item.label}: iniciando pre-upload | lane {lane} | "
                f"espera_fila {format_seconds(queue_wait)} | prof_fila_enfileirar {item.preupload_queue_depth_at_enqueue} | "
                f"itens_frente {item.preupload_items_ahead_at_enqueue} | inicio_abs {format_absolute_timestamp(time.time())}",
            )
            await preupload_item(source_client, preupload_client, destination_peer, context, item, worker_id)
        finally:
            if item is not None:
                context.active_preupload_workers = max(0, context.active_preupload_workers - 1)
            await release_background_slot(context, "preupload", acquired_background_slot)
            context.preupload_queue.task_done()

async def send_item(client, context, item):
    item.send_started_at = time.time()
    item.publish_started_at = item.send_started_at
    await log_context(
        context,
        f"[PUB] {item.label}: enviando {item.media_kind} | pronto {format_budget_usage(context)} | "
        f"inicio_abs {format_absolute_timestamp(item.publish_started_at)}",
    )

    if item.kind == "album":
        sent_message_ids = await send_preuploaded_album(client, context.destination, item)
        await send_overflow_text(client, context.destination, item.overflow_text)
    else:
        sent_message_ids = await send_downloaded_message(client, item, context.destination, context.custom_caption)

    record_published_message_ids(context, item, sent_message_ids)

    item.send_finished_at = time.time()
    item.publish_finished_at = item.send_finished_at
    item.acked_at = item.send_finished_at
    context.metrics["publish_phase_seconds"] += max(0.0, item.publish_finished_at - item.publish_started_at)
    context.metrics["publish_items"] += 1

async def fetch_pinned_message_ids(client, chat_id, top_msg_id=None):
    peer = await client.resolve_peer(chat_id)
    pinned_message_ids = []
    seen_ids = set()
    offset_id = 0

    while True:
        result = await call_telegram(
            client.invoke,
            raw.functions.messages.Search(
                peer=peer,
                q="",
                filter=raw.types.InputMessagesFilterPinned(),
                min_date=0,
                max_date=0,
                offset_id=offset_id,
                add_offset=0,
                limit=100,
                max_id=0,
                min_id=0,
                hash=0,
                top_msg_id=top_msg_id,
            ),
        )
        batch_ids = []
        for message in getattr(result, "messages", []) or []:
            message_id = getattr(message, "id", None)
            if message_id is None or message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            batch_ids.append(message_id)
            pinned_message_ids.append(message_id)
        if len(batch_ids) < 100:
            break
        offset_id = batch_ids[-1]

    return pinned_message_ids

def build_pinned_sync_order(source_pinned_message_ids, primary_pinned_message_id):
    ordered_ids = []
    seen_ids = set()
    for message_id in source_pinned_message_ids:
        if message_id in seen_ids:
            continue
        seen_ids.add(message_id)
        ordered_ids.append(message_id)

    if primary_pinned_message_id and primary_pinned_message_id in ordered_ids:
        ordered_ids = [message_id for message_id in ordered_ids if message_id != primary_pinned_message_id]
        ordered_ids.append(primary_pinned_message_id)

    return ordered_ids

async def sync_pinned_messages(client, context):
    source_pinned_message_ids = build_pinned_sync_order(
        context.source_pinned_message_ids,
        context.source_primary_pinned_message_id,
    )

    await log_context(
        context,
        f"[PIN] Origem possui {len(source_pinned_message_ids)} mensagem(ns) fixada(s) | "
        f"ids {source_pinned_message_ids or '[]'} | atual {context.source_primary_pinned_message_id or '--'}.",
    )

    destination_top_msg_id = context.destination.get("thread_id")
    destination_current_pinned_ids = await fetch_pinned_message_ids(
        client,
        context.destination["chat_id"],
        top_msg_id=destination_top_msg_id,
    )
    await log_context(
        context,
        f"[PIN] Destino atualmente possui {len(destination_current_pinned_ids)} mensagem(ns) fixada(s) | "
        f"ids {destination_current_pinned_ids or '[]'}.",
    )

    target_destination_ids = []
    missing_source_ids = []
    for source_message_id in source_pinned_message_ids:
        destination_message_id = context.published_message_ids.get(source_message_id)
        if destination_message_id is None:
            missing_source_ids.append(source_message_id)
            continue
        target_destination_ids.append(destination_message_id)
        await log_context(
            context,
            f"[PIN] Mapeamento de pin: origem {source_message_id} -> destino {destination_message_id}.",
        )

    if missing_source_ids:
        await log_context(
            context,
            f"[PIN] Mensagens fixadas da origem sem mapeamento no destino: {missing_source_ids}.",
        )

    extra_destination_ids = [message_id for message_id in destination_current_pinned_ids if message_id not in target_destination_ids]
    if extra_destination_ids:
        await log_context(
            context,
            f"[PIN] Desfixando {len(extra_destination_ids)} mensagem(ns) extra no destino: {extra_destination_ids}.",
        )
        for destination_message_id in extra_destination_ids:
            await call_telegram(
                client.unpin_chat_message,
                chat_id=context.destination["chat_id"],
                message_id=destination_message_id,
            )

    if not target_destination_ids:
        await log_context(
            context,
            "[PIN] Nenhuma mensagem da origem foi mapeada para fixacao no destino.",
        )
        return

    for destination_message_id in target_destination_ids:
        await call_telegram(
            client.pin_chat_message,
            chat_id=context.destination["chat_id"],
            message_id=destination_message_id,
            disable_notification=True,
        )
        await log_context(
            context,
            f"[PIN] Mensagem {destination_message_id} fixada no destino.",
        )

    await log_context(
        context,
        f"[PIN] Sincronizacao concluida | total fixadas no destino: {len(target_destination_ids)}.",
    )

async def log_item_completion(context, item, success):
    processed = context.success_count + context.failure_count
    elapsed = time.time() - context.started_at
    remaining = max(0, context.total_items - processed)
    eta = format_seconds((elapsed / processed) * remaining) if processed else "--:--"
    status = "OK" if success else "ERRO"
    extra = f" | motivo: {item.error}" if item.error else ""
    item_state = ensure_item_analytics_state(context, item)
    item_state["download_queue_wait_seconds"] = seconds_between(item.download_queued_at, item.download_started_at)
    item_state["download_seconds"] = seconds_between(item.download_started_at, item.download_finished_at)
    item_state["pre_queue_wait_seconds"] = seconds_between(item.preupload_queued_at, item.preupload_started_at)
    item_state["preupload_seconds"] = seconds_between(item.preupload_started_at, item.preupload_finished_at)
    item_state["preupload_wall_e2e_seconds"] = seconds_between(item.first_preupload_started_at, item.preupload_finished_at)
    item_state["publish_seconds"] = seconds_between(item.publish_started_at, item.publish_finished_at)
    item_state["source_mode_initial"] = item.source_mode_initial or item_state.get("source_mode_initial", "")
    item_state["source_mode_final"] = item.source_mode_final or item_state.get("source_mode_final", "")
    item_state["source_fallback_count"] = item.source_fallback_count
    item_state["source_fallback_resume_chunk"] = item.source_fallback_resume_chunk
    item_state["source_dc"] = item.source_dc_id
    update_item_source_wait_totals(item_state)
    item_state["success"] = bool(success)
    item_state["error"] = item.error or ""
    record_analytics_event(
        context,
        "item_completed",
        item=item,
        success=bool(success),
        pre_q_wait_seconds=item_state["pre_queue_wait_seconds"],
        preupload_seconds=item_state["preupload_seconds"],
        preupload_wall_e2e_seconds=item_state["preupload_wall_e2e_seconds"],
        source_wait_seconds=item_state["source_wait_seconds"],
        source_budget_wait_seconds=item_state["source_budget_wait_seconds"],
        source_api_stall_seconds=item_state["source_api_stall_seconds"],
        source_local_retry_seconds=item_state["source_local_retry_seconds"],
        stream_slot_wait_seconds=item_state["stream_slot_wait_seconds"],
        fallback_legacy=item_state["fallback_legacy"],
    )
    timing_tail = ""
    if item.stream_relay:
        timing_tail += f" | wall: {format_seconds(item_state['preupload_wall_e2e_seconds'])}"
        timing_tail += f" | slot: {format_seconds(item_state['stream_slot_wait_seconds'])}"
        if item_state["source_wait_seconds"] > 0:
            timing_tail += f" | src_wait: {format_seconds(item_state['source_wait_seconds'])}"
    await log_context(
        context,
        f"[{processed}/{context.total_items}] {status} | restantes: {remaining} | "
        f"decorrido: {format_seconds(elapsed)} | ETA: {eta} | disco local: {format_budget_usage(context)} | "
        f"dl_q_wait: {format_phase_duration(item.download_queued_at, item.download_started_at)} | "
        f"down: {format_phase_duration(item.download_started_at, item.download_finished_at)} | "
        f"pre_q_wait: {format_phase_duration(item.preupload_queued_at, item.preupload_started_at)} | "
        f"pre-up: {format_phase_duration(item.preupload_started_at, item.preupload_finished_at)} | "
        f"pub: {format_phase_duration(item.publish_started_at, item.publish_finished_at)} | "
        f"ack: {format_phase_duration(item.publish_finished_at, item.acked_at)} | "
        f"up: {format_phase_duration(item.send_started_at, item.send_finished_at)}{timing_tail} | {item.label}{extra}",
    )

async def retry_failed_item_before_abort(context, item):
    if item.failure_retry_attempts >= FAILED_ITEM_RETRY_MAX_ATTEMPTS:
        return False
    if not is_retryable_failure_error(item.error):
        return False

    item.failure_retry_attempts += 1
    wait_seconds = min(
        FAILED_ITEM_RETRY_BACKOFF_MAX_SECONDS,
        FAILED_ITEM_RETRY_BACKOFF_BASE_SECONDS * item.failure_retry_attempts,
    )
    attempt = item.failure_retry_attempts
    last_error = item.error

    record_analytics_event(
        context,
        "item_retry_before_abort",
        item=item,
        attempt=attempt,
        max_attempts=FAILED_ITEM_RETRY_MAX_ATTEMPTS,
        wait_seconds=float(wait_seconds),
        error=last_error,
    )
    await log_context(
        context,
        f"[PUB] {item.label}: falha transitoria detectada ({last_error}) | "
        f"retry global {attempt}/{FAILED_ITEM_RETRY_MAX_ATTEMPTS} em {wait_seconds}s antes de abortar.",
    )

    await asyncio.sleep(wait_seconds)
    reset_item_for_retry(item)
    item_state = ensure_item_analytics_state(context, item)
    item_state["source_mode_final"] = ""

    if item.stream_relay:
        await enqueue_preupload_item(context, item)
    elif item.needs_download:
        await enqueue_download_item(context, item)
    else:
        item.state = "ready"
        async with context.ready_condition:
            context.ready_items[item.seq] = item
            context.ready_condition.notify_all()

    await schedule_more_items(context)
    await notify_pipeline_slots(context)
    return True

async def publisher_loop(client, context):
    while context.next_seq_to_publish <= context.total_items:
        async with context.ready_condition:
            while context.next_seq_to_publish not in context.ready_items:
                await context.ready_condition.wait()
            item = context.ready_items.pop(context.next_seq_to_publish)

        if item.state == "failed":
            if await retry_failed_item_before_abort(context, item):
                continue
            context.failure_count += 1
            await log_item_completion(context, item, False)
            await log_context(
                context,
                f"[PUB] interrompendo job no item seq {item.seq} para preservar a ordem. "
                f"Resolva o problema e reexecute para retomar a partir desse ponto.",
            )
            return

        try:
            await send_item(client, context, item)
        except Exception as error:
            item.error = simplify_error(error)
            item.state = "failed"
            item.send_finished_at = time.time()
            if await retry_failed_item_before_abort(context, item):
                continue
            context.failure_count += 1
            await log_item_completion(context, item, False)
            await log_context(
                context,
                f"[PUB] interrompendo job no item seq {item.seq} para preservar a ordem. "
                f"Resolva o problema e reexecute para retomar a partir desse ponto.",
            )
            return
        finally:
            item.aux_paths = []
            item.local_paths = []

        save_progress(context.progress_file, item.last_message_id)
        item.state = "done"
        context.success_count += 1
        await log_item_completion(context, item, True)
        context.next_seq_to_publish += 1
        await schedule_more_items(context)
        await notify_pipeline_slots(context)

def clean_filename(filename):
    unsupported_chars = '<>:"/\\|?#{}[]*'  
    for char in unsupported_chars:
        filename = filename.replace(char, '_')
    filename = filename.strip().strip('.')
    return filename    

def generate_progress_filename(channel_source, destination, chat_title):
    filename = (
        f"{session_name}_{chat_title}_{channel_source}_"
        f"{destination['chat_id']}_{destination.get('thread_id') or 'chat'}.json"
    )
    cleaned_filename = clean_filename(filename)
    return os.path.join("forward_task", cleaned_filename)

def save_progress(filename, last_message_id):
    with open(filename, 'w') as file:
        json.dump({'last_message_id': last_message_id}, file)

def get_previous_progress(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            data = json.load(file)
            return data.get('last_message_id')
    return None        

def choose_resume_mode(progress_file, last_processed_msg_id):
    if not last_processed_msg_id:
        return None

    print(f"Foi encontrado um progresso salvo ate a mensagem {last_processed_msg_id}.")
    answer = input("Deseja retomar dali? (S/n): ").strip().lower()
    if answer in ("", "s", "sim", "y", "yes"):
        return last_processed_msg_id

    try:
        os.remove(progress_file)
    except OSError:
        pass
    print("Progresso antigo ignorado. O envio vai recomecar do inicio.")
    return None

def build_work_queue(messages, choices):
    queue = []
    i = 0
    while i < len(messages):
        message = messages[i]
        media_group_id = getattr(message, "media_group_id", None)

        if media_group_id and is_album_compatible(message):
            album_messages = [message]
            j = i + 1
            while j < len(messages) and getattr(messages[j], "media_group_id", None) == media_group_id:
                album_messages.append(messages[j])
                j += 1

            if all(message_matches_choices(item, choices) and is_album_compatible(item) for item in album_messages):
                queue.append(("album", album_messages))
                i = j
                continue

        if message_matches_choices(message, choices):
            queue.append(("message", message))
        i += 1
    return queue

async def forward_messages_from_channel(choices, channel_source, destination, chat_title, custom_caption=None):
    if custom_caption is None:
        custom_caption = get_custom_caption()
    progress_file = generate_progress_filename(channel_source, destination, chat_title)
    last_processed_msg_id = get_previous_progress(progress_file)
    last_processed_msg_id = choose_resume_mode(progress_file, last_processed_msg_id)

    async with Client(
        session_name,
        no_updates=True,
        sleep_threshold=PYROGRAM_CLIENT_SLEEP_THRESHOLD_SECONDS,
        max_concurrent_transmissions=UPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS,
    ) as upload_client:
        session_string = await upload_client.export_session_string()
        def build_download_client(client_name, takeout):
            return Client(
                client_name,
                session_string=session_string,
                in_memory=True,
                no_updates=True,
                takeout=takeout,
                sleep_threshold=PYROGRAM_CLIENT_SLEEP_THRESHOLD_SECONDS,
                max_concurrent_transmissions=(
                    DOWNLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS_TAKEOUT
                    if takeout
                    else DOWNLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS
                ),
            )

        def build_preupload_client(client_name):
            return Client(
                client_name,
                session_string=session_string,
                in_memory=True,
                no_updates=True,
                sleep_threshold=PYROGRAM_CLIENT_SLEEP_THRESHOLD_SECONDS,
                max_concurrent_transmissions=PREUPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS,
            )

        async def fetch_history(client, source_chat_id):
            await call_telegram(client.get_chat, source_chat_id)
            return [message async for message in client.get_chat_history(source_chat_id)]

        download_client = None
        preupload_clients = []
        status_reporter = None
        download_mode = "normal" if PREFER_DOWNLOAD_MODE_NORMAL else "takeout"

        try:
            if PREFER_DOWNLOAD_MODE_NORMAL:
                download_client = await start_client_with_retry(
                    lambda: build_download_client(f"{session_name}_download_runtime_normal", takeout=False),
                    "download_client_normal",
                )
            else:
                while True:
                    download_client = build_download_client(
                        f"{session_name}_download_runtime",
                        takeout=True,
                    )
                    try:
                        download_client = await start_client_with_retry(
                            lambda: build_download_client(f"{session_name}_download_runtime", takeout=True),
                            "download_client_takeout",
                        )
                        break
                    except TakeoutInitDelay as error:
                        try:
                            await download_client.stop()
                        except Exception:
                            pass
                        download_client = None

                        wait_seconds = getattr(error, "value", None)
                        print("O Telegram exige confirmacao de exportacao no celular para usar o modo takeout.")
                        if wait_seconds:
                            print(f"Se nada for confirmado, o Telegram informou uma espera de {wait_seconds} segundo(s).")
                        print("1 - Vou confirmar no celular agora e tentar takeout novamente")
                        print("2 - Continuar com download normal")
                        print("3 - Cancelar")
                        choice = input("Escolha (1/2/3): ").strip() or "1"

                        if choice == "1":
                            input("Confirme a exportacao no celular e pressione Enter para tentar novamente.")
                            continue
                        if choice == "2":
                            download_client = await start_client_with_retry(
                                lambda: build_download_client(f"{session_name}_download_runtime_fallback", takeout=False),
                                "download_client_fallback",
                            )
                            download_mode = "normal"
                            print("Continuando no modo normal de download.")
                            break
                        raise RuntimeError("Clonagem cancelada pelo usuario.")

            source_chat = await call_telegram(upload_client.get_chat, channel_source)
            source_primary_pinned_message_id = getattr(getattr(source_chat, "pinned_message", None), "id", None)
            source_pinned_message_ids = await fetch_pinned_message_ids(upload_client, channel_source)
            if source_primary_pinned_message_id and source_primary_pinned_message_id not in source_pinned_message_ids:
                source_pinned_message_ids.append(source_primary_pinned_message_id)
            all_messages = await fetch_history(upload_client, channel_source)

            if last_processed_msg_id:
                print(f"Retomando a partir da mensagem seguinte ao ID {last_processed_msg_id}.")
                all_messages = [msg for msg in all_messages if msg.id > last_processed_msg_id]
            else:
                print("Iniciando do começo do canal.")

            all_messages.reverse()
            work_items = build_work_items(all_messages, choices)

            context = CloneJobContext(
                channel_source=channel_source,
                destination=destination,
                chat_title=chat_title,
                custom_caption=custom_caption,
                progress_file=progress_file,
                work_items=work_items,
                disk_budget=DiskBudgetManager(
                    MAX_LOCAL_DISK_BYTES_PER_JOB,
                    reserved_headroom_bytes=DISK_HEADROOM_BYTES,
                ),
                download_queue=asyncio.PriorityQueue(),
                preupload_queue=asyncio.PriorityQueue(),
                destination_peer=await upload_client.resolve_peer(destination["chat_id"]),
                source_pinned_message_ids=source_pinned_message_ids,
                source_primary_pinned_message_id=source_primary_pinned_message_id,
                total_items=len(work_items),
                started_at=time.time(),
                source_download_mode=download_mode,
            )
            configure_detailed_logging(context)
            install_pyrogram_flood_handler(context)
            await probe_telegram_app_config(upload_client, context)
            tg_cap_message = (
                f"Caps Telegram (fonte {context.tg_queue_cap_source}): "
                f"large_queue_max_active_operations_count={context.tg_queue_cap_large}, "
                f"small_queue_max_active_operations_count={context.tg_queue_cap_small}, "
                f"cap_efetivo_tg_large={get_effective_telegram_queue_cap(context, 'tg_large')} "
                f"(modo {context.source_download_mode}, enforced={'on' if ENFORCE_TELEGRAM_QUEUE_CAPS else 'off'})"
            )
            print(tg_cap_message)
            write_detailed_log_line(context, f"[{time.strftime('%H:%M:%S')}] [RUN] {tg_cap_message}")
            for item in context.work_items:
                ensure_item_analytics_state(context, item)

            await schedule_more_items(context)

            for worker_id in range(1, PREUPLOAD_WORKERS + 1):
                preupload_client = await connect_client_with_retry(
                    lambda worker_id=worker_id: build_preupload_client(f"{session_name}_preupload_runtime_{worker_id}"),
                    f"preupload_client_{worker_id}",
                    shared_me=upload_client.me,
                    context=context,
                )
                preupload_clients.append(preupload_client)

            status_reporter = asyncio.create_task(pipeline_status_loop(context))

            if not SOURCE_BUDGET_ENABLED and SOURCE_READ_CUSTOM_MIN_BYTES >= (1 << 50):
                startup_message = (
                    f"Iniciando reenvio de {context.total_items} item(ns) "
                    f"do canal '{chat_title}' para {destination['chat_id']} ({destination['mode_label']}). "
                    f"Downloaders: {DOWNLOAD_WORKERS} | Limite disco local: {format_bytes(MAX_LOCAL_DISK_BYTES_PER_JOB)} | "
                    f"Reserva critica: {format_bytes(DISK_HEADROOM_BYTES)} | Pre-uploaders: {PREUPLOAD_WORKERS} | "
                    f"Streams ativos: {STREAM_RELAY_MAX_ACTIVE} | Sessoes por stream grande: {STREAM_UPLOAD_SESSIONS} | "
                    f"Janela: {SCHEDULE_LOOKAHEAD} | Download: {download_mode} | Relay engine: {STREAM_RELAY_ENGINE}"
                )
            else:
                startup_message = (
                    f"Iniciando reenvio de {context.total_items} item(ns) "
                    f"do canal '{chat_title}' para {destination['chat_id']} ({destination['mode_label']}). "
                    f"Downloaders: {DOWNLOAD_WORKERS} | Limite disco local: {format_bytes(MAX_LOCAL_DISK_BYTES_PER_JOB)} | "
                    f"Reserva critica: {format_bytes(DISK_HEADROOM_BYTES)} | Pre-uploaders: {PREUPLOAD_WORKERS} | "
                    f"Streams ativos: {STREAM_RELAY_MAX_ACTIVE} | Sessoes por stream grande: {STREAM_UPLOAD_SESSIONS} | "
                    f"Fila por sessao: {STREAM_UPLOAD_QUEUE_DEPTH} | Leitura origem: custom>={format_bytes(SOURCE_READ_CUSTOM_MIN_BYTES)} | "
                    f"sleep<={SOURCE_READ_SLEEP_THRESHOLD_SECONDS}s | retry local {SOURCE_READ_LOCAL_RETRY_MAX_ATTEMPTS}x | "
                    f"paralelo {SOURCE_READ_PARALLEL_SESSIONS}x>{format_bytes(SOURCE_READ_PARALLEL_MIN_BYTES)} "
                    f"(orcamento/DC: slots {SOURCE_READ_SESSION_BUDGET}, pequenas {SOURCE_READ_SMALL_MAX_ACTIVE_PER_DC}, "
                    f"grandes {SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC}"
                    f"{f'-{SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC_BURST}' if SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC_BURST > SOURCE_READ_LARGE_MAX_ACTIVE_PER_DC else ''}) | "
                    f"Relay por faixa: >={format_bytes(STREAM_RELAY_HUGE_BYTES)}=>{STREAM_RELAY_MAX_HUGE_ACTIVE}, "
                    f">={format_bytes(STREAM_RELAY_LARGE_BYTES)}=>{STREAM_RELAY_MAX_LARGE_ACTIVE}"
                    f"{f'-{STREAM_RELAY_MAX_LARGE_ACTIVE_BURST}' if STREAM_RELAY_MAX_LARGE_ACTIVE_BURST > STREAM_RELAY_MAX_LARGE_ACTIVE else ''} | "
                    f"CDN origem: {'on' if SOURCE_READ_ENABLE_CDN else 'off'} | Janela: {SCHEDULE_LOOKAHEAD} | "
                    f"Download: {download_mode} | Relay engine: {STREAM_RELAY_ENGINE}"
                )
            if context.log_mode == "quiet":
                print(f"Clonando {context.total_items} item(ns) de '{chat_title}' para {destination['chat_id']}...")
            else:
                print(startup_message)
            write_detailed_log_line(context, f"[{time.strftime('%H:%M:%S')}] [RUN] {startup_message}")
            if context.detailed_log_path:
                log_destination_message = f"Console: {context.log_mode} | Log detalhado temporario: {context.detailed_log_path}"
                if context.log_mode != "quiet":
                    print(log_destination_message)
                write_detailed_log_line(context, f"[{time.strftime('%H:%M:%S')}] [RUN] {log_destination_message}")
            else:
                log_destination_message = f"Console: {context.log_mode} | Log detalhado temporario: off"
                if context.log_mode != "quiet":
                    print(log_destination_message)
                write_detailed_log_line(context, f"[{time.strftime('%H:%M:%S')}] [RUN] {log_destination_message}")

            downloaders = [
                asyncio.create_task(downloader_loop(download_client, context, worker_id))
                for worker_id in range(1, DOWNLOAD_WORKERS + 1)
            ]
            preuploaders = [
                asyncio.create_task(preuploader_loop(download_client, preupload_client, context, worker_id))
                for worker_id, preupload_client in enumerate(preupload_clients, start=1)
            ]
            publisher = asyncio.create_task(publisher_loop(upload_client, context))

            await publisher
            if context.failure_count == 0 and context.success_count == context.total_items:
                await sync_pinned_messages(upload_client, context)
            else:
                await log_context(
                    context,
                    f"[PIN] Sincronizacao ignorada porque o job terminou incompleto | "
                    f"sucessos {context.success_count} | falhas {context.failure_count} | total {context.total_items}.",
                )
            await log_context(context, format_metrics_summary(context))

            previous_report = load_previous_analytics_report(build_run_fingerprint(context))
            analytics_report = build_run_analytics_report(context, previous_report=previous_report)
            context.analytics_report = analytics_report
            save_analytics_report(analytics_report)
            emit_executive_summary(context, analytics_report)
            if ANALYTICS_TO_FILE and analytics_report.get("run", {}).get("report_path"):
                await log_context(
                    context,
                    f"[METRICS] analytics persistido em {analytics_report['run']['report_path']}",
                )

            for _ in range(DOWNLOAD_WORKERS):
                await context.download_queue.put(((float("inf"), float("inf"), float("inf")), float("inf"), None))
            await asyncio.gather(*downloaders)

            for _ in range(PREUPLOAD_WORKERS):
                await context.preupload_queue.put(((float("inf"), float("inf"), float("inf")), float("inf"), None))
            await asyncio.gather(*preuploaders)

            completion_message = (
                f"Tarefa concluída. Sucessos: {context.success_count} | "
                f"Falhas: {context.failure_count} | Total: {context.total_items}"
            )
            print(completion_message)
            write_detailed_log_line(context, f"[{time.strftime('%H:%M:%S')}] [RUN] {completion_message}")
        finally:
            if status_reporter:
                status_reporter.cancel()
                try:
                    await status_reporter
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            if 'context' in locals():
                await close_media_session_pools(context)
                close_detailed_logging(context)
            await close_client_quietly(download_client)
            for preupload_client in preupload_clients:
                await close_client_quietly(preupload_client)

def cleanup_asyncio_warnings():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return
    if loop.is_closed():
        return

    pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
    if not pending:
        return

    for task in pending:
        task.cancel()

    try:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    except Exception:
        pass

    try:
        loop.run_until_complete(loop.shutdown_asyncgens())
    except Exception:
        pass

def cleanup_job_state():
    global run_download_path, task_lock_path
    if run_download_path:
        cleanup_run_download_dir(run_download_path)
        run_download_path = None
    if task_lock_path:
        release_runtime_lock(task_lock_path)
        task_lock_path = None

def cleanup_runtime_state():
    cleanup_job_state()
    release_runtime_lock(runtime_lock_path)

BATCH_COOLDOWN_SECONDS = 60

if __name__ == "__main__":
    warnings.filterwarnings("ignore", message="coroutine 'Client.handle_updates' was never awaited")
    runtime_lock_path = None
    task_lock_path = None
    run_download_path = None
    try:
        show_banner()
        session_name, runtime_lock_path = acquire_available_session(lock_prefix="session_forward")
        authenticate(session_name)
        cache_path()
        job_count = get_batch_job_count()
        jobs = collect_batch_jobs(job_count)
        choices = get_user_choices()
        for index, job in enumerate(jobs):
            print(f"\n>>> Iniciando clonagem {index + 1} de {job_count}: {job['chat_title']}")
            run_id = build_run_id("forward")
            run_download_path = create_run_download_dir(run_id)
            download_path = run_download_path
            task_lock_path = acquire_runtime_lock(
                build_lock_name(
                    "task_forward",
                    job["channel_source"],
                    job["destination"]["chat_id"],
                    job["destination"].get("thread_id") or "chat",
                    job["chat_title"],
                )
            )
            try:
                asyncio.run(
                    forward_messages_from_channel(
                        choices,
                        job["channel_source"],
                        job["destination"],
                        job["chat_title"],
                        custom_caption=job["custom_caption"],
                    )
                )
            finally:
                cleanup_job_state()

            if index < len(jobs) - 1:
                print(f"Aguardando {BATCH_COOLDOWN_SECONDS}s antes da próxima clonagem...")
                time.sleep(BATCH_COOLDOWN_SECONDS)
    finally:
        cleanup_runtime_state()
        cleanup_asyncio_warnings()
