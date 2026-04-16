import os
import sys
import time
import json
import asyncio
import warnings
import re
import subprocess
import shutil
import random
from dataclasses import dataclass, field
from typing import List, Optional
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
from pyrogram.errors import FloodWait, TakeoutInitDelay
import pyrogram

""" Global """
session_name = "user"
download_path = "downloads"
MEDIA_CAPTION_LIMIT = 1024
runtime_lock_path = None
task_lock_path = None
run_download_path = None
DOWNLOAD_WORKERS = 4
PREUPLOAD_WORKERS = 3
PUBLISH_WORKERS = 1
MAX_TEMP_BYTES_PER_JOB = 2 * 1024 * 1024 * 1024
RESERVE_MARGIN_SINGLE = 8 * 1024 * 1024
RESERVE_MARGIN_ALBUM = 16 * 1024 * 1024
SCHEDULE_LOOKAHEAD = max(DOWNLOAD_WORKERS + PREUPLOAD_WORKERS + 3, 16)
UPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS = PUBLISH_WORKERS + 1
PREUPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS = PREUPLOAD_WORKERS
DOWNLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS = DOWNLOAD_WORKERS

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
    download_started_at: Optional[float] = None
    download_finished_at: Optional[float] = None
    preupload_started_at: Optional[float] = None
    preupload_finished_at: Optional[float] = None
    send_started_at: Optional[float] = None
    send_finished_at: Optional[float] = None
    remote_media: object = None
    remote_media_group: List[object] = field(default_factory=list)
    caption_text: str = ""

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

class BudgetManager:
    def __init__(self, max_bytes):
        self.max_bytes = max_bytes
        self.reserved_bytes = 0
        self.oversize_owner_seq = None
        self._condition = asyncio.Condition()

    def current_bytes(self):
        return self.reserved_bytes

    def _can_reserve(self, item):
        if self.oversize_owner_seq is not None:
            return False
        if item.estimated_bytes > self.max_bytes:
            return self.reserved_bytes == 0
        return (self.reserved_bytes + item.estimated_bytes) <= self.max_bytes

    async def reserve(self, item):
        waited = False
        async with self._condition:
            while not self._can_reserve(item):
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
    budget: BudgetManager
    download_queue: asyncio.PriorityQueue
    preupload_queue: asyncio.PriorityQueue
    destination_peer: object = None
    ready_items: dict = field(default_factory=dict)
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
    active_background_downloads: int = 0
    active_background_preuploads: int = 0

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
    if not num_bytes:
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

def format_budget_usage(context):
    return f"{format_bytes(context.budget.current_bytes())}/{format_bytes(context.budget.max_bytes)}"

async def log_context(context, message):
    async with context.log_lock:
        print(message)

def build_work_items(messages, choices):
    work_queue = build_work_queue(messages, choices)
    items = []
    for seq, (kind, payload) in enumerate(work_queue, start=1):
        if kind == "album":
            items.append(
                WorkItem(
                    seq=seq,
                    kind="album",
                    messages=list(payload),
                    estimated_bytes=estimate_album_bytes(payload),
                    label=f"Album {getattr(payload[0], 'media_group_id', 'sem_id')}",
                    media_kind="album",
                )
            )
        else:
            message = payload
            items.append(
                WorkItem(
                    seq=seq,
                    kind="message",
                    messages=[message],
                    estimated_bytes=estimate_single_message_bytes(message),
                    label=f"Mensagem {message.id} ({media_kind_label(message)})",
                    media_kind=media_kind_label(message),
                )
            )
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
    if is_head_blocked(context):
        return min(context.total_items, context.next_seq_to_publish + 2)
    return min(context.total_items, context.next_seq_to_publish + SCHEDULE_LOOKAHEAD - 1)

def queue_lane_label(context, item):
    if item.seq == context.next_seq_to_publish:
        return "faixa rapida"
    if item.seq <= context.next_seq_to_publish + 2:
        return "prioridade proxima"
    return "fundo"

async def enqueue_download_item(context, item):
    await context.download_queue.put((item.seq, item))

async def enqueue_preupload_item(context, item):
    await context.preupload_queue.put((item.seq, item))

async def notify_pipeline_slots(context):
    async with context.download_slot_condition:
        context.download_slot_condition.notify_all()
    async with context.preupload_slot_condition:
        context.preupload_slot_condition.notify_all()

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

    async with condition:
        while is_head_blocked(context) and getattr(context, attr_name) >= limit:
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

async def call_telegram(operation, *args, **kwargs):
    while True:
        try:
            return await operation(*args, **kwargs)
        except FloodWait as error:
            await asyncio.sleep(error.value)

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

async def prepare_single_item_for_publish(preupload_client, destination_peer, context, item):
    message = item.first_message
    final_caption = get_caption(message, context.custom_caption)
    media_caption, overflow_text = split_caption_for_media(final_caption)
    item.caption_text = media_caption
    item.overflow_text = overflow_text

    if message.text:
        item.state = "ready"
        return

    file_name = item.local_paths[0]
    thumb_path = None
    if message.video or message.animation:
        thumb_path = await asyncio.to_thread(extract_thumbnail, file_name)
        if thumb_path:
            item.aux_paths.append(thumb_path)

    item.remote_media = await upload_media_reference(preupload_client, destination_peer, message, file_name, thumb_path)
    item.state = "ready"

async def prepare_album_for_publish(preupload_client, destination_peer, context, item):
    item.remote_media_group = []
    first_caption, overflow_text = split_caption_for_media(get_caption(item.first_message, context.custom_caption))
    item.caption_text = first_caption
    item.overflow_text = overflow_text

    for index, message in enumerate(item.messages):
        file_name = item.local_paths[index]
        thumb_path = None
        if message.video:
            thumb_path = await asyncio.to_thread(extract_thumbnail, file_name)
            if thumb_path:
                item.aux_paths.append(thumb_path)
        remote_media = await upload_media_reference(preupload_client, destination_peer, message, file_name, thumb_path)
        item.remote_media_group.append(remote_media)

    item.state = "ready"

async def preupload_item(preupload_client, destination_peer, context, item, worker_id):
    item.preupload_started_at = time.time()
    item.state = "preuploading"
    try:
        if item.kind == "album":
            await prepare_album_for_publish(preupload_client, destination_peer, context, item)
        else:
            await prepare_single_item_for_publish(preupload_client, destination_peer, context, item)
        item.preupload_finished_at = time.time()
        await log_context(
            context,
            f"[PRE{worker_id}] {item.label}: pre-upload concluido em {format_phase_duration(item.preupload_started_at, item.preupload_finished_at)}",
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
        await context.budget.release(item)

async def download_media_with_retry(client, message, file_name, attempts=2):
    current_message = message
    for attempt in range(attempts):
        try:
            media = get_message_media(current_message)
            if not media:
                raise RuntimeError("Mensagem sem midia para baixar.")
            return await call_telegram(
                client.download_media,
                media,
                file_name=file_name,
            ), current_message
        except Exception as error:
            if "FILE_REFERENCE_EXPIRED" in str(error).upper() and attempt < attempts - 1:
                current_message = await refresh_message(client, current_message)
                await asyncio.sleep(1)
                continue
            raise

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
        await call_telegram(client.invoke, rpc)
    except Exception as error:
        if "REPLY_MARKUP_INVALID" in str(error).upper() and reply_markup:
            rpc.reply_markup = None
            await call_telegram(client.invoke, rpc)
        else:
            raise

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
    await call_telegram(client.invoke, rpc)

async def send_downloaded_message(client, item, destination, custom_caption):
    message = item.first_message
    reply_markup = get_reply_markup(message)

    if message.text:
        links_from_buttons = extract_links_from_buttons(message.reply_markup)
        text_with_links = (message.text + ' ' + links_from_buttons).strip()
        if custom_caption:
            text_with_links = f"{custom_caption} {text_with_links}".strip()
        await safe_send_with_buttons(
            client.send_message,
            fallback_func=client.send_message,
            chat_id=destination["chat_id"],
            text=text_with_links,
            reply_markup=reply_markup,
            message_thread_id=destination.get("thread_id"),
        )
        return

    await send_preuploaded_single_media(client, destination, item)
    await send_overflow_text(client, destination, item.overflow_text)

async def download_item(download_client, context, item, worker_id):
    item.download_started_at = time.time()
    item.state = "downloading"

    prefix = f"[DL{worker_id}]"
    if item.estimated_bytes > context.budget.max_bytes:
        await log_context(
            context,
            f"{prefix} {item.label}: aguardou exclusividade por exceder {format_bytes(context.budget.max_bytes)}.",
        )

    await log_context(
        context,
        f"{prefix} {item.label}: baixando {item.media_kind} | reserva {format_bytes(item.estimated_bytes)} | orcamento {format_budget_usage(context)}",
    )

    try:
        if item.kind == "album":
            refreshed_messages = []
            local_paths = []
            for message in item.messages:
                file_name, refreshed_message = await download_media_with_retry(
                    download_client,
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
                    message,
                    get_cleaned_file_path(get_message_media(message), download_path, message.id),
                )
                item.messages = [refreshed_message]
                item.local_paths = [file_name]

        item.download_finished_at = time.time()
        actual_bytes = measure_paths_size(item.local_paths)
        await context.budget.adjust_after_download(item, actual_bytes)
        await log_context(
            context,
            f"{prefix} {item.label}: download concluido em {format_phase_duration(item.download_started_at, item.download_finished_at)} | disco {format_budget_usage(context)}",
        )
        item.state = "downloaded"
        await log_context(
            context,
            f"[QUEUE] {item.label}: entrou na fila de pre-upload | lane {queue_lane_label(context, item)} | disco {format_budget_usage(context)}",
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
        await context.budget.release(item)
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
        _, item = await context.download_queue.get()
        acquired_background_slot = False
        try:
            if item is None:
                return
            acquired_background_slot = await acquire_background_slot(context, "download", item)
            waited_for_budget = await context.budget.reserve(item)
            if waited_for_budget:
                await log_context(
                    context,
                    f"[DL{worker_id}] {item.label}: orcamento liberado, retomando download | disco {format_budget_usage(context)}",
                )
            await download_item(download_client, context, item, worker_id)
        finally:
            await release_background_slot(context, "download", acquired_background_slot)
            context.download_queue.task_done()

async def preuploader_loop(preupload_client, context, worker_id):
    destination_peer = await preupload_client.resolve_peer(context.destination["chat_id"])
    while True:
        _, item = await context.preupload_queue.get()
        acquired_background_slot = False
        try:
            if item is None:
                return
            acquired_background_slot = await acquire_background_slot(context, "preupload", item)
            await log_context(
                context,
                f"[PRE{worker_id}] {item.label}: iniciando pre-upload | lane {queue_lane_label(context, item)}",
            )
            await preupload_item(preupload_client, destination_peer, context, item, worker_id)
        finally:
            await release_background_slot(context, "preupload", acquired_background_slot)
            context.preupload_queue.task_done()

async def send_item(client, context, item):
    item.send_started_at = time.time()
    await log_context(
        context,
        f"[PUB] {item.label}: enviando {item.media_kind} | pronto {format_budget_usage(context)}",
    )

    if item.kind == "album":
        await send_preuploaded_album(client, context.destination, item)
        await send_overflow_text(client, context.destination, item.overflow_text)
    else:
        await send_downloaded_message(client, item, context.destination, context.custom_caption)

    item.send_finished_at = time.time()

async def log_item_completion(context, item, success):
    processed = context.success_count + context.failure_count
    elapsed = time.time() - context.started_at
    remaining = max(0, context.total_items - processed)
    eta = format_seconds((elapsed / processed) * remaining) if processed else "--:--"
    status = "OK" if success else "ERRO"
    extra = f" | motivo: {item.error}" if item.error else ""
    await log_context(
        context,
        f"[{processed}/{context.total_items}] {status} | restantes: {remaining} | "
        f"decorrido: {format_seconds(elapsed)} | ETA: {eta} | disco: {format_budget_usage(context)} | "
        f"down: {format_phase_duration(item.download_started_at, item.download_finished_at)} | "
        f"pre-up: {format_phase_duration(item.preupload_started_at, item.preupload_finished_at)} | "
        f"up: {format_phase_duration(item.send_started_at, item.send_finished_at)} | {item.label}{extra}",
    )

async def publisher_loop(client, context):
    while context.next_seq_to_publish <= context.total_items:
        async with context.ready_condition:
            while context.next_seq_to_publish not in context.ready_items:
                await context.ready_condition.wait()
            item = context.ready_items.pop(context.next_seq_to_publish)

        try:
            if item.state == "failed":
                context.failure_count += 1
                await log_item_completion(context, item, False)
            else:
                await send_item(client, context, item)
                save_progress(context.progress_file, item.last_message_id)
                item.state = "done"
                context.success_count += 1
                await log_item_completion(context, item, True)
        except Exception as error:
            item.error = simplify_error(error)
            item.state = "failed"
            item.send_finished_at = time.time()
            context.failure_count += 1
            await log_item_completion(context, item, False)
        finally:
            item.aux_paths = []
            item.local_paths = []
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

async def forward_messages_from_channel(choices, channel_source, destination, chat_title):
    custom_caption = get_custom_caption()
    progress_file = generate_progress_filename(channel_source, destination, chat_title)
    last_processed_msg_id = get_previous_progress(progress_file)
    last_processed_msg_id = choose_resume_mode(progress_file, last_processed_msg_id)

    async with Client(
        session_name,
        no_updates=True,
        sleep_threshold=30,
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
                sleep_threshold=30,
                max_concurrent_transmissions=DOWNLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS,
            )

        def build_preupload_client(client_name):
            return Client(
                client_name,
                session_string=session_string,
                in_memory=True,
                no_updates=True,
                sleep_threshold=30,
                max_concurrent_transmissions=PREUPLOAD_CLIENT_MAX_CONCURRENT_TRANSMISSIONS,
            )

        async def fetch_history(client, source_chat_id):
            await call_telegram(client.get_chat, source_chat_id)
            return [message async for message in client.get_chat_history(source_chat_id)]

        download_client = None
        preupload_clients = []
        download_mode = "takeout"

        try:
            while True:
                download_client = build_download_client(
                    f"{session_name}_download_runtime",
                    takeout=True,
                )
                try:
                    await download_client.start()
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
                        download_client = build_download_client(
                            f"{session_name}_download_runtime_fallback",
                            takeout=False,
                        )
                        await download_client.start()
                        download_mode = "normal"
                        print("Continuando no modo normal de download.")
                        break
                    raise RuntimeError("Clonagem cancelada pelo usuario.")

            await call_telegram(download_client.get_chat, channel_source)
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
                budget=BudgetManager(MAX_TEMP_BYTES_PER_JOB),
                download_queue=asyncio.PriorityQueue(),
                preupload_queue=asyncio.PriorityQueue(),
                destination_peer=await upload_client.resolve_peer(destination["chat_id"]),
                total_items=len(work_items),
                started_at=time.time(),
            )

            await schedule_more_items(context)

            for worker_id in range(1, PREUPLOAD_WORKERS + 1):
                preupload_client = build_preupload_client(f"{session_name}_preupload_runtime_{worker_id}")
                await preupload_client.start()
                preupload_clients.append(preupload_client)

            print(
                f"Iniciando reenvio de {context.total_items} item(ns) "
                f"do canal '{chat_title}' para {destination['chat_id']} ({destination['mode_label']}). "
                f"Downloaders: {DOWNLOAD_WORKERS} | Limite por job: {format_bytes(MAX_TEMP_BYTES_PER_JOB)} | "
                f"Pre-uploaders: {PREUPLOAD_WORKERS} | Janela: {SCHEDULE_LOOKAHEAD} | Download: {download_mode}"
            )

            downloaders = [
                asyncio.create_task(downloader_loop(download_client, context, worker_id))
                for worker_id in range(1, DOWNLOAD_WORKERS + 1)
            ]
            preuploaders = [
                asyncio.create_task(preuploader_loop(preupload_client, context, worker_id))
                for worker_id, preupload_client in enumerate(preupload_clients, start=1)
            ]
            publisher = asyncio.create_task(publisher_loop(upload_client, context))

            await publisher

            for _ in range(DOWNLOAD_WORKERS):
                await context.download_queue.put((float("inf"), None))
            await asyncio.gather(*downloaders)

            for _ in range(PREUPLOAD_WORKERS):
                await context.preupload_queue.put((float("inf"), None))
            await asyncio.gather(*preuploaders)

            print(
                f"Tarefa concluída. Sucessos: {context.success_count} | "
                f"Falhas: {context.failure_count} | Total: {context.total_items}"
            )
        finally:
            try:
                await download_client.stop()
            except Exception:
                pass
            for preupload_client in preupload_clients:
                try:
                    await preupload_client.stop()
                except Exception:
                    pass

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

def cleanup_runtime_state():
    cleanup_run_download_dir(run_download_path)
    release_runtime_lock(task_lock_path)
    release_runtime_lock(runtime_lock_path)

if __name__ == "__main__":
    warnings.filterwarnings("ignore", message="coroutine 'Client.handle_updates' was never awaited")
    try:
        show_banner()
        session_name, runtime_lock_path = acquire_available_session(lock_prefix="session_forward")
        authenticate(session_name)
        cache_path()
        run_id = build_run_id("forward")
        run_download_path = create_run_download_dir(run_id)
        download_path = run_download_path
        channel_source, destination, chat_title = get_channels()
        task_lock_path = acquire_runtime_lock(
            build_lock_name(
                "task_forward",
                channel_source,
                destination["chat_id"],
                destination.get("thread_id") or "chat",
                chat_title,
            )
        )
        choices = get_user_choices()
        asyncio.run(forward_messages_from_channel(choices, channel_source, destination, chat_title))
    finally:
        cleanup_runtime_state()
        cleanup_asyncio_warnings()
