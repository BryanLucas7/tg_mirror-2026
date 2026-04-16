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
import pyrogram

""" Global """
session_name = "user"
download_path = "downloads"
MEDIA_CAPTION_LIMIT = 1024
runtime_lock_path = None
task_lock_path = None
run_download_path = None

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
            random_id=random.getrandbits(64),
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
        print("2 - Converter para supergrupo e depois decidir sobre topicos")
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

def refresh_message(client, message):
    refreshed = client.get_messages(message.chat.id, message.id)
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

def download_media_with_retry(client, message, file_name, attempts=2, progress_callback=None):
    current_message = message
    for attempt in range(attempts):
        try:
            media = get_message_media(current_message)
            if not media:
                raise RuntimeError("Mensagem sem midia para baixar.")
            return client.download_media(
                media,
                file_name=file_name,
                progress=progress_callback,
            ), current_message
        except Exception as error:
            if "FILE_REFERENCE_EXPIRED" in str(error).upper() and attempt < attempts - 1:
                current_message = refresh_message(client, current_message)
                time.sleep(1)
                continue
            raise

def send_video_with_metadata(client, destination, message, final_caption, file_name, progress_callback=None):
    duration = collect_video_duration(file_name) or getattr(message.video, "duration", 0) or 0
    width = getattr(message.video, "width", None)
    height = getattr(message.video, "height", None)
    thumbnail_path = extract_thumbnail(file_name)

    kwargs = {
        "chat_id": destination["chat_id"],
        "video": file_name,
        "caption": final_caption,
        "duration": duration,
        "supports_streaming": True,
    }
    if progress_callback:
        kwargs["progress"] = progress_callback
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
        safe_send_with_buttons(
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

def send_overflow_text(client, destination, overflow_text, processed, total, message_id):
    if not overflow_text:
        return
    print_stage(processed, total, message_id, "enviando texto complementar")
    kwargs = {
        "chat_id": destination["chat_id"],
        "text": overflow_text,
    }
    if destination.get("thread_id"):
        kwargs["message_thread_id"] = destination["thread_id"]
    client.send_message(**kwargs)

def safe_send_with_buttons(send_func, fallback_func=None, **kwargs):
    try:
        return send_func(**kwargs)
    except Exception as error:
        if "REPLY_MARKUP_INVALID" in str(error).upper() and fallback_func:
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("reply_markup", None)
            return fallback_func(**fallback_kwargs)
        raise

def cleanup_paths(paths):
    for path in paths:
        if path and os.path.exists(path):
            safe_remove(path)

def send_media_group_with_fallback(client, messages, destination, custom_caption, processed, total):
    files_to_remove = []
    thumbs_to_remove = []

    try:
        media_group = []
        for index, message in enumerate(messages):
            refreshed_message = refresh_message(client, message)
            media = refreshed_message.photo or refreshed_message.video or refreshed_message.audio or refreshed_message.document
            print_stage(processed, total, message.id, f"baixando item {index + 1}/{len(messages)} do album")
            file_name, refreshed_message = download_media_with_retry(
                client,
                refreshed_message,
                file_name=get_cleaned_file_path(media, download_path, message.id),
                progress_callback=build_progress_callback(
                    processed,
                    total,
                    message.id,
                    f"baixando item {index + 1}/{len(messages)} do album",
                ),
            )
            files_to_remove.append(file_name)

            caption_text = get_caption(refreshed_message, custom_caption) if index == 0 else ""
            input_media = build_input_media(refreshed_message, file_name, caption_text)

            thumb = getattr(input_media, "thumb", None)
            if thumb:
                thumbs_to_remove.append(thumb)
            media_group.append(input_media)

        print_stage(processed, total, messages[0].id, f"enviando album com {len(messages)} item(ns)")
        kwargs = {
            "chat_id": destination["chat_id"],
            "media": media_group,
        }
        if destination.get("thread_id"):
            kwargs["message_thread_id"] = destination["thread_id"]
        client.send_media_group(**kwargs)
        return True
    finally:
        cleanup_paths(thumbs_to_remove)
        cleanup_paths(files_to_remove)

def fallback_send_media(client, message, destination, final_caption, processed, total):
    file_name = None
    media_caption, overflow_text = split_caption_for_media(final_caption)
    reply_markup = get_reply_markup(message)

    try:
        if message.photo:
            print_stage(processed, total, message.id, f"baixando foto ({format_bytes(message.photo.file_size)})")
            file_name, message = download_media_with_retry(
                client,
                refresh_message(client, message),
                get_cleaned_file_path(message.photo, download_path, message.id),
                progress_callback=build_progress_callback(processed, total, message.id, "baixando foto"),
            )
            print_stage(processed, total, message.id, "enviando foto")
            safe_send_with_buttons(
                client.send_photo,
                fallback_func=client.send_photo,
                chat_id=destination["chat_id"],
                photo=file_name,
                caption=media_caption,
                progress=build_progress_callback(processed, total, message.id, "enviando foto"),
                reply_markup=reply_markup,
                message_thread_id=destination.get("thread_id"),
            )
            send_overflow_text(client, destination, overflow_text, processed, total, message.id)
        elif message.audio:
            print_stage(processed, total, message.id, f"baixando audio ({format_bytes(message.audio.file_size)})")
            file_name, message = download_media_with_retry(
                client,
                refresh_message(client, message),
                get_cleaned_file_path(message.audio, download_path, message.id),
                progress_callback=build_progress_callback(processed, total, message.id, "baixando audio"),
            )
            print_stage(processed, total, message.id, "enviando audio")
            safe_send_with_buttons(
                client.send_audio,
                fallback_func=client.send_audio,
                chat_id=destination["chat_id"],
                audio=file_name,
                caption=media_caption,
                progress=build_progress_callback(processed, total, message.id, "enviando audio"),
                reply_markup=reply_markup,
                message_thread_id=destination.get("thread_id"),
            )
            send_overflow_text(client, destination, overflow_text, processed, total, message.id)
        elif message.video:
            file_name, message = download_media_with_retry(
                client,
                refresh_message(client, message),
                get_cleaned_file_path(message.video, download_path, message.id),
                progress_callback=build_progress_callback(processed, total, message.id, "baixando video"),
            )
            send_video_with_metadata(
                client,
                destination,
                message,
                media_caption,
                file_name,
                build_progress_callback(processed, total, message.id, "enviando video"),
            )
            send_overflow_text(client, destination, overflow_text, processed, total, message.id)
        elif message.document:
            print_stage(processed, total, message.id, f"baixando arquivo ({format_bytes(message.document.file_size)})")
            file_name, message = download_media_with_retry(
                client,
                refresh_message(client, message),
                get_cleaned_file_path(message.document, download_path, message.id),
                progress_callback=build_progress_callback(processed, total, message.id, "baixando arquivo"),
            )
            print_stage(processed, total, message.id, "enviando arquivo")
            safe_send_with_buttons(
                client.send_document,
                fallback_func=client.send_document,
                chat_id=destination["chat_id"],
                document=file_name,
                caption=media_caption,
                progress=build_progress_callback(processed, total, message.id, "enviando arquivo"),
                reply_markup=reply_markup,
                message_thread_id=destination.get("thread_id"),
            )
            send_overflow_text(client, destination, overflow_text, processed, total, message.id)
        elif message.sticker:
            print_stage(processed, total, message.id, "baixando sticker")
            file_name, message = download_media_with_retry(
                client,
                refresh_message(client, message),
                get_cleaned_file_path(message.sticker, download_path, message.id),
                progress_callback=build_progress_callback(processed, total, message.id, "baixando sticker"),
            )
            print_stage(processed, total, message.id, "enviando sticker")
            sticker_kwargs = {
                "chat_id": destination["chat_id"],
                "sticker": file_name,
            }
            if destination.get("thread_id"):
                sticker_kwargs["message_thread_id"] = destination["thread_id"]
            client.send_sticker(**sticker_kwargs)
            send_overflow_text(client, destination, overflow_text, processed, total, message.id)
        elif message.animation:
            print_stage(processed, total, message.id, f"baixando animacao ({format_bytes(message.animation.file_size)})")
            file_name, message = download_media_with_retry(
                client,
                refresh_message(client, message),
                get_cleaned_file_path(message.animation, download_path, message.id),
                progress_callback=build_progress_callback(processed, total, message.id, "baixando animacao"),
            )
            print_stage(processed, total, message.id, "enviando animacao")
            safe_send_with_buttons(
                client.send_animation,
                fallback_func=client.send_animation,
                chat_id=destination["chat_id"],
                animation=file_name,
                caption=media_caption,
                progress=build_progress_callback(processed, total, message.id, "enviando animacao"),
                reply_markup=reply_markup,
                message_thread_id=destination.get("thread_id"),
            )
            send_overflow_text(client, destination, overflow_text, processed, total, message.id)
        elif message.text:
            print_stage(processed, total, message.id, "enviando texto")
            safe_send_with_buttons(
                client.send_message,
                fallback_func=client.send_message,
                chat_id=destination["chat_id"],
                text=final_caption or message.text,
                reply_markup=reply_markup,
                message_thread_id=destination.get("thread_id"),
            )
        else:
            return False
        return True
    finally:
        if file_name and os.path.exists(file_name):
            safe_remove(file_name)

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

def forward_message(client, message, destination, progress_file, custom_caption, processed, total):
    try:
        links_from_buttons = extract_links_from_buttons(message.reply_markup)
        final_caption = get_caption(message, custom_caption)

        if message.text:
            text_with_links = (message.text + ' ' + links_from_buttons).strip()
            if custom_caption:
                text_with_links = f"{custom_caption} {text_with_links}".strip()
            print_stage(processed, total, message.id, "enviando texto")
            safe_send_with_buttons(
                client.send_message,
                fallback_func=client.send_message,
                chat_id=destination["chat_id"],
                text=text_with_links,
                reply_markup=get_reply_markup(message),
                message_thread_id=destination.get("thread_id"),
            )
        else:
            if not fallback_send_media(client, message, destination, final_caption, processed, total):
                raise RuntimeError("Tipo de mensagem não suportado para reenvio.")
          
        save_progress(progress_file, message.id)
        return True, f"Mensagem {message.id} ({media_kind_label(message)})"
    except Exception as e:
        print(f"Mensagem {message.id}: falhou porque {simplify_error(e)}")
        return False, f"Mensagem {message.id} ({media_kind_label(message)})"

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

def forward_messages_from_channel(choices, channel_source, destination, chat_title):
    custom_caption = get_custom_caption()  # Pegue a legenda personalizada do usuário
    with Client(session_name) as client:
        progress_file = generate_progress_filename(channel_source, destination, chat_title)
        last_processed_msg_id = get_previous_progress(progress_file)
        last_processed_msg_id = choose_resume_mode(progress_file, last_processed_msg_id)
        all_messages = list(client.get_chat_history(channel_source))
        
        if last_processed_msg_id:
            print(f"Retomando a partir da mensagem seguinte ao ID {last_processed_msg_id}.")
            all_messages = [msg for msg in all_messages if msg.id > last_processed_msg_id]
        else:
            print("Iniciando do começo do canal.")
        all_messages.reverse()
        work_queue = build_work_queue(all_messages, choices)
        total_items = len(work_queue)
        success_count = 0
        failure_count = 0
        started_at = time.time()

        print(
            f"Iniciando reenvio de {total_items} item(ns) "
            f"do canal '{chat_title}' para {destination['chat_id']} ({destination['mode_label']})."
        )

        for index, (kind, payload) in enumerate(work_queue, start=1):
            try:
                if kind == "album":
                    print_stage(index, total_items, payload[0].id, f"baixando album com {len(payload)} item(ns)")
                    send_media_group_with_fallback(client, payload, destination, custom_caption, index, total_items)
                    save_progress(progress_file, payload[-1].id)
                    success_count += 1
                    print_overall_progress(
                        index,
                        total_items,
                        started_at,
                        f"Album {getattr(payload[0], 'media_group_id', 'sem_id')} reenviado",
                        True,
                    )
                else:
                    success, label = forward_message(client, payload, destination, progress_file, custom_caption, index, total_items)
                    if success:
                        success_count += 1
                    else:
                        failure_count += 1
                    print_overall_progress(index, total_items, started_at, label, success)
            except Exception as e:
                failure_count += 1
                label = f"Album {getattr(payload[0], 'media_group_id', 'sem_id')}" if kind == "album" else f"Mensagem {payload.id}"
                print(f"{label}: falhou porque {simplify_error(e)}")
                print_overall_progress(index, total_items, started_at, label, False)
            time.sleep(2)

        print(
            f"Tarefa concluída. Sucessos: {success_count} | "
            f"Falhas: {failure_count} | Total: {total_items}"
        )

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
        forward_messages_from_channel(choices, channel_source, destination, chat_title)
    finally:
        cleanup_runtime_state()
        cleanup_asyncio_warnings()
