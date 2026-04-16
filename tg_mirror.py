import os
import time
import json
import asyncio
from pathlib import Path
import subprocess
from tqdm import tqdm
from utils import Banner, show_banner, cache_path, authenticate
import re
import shutil

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client
import pyrogram
import pyrogram.utils
from pyrogram.types import InputMediaAudio, InputMediaDocument, InputMediaPhoto, InputMediaVideo

""" Global """
session_name = "user"
video_path = 'downloads'

def resolve_binary(executable_name):
    local_path = os.path.join("tools", "ffmpeg", "bin", executable_name)
    return local_path if os.path.exists(local_path) else shutil.which(executable_name)

FFMPEG_PATH = resolve_binary("ffmpeg.exe")
FFPROBE_PATH = resolve_binary("ffprobe.exe")

def limpar_nome_arquivo(nome_arquivo):
    nome_limpo = re.sub(r'[^a-zA-Z0-9]', '_', nome_arquivo)    
    chars_invalidos = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in chars_invalidos:
        nome_limpo = nome_limpo.replace(char, '_')
    return nome_limpo

def get_cleaned_file_path(media, directory):
    extension = media.file_name.split('.')[-1] if media.file_name and '.' in media.file_name else 'unknown'
    clean_name = limpar_nome_arquivo(media.file_name or f"{media.file_id}.{extension}")
    return os.path.join(directory, clean_name)

def get_channels():
    with Client(session_name) as client:
        channel_source = input("Forneça o @username ou ID do canal / grupo de origem: ")
        channel_target = input("Forneça o @username ou ID do canal de destino: ")
        channel_source = parse_channel_input(channel_source)
        channel_target = parse_channel_input(channel_target)
        source_chat = client.get_chat(channel_source)
        target_chat = client.get_chat(channel_target)
        return source_chat.id, target_chat.id, source_chat.title

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

def get_user_choices():
    print("Quais conteudos você deseja processar?:\n")
    options = ["Processar todos os Conteúdos", "Fotos", "Áudios", "Vídeos", "Arquivos", "Texto", "Sticker", "Animação - GIFs"]
    for i, option in enumerate(options):
        print(f"{i} - {option}")
    choices = input("\nInforme os conteúdos que deseja procesar separados por vírgula (ex: 1,3) < 0 para processar todos : ").split(',')
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
            if getattr(button, "url", None):
                link_texts.append(f"{button.text} ({button.url})")
    return ' '.join(link_texts)

def extract_text_links_from_caption(message):
    if not hasattr(message, 'caption_entities') or not message.caption_entities:
        return ''

    links = []
    for entity in message.caption_entities:
        if entity.type == "text_link":
            links.append(entity.url)
    return ' '.join(links)

def build_caption(message):
    caption_parts = []

    if message.caption:
        caption_parts.append(str(message.caption))

    links_from_buttons = extract_links_from_buttons(message.reply_markup)
    if links_from_buttons:
        caption_parts.append(links_from_buttons)

    links_from_caption = extract_text_links_from_caption(message)
    if links_from_caption:
        caption_parts.append(links_from_caption)

    return ' '.join(part for part in caption_parts if part).strip()

def extract_thumbnail(video_path: str) -> str:
    if not FFMPEG_PATH:
        return ""
    thumbnail_path = video_path + ".jpg"

    # Extract frame from 00:00:01
    thumbnail_command = [
        FFMPEG_PATH,
        '-v', 'quiet',    
        '-stats',        
        '-y',
        '-i', video_path,
        '-ss', '00:00:01',
        '-vframes', '1',
        thumbnail_path
    ]
    try:
        subprocess.run(thumbnail_command)
        return thumbnail_path
    except Exception as e:
        print(f"Erro ao extrair miniatura: {e}")
        return ""

def collect_video_duration(video_path: str) -> int:
    if not FFPROBE_PATH:
        return 0
    try:
        ffprobe_command = [
            FFPROBE_PATH,
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        duration = subprocess.check_output(ffprobe_command).decode('utf-8').strip()
        return int(float(duration))
    except Exception as e:
        print(f"Erro ao coletar duração do vídeo: {e}")
        return 0

def send_video_with_metadata(client, channel_target, message, file_name, caption_text, progress_callback):
    duration = collect_video_duration(file_name) or getattr(message.video, "duration", 0) or 0
    width = getattr(message.video, "width", None)
    height = getattr(message.video, "height", None)
    thumbnail_path = extract_thumbnail(file_name)

    kwargs = {
        "chat_id": channel_target,
        "video": file_name,
        "caption": caption_text,
        "duration": duration,
        "supports_streaming": True,
        "progress": progress_callback,
    }
    if width:
        kwargs["width"] = width
    if height:
        kwargs["height"] = height
    if thumbnail_path:
        kwargs["thumb"] = thumbnail_path

    try:
        client.send_video(**kwargs)
    finally:
        if thumbnail_path and os.path.exists(thumbnail_path):
            os.remove(thumbnail_path)

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

def cleanup_paths(paths):
    for path in paths:
        if path and os.path.exists(path):
            os.remove(path)

def build_group_input_media(message, file_name, caption_text):
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

def send_media_group_with_reupload(client, album_messages, channel_target):
    files_to_remove = []
    thumbs_to_remove = []

    try:
        media_group = []
        for index, message in enumerate(album_messages):
            media = message.photo or message.video or message.audio or message.document
            file_name = get_cleaned_file_path(media, video_path)
            client.download_media(media, file_name=file_name)
            files_to_remove.append(file_name)

            caption_text = build_caption(message) if index == 0 else ""
            input_media = build_group_input_media(message, file_name, caption_text)
            thumb = getattr(input_media, "thumb", None)
            if thumb:
                thumbs_to_remove.append(thumb)
            media_group.append(input_media)

        client.send_media_group(channel_target, media_group)
    finally:
        cleanup_paths(thumbs_to_remove)
        cleanup_paths(files_to_remove)

def clean_filename(filename):
    unsupported_chars = '<>:"/\\|?#{}[]*'  
    for char in unsupported_chars:
        filename = filename.replace(char, '_')
    filename = filename.strip().strip('.')
    return filename    

def get_json_filepath(channel_source, channel_target, chat_title):
    return f"downloaded_media_{chat_title}_{channel_source}_{channel_target}.json"

def get_json_filepath(channel_source, channel_target, chat_title):
    filename = f"downloaded_media_{chat_title}_{channel_source}_{channel_target}.json"
    cleaned_filename = clean_filename(filename)
    return os.path.join('download_tasks', cleaned_filename)

def download_and_upload_media_from_channel(choices, channel_source, channel_target, chat_title):
    downloaded_media = []
    last_processed_id = 0
    json_filepath = get_json_filepath(channel_source, channel_target, chat_title)

    if os.path.exists(json_filepath):
        with open(json_filepath, "r") as json_file:
            data = json.load(json_file)
            last_processed_id = data["last_processed_id"] if "last_processed_id" in data else 0
            print(f"Retomando do ID da próxima mensagem após a última processada: {last_processed_id + 1}")

    with Client(session_name) as client:
        all_messages = list(client.get_chat_history(channel_source))
        all_messages.reverse()
        start_processing = False

        i = 0
        while i < len(all_messages):
            message = all_messages[i]
            
            if start_processing or message.id > last_processed_id:
                start_processing = True
            else:
                i += 1
                continue

            media_group_id = getattr(message, "media_group_id", None)
            if media_group_id and is_album_compatible(message):
                album_messages = [message]
                j = i + 1
                while j < len(all_messages) and getattr(all_messages[j], "media_group_id", None) == media_group_id:
                    album_messages.append(all_messages[j])
                    j += 1

                if all(message_matches_choices(item, choices) and is_album_compatible(item) for item in album_messages):
                    try:
                        send_media_group_with_reupload(client, album_messages, channel_target)
                        last_processed_id = album_messages[-1].id
                        with open(json_filepath, "w") as json_file:
                            json.dump({"last_processed_id": last_processed_id}, json_file)
                        os.system('clear || cls')
                        print(f"Album {media_group_id} reenviado ao canal de destino.")
                    except Exception as e:
                        print(f"Erro ao reenviar álbum {media_group_id}: {e}")
                    time.sleep(10)
                    i = j
                    continue

            file_name = None
            caption_text = build_caption(message)
            download_start_time = None
            last_update_time = None
            bytes_downloaded = 0
         
            def progress(current, total, operation="Downloading"):
                nonlocal download_start_time, last_update_time, bytes_downloaded
                if download_start_time is None:
                    download_start_time = time.time()
                    last_update_time = download_start_time
                else:
                    current_time = time.time()
                    elapsed_time = current_time - last_update_time
                    
                    if elapsed_time > 0.5:
                        speed_bps = (current - bytes_downloaded) / elapsed_time  # bytes por segundo
                        speed_mbps = (speed_bps * 8) / (10**6)  # megabits por segundo

                        bar.set_description(f"{operation} at {speed_mbps:.2f} Mbps")
                        bytes_downloaded = current
                        last_update_time = current_time
                bar.n = current
                bar.refresh()
    
            if 1 in choices and message.photo:
                file_size = message.photo.file_size
                bar = tqdm(total=file_size, desc="Downloading", leave=False)
                file_name = client.download_media(message.photo, progress=progress)
                client.send_photo(channel_target, file_name, caption=caption_text, progress=lambda c, t: progress(c, t, "Uploading"))

            if 2 in choices and message.audio:
                file_size = message.audio.file_size
                bar = tqdm(total=file_size, desc="Downloading", leave=False)
                file_name = get_cleaned_file_path(message.audio, video_path)
                client.download_media(message.audio, file_name=file_name, progress=lambda c, t: progress(c, t, "Downloading"))
                client.send_audio(channel_target, file_name, caption=caption_text, progress=lambda c, t: progress(c, t, "Uploading"))

            if 3 in choices and message.video:
                file_size = message.video.file_size
                bar = tqdm(total=file_size, desc="Downloading", leave=False)
                file_name = get_cleaned_file_path(message.video, video_path)
                client.download_media(message.video, file_name=file_name, progress=lambda c, t: progress(c, t, "Downloading"))
                bar = tqdm(total=file_size, desc="Uploading ...", leave=False)
                send_video_with_metadata(
                    client,
                    channel_target,
                    message,
                    file_name,
                    caption_text,
                    lambda c, t: progress(c, t, "Uploading"),
                )

            if 4 in choices and message.document:
                file_size = message.document.file_size
                bar = tqdm(total=file_size, desc="Downloading", leave=False)
                file_name = get_cleaned_file_path(message.document, video_path)
                client.download_media(message.document, file_name=file_name, progress=progress)
                client.send_document(channel_target, file_name, caption=caption_text, progress=progress)

            if 5 in choices and message.text:
                text_with_links = (message.text + ' ' + extract_links_from_buttons(message.reply_markup)).strip()
                client.send_message(channel_target, text_with_links)

            if 6 in choices and message.sticker:
                file_name = get_cleaned_file_path(message.sticker, video_path)
                client.download_media(message.sticker, file_name=file_name)
                client.send_sticker(channel_target, file_name)

            if 7 in choices and message.animation:
                file_name = get_cleaned_file_path(message.animation, video_path)
                client.download_media(message.animation, file_name=file_name)
                client.send_animation(channel_target, file_name)
            if file_name:                            
                last_processed_id = message.id
                with open(json_filepath, "w") as json_file:
                    json.dump({"last_processed_id": last_processed_id}, json_file)
                    os.system('clear || cls')
                    print(f"Detalhes da mensagem {message.id} adicionados à lista e mídia / arquivo enviada ao canal de destino.")        
                os.remove(file_name)
            # Intervalo de 10s para evitar abuso da API do Telegram
            time.sleep(10)
            i += 1
        print("Tarefa concluida e log salvo no arquivo JSON.")

if __name__ == "__main__":
    show_banner()
    cache_path()
    authenticate()
    channel_source, channel_target, chat_title = get_channels()
    choices = get_user_choices()
    download_and_upload_media_from_channel(choices, channel_source, channel_target, chat_title)
