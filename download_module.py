import os
import time
import json
import shutil
import asyncio
from tqdm import tqdm
import re
from utils import (
    limpar_nome_arquivo,
    Banner,
    show_banner,
    cache_path,
    authenticate,
    rename_files,
    acquire_available_session,
    acquire_runtime_lock,
    release_runtime_lock,
    build_run_id,
    create_run_download_dir,
    cleanup_run_download_dir,
    build_lock_name,
    safe_download_media,
    refresh_message_reference,
)

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client
import pyrogram

""" Global """
session_name = "user"
video_path = 'downloads' 
task_directory = 'chat_download_task'
runtime_lock_path = None
task_lock_path = None
run_download_path = None

def limpar_nome_arquivo(nome_arquivo):
    nome_limpo = re.sub(r'[^a-zA-Z0-9]', '_', nome_arquivo)
    chars_invalidos = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in chars_invalidos:
        nome_limpo = nome_limpo.replace(char, '_')
    return nome_limpo

def get_cleaned_file_path(media, directory, chat_title, caption=None):
    if caption:
        base_name = limpar_nome_arquivo(caption)
    else:
        base_name = media.file_name or f"{media.file_id}"

    extension = media.file_name.split('.')[-1] if media.file_name and '.' in media.file_name else 'unknown'
    clean_name = f"{base_name}.{extension}"
    
    # Aqui nós adicionamos o nome do canal ao caminho do diretório
    chat_directory = os.path.join(directory, limpar_nome_arquivo(chat_title))
    
    return os.path.join(chat_directory, clean_name)

def get_channel():
    with Client(session_name) as client:
        channel_source = input("Forneça o @username ou ID do canal / grupo que deseja baixar os conteúdos ")
        channel_source = parse_channel_input(channel_source)
        chat_info = client.get_chat(channel_source)
        return channel_source, chat_info.title

def parse_channel_input(channel_input: str):
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
    options = ["Processar todos os Conteúdos", "Fotos", "Áudios", "Vídeos", "Arquivos"]
    for i, option in enumerate(options):
        print(f"{i} - {option}")
    choices = input("\nInforme os conteúdos que deseja procesar separados por vírgula (ex: 1,3) < 0 para processar todos : ").split(',')
    choices = [int(choice.strip()) for choice in choices]
    if 0 in choices:
        choices = [1, 2, 3, 4]
    return choices

def download_progress(current, total):
    bar.update(current - bar.n)

def save_last_processed_message_id(chat_title, channel_source, last_id):
    if not os.path.exists(task_directory):
        os.makedirs(task_directory)
    with open(f"{task_directory}/{session_name}_{chat_title}_{channel_source}.json", 'w') as file:
        json.dump({'last_processed_id': last_id}, file)

def load_last_processed_message_id(chat_title, channel_source):
    json_filepath = f"{task_directory}/{session_name}_{chat_title}_{channel_source}.json"
    try:
        with open(json_filepath, "r") as json_file:
            data = json.load(json_file)
            last_processed_id = data["last_processed_id"] if "last_processed_id" in data else 0
            print(f"Retomando do ID da próxima mensagem após a última processada: {last_processed_id + 1}")
            return last_processed_id
    except FileNotFoundError:
        return 0

def download_media_from_channel(choices, channel_source, chat_title):
    with Client(session_name) as client:
        chat_directory = os.path.join(video_path, limpar_nome_arquivo(chat_title))
        if not os.path.exists(chat_directory):
            os.makedirs(chat_directory)
        last_id = load_last_processed_message_id(chat_title, channel_source)
        all_messages = list(client.get_chat_history(channel_source))
        all_messages.reverse()

        for count, message in enumerate(all_messages):
            message = refresh_message_reference(client, channel_source, message)
            file_name = None
            global bar

            if 1 in choices and message.photo:
                bar = tqdm(total=message.photo.file_size, desc="Downloading Photo", leave=False)
                file_name = safe_download_media(client, message.photo, progress=download_progress)
                bar.close()
               
                if file_name:
                    destination_directory = os.path.join(video_path, limpar_nome_arquivo(chat_title))
                    destination_path = os.path.join(destination_directory, os.path.basename(file_name))
                    os.makedirs(destination_directory, exist_ok=True)
                    shutil.move(file_name, destination_path)
            
            if 2 in choices and message.audio:                
                bar = tqdm(total=message.audio.file_size, desc="Downloading Audio", leave=False)
                file_name = safe_download_media(client, message.audio, progress=download_progress)
                bar.close()

                if file_name:
                    destination_directory = os.path.join(video_path, limpar_nome_arquivo(chat_title))
                    destination_path = os.path.join(destination_directory, os.path.basename(file_name))
                    os.makedirs(destination_directory, exist_ok=True)
                    shutil.move(file_name, destination_path)

            if 3 in choices and message.video:
                bar = tqdm(total=message.video.file_size, desc="Downloading Video", leave=False)
                file_name = get_cleaned_file_path(message.video, video_path, chat_title, message.caption)  # Aqui especificamos chat_title
                safe_download_media(client, message.video, file_name=file_name, progress=download_progress)
                bar.close()

            if 4 in choices and message.document:
                bar = tqdm(total=message.document.file_size, desc="Downloading Document", leave=False)
                file_name = get_cleaned_file_path(message.document, video_path, chat_title)
                safe_download_media(client, message.document, file_name=file_name, progress=download_progress)
                bar.close()

            if file_name:
                os.system('clear || cls')
                print(f"\nDetalhes da mensagem {message.id} baixados e salvos em {file_name}.")
                save_last_processed_message_id(chat_title, channel_source, message.id)
            time.sleep(10)
            
        print("Tarefa concluída.")

if __name__ == "__main__":
    try:
        show_banner()
        cache_path()
        session_name, runtime_lock_path = acquire_available_session(lock_prefix="session")
        authenticate(session_name)
        run_id = build_run_id("download")
        run_download_path = create_run_download_dir(run_id)
        video_path = run_download_path
        channel_source, chat_title = get_channel()
        task_lock_path = acquire_runtime_lock(build_lock_name("task_download", channel_source, chat_title))
        choices = get_user_choices()
        download_media_from_channel(choices, channel_source, chat_title)    
        rename_files(video_path, chat_title)
    finally:
        cleanup_run_download_dir(run_download_path)
        release_runtime_lock(task_lock_path)
        release_runtime_lock(runtime_lock_path)
