import os
import re
import asyncio
import json
import shutil
import time
from colorama import Fore
import pyfiglet
import random

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client

def limpar_nome_arquivo(nome_arquivo):
    nome_limpo = re.sub(r'[^a-zA-Z0-9]', '_', nome_arquivo)
    chars_invalidos = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in chars_invalidos:
        nome_limpo = nome_limpo.replace(char, '_')
    return nome_limpo

class Banner:
    def __init__(self, banner):
        self.banner = banner
        self.lg = Fore.LIGHTGREEN_EX
        self.w = Fore.WHITE
        self.cy = Fore.CYAN
        self.ye = Fore.YELLOW
        self.r = Fore.RED
        self.n = Fore.RESET

    def print_banner(self):
        colors = [self.lg, self.r, self.w, self.cy, self.ye]
        f = pyfiglet.Figlet(font='slant')
        banner = f.renderText(self.banner)
        print(f'{random.choice(colors)}{banner}{self.n}')
        print(f'{self.r}  Version: v0.0.2 https://github.com/viniped \n{self.n}')

def show_banner():
    banner = Banner('TG - Mirror')
    banner.print_banner()

def cache_path():
    directories = ['downloads', 'download_tasks','forward_task','chat_download_task', 'runtime_locks']

    for dir_name in directories:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

def ensure_directory(path):
    os.makedirs(path, exist_ok=True)
    return path

def build_run_id(prefix="run"):
    return f"{prefix}_{time.strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"

def _is_process_running(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True

def acquire_runtime_lock(lock_name):
    ensure_directory("runtime_locks")
    lock_path = os.path.join("runtime_locks", f"{lock_name}.lock")
    payload = {
        "pid": os.getpid(),
        "created_at": int(time.time()),
    }

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(payload, file)
            return lock_path
        except FileExistsError:
            try:
                with open(lock_path, "r", encoding="utf-8") as file:
                    current = json.load(file)
            except Exception:
                current = {}

            if not _is_process_running(current.get("pid")):
                try:
                    os.remove(lock_path)
                    continue
                except OSError:
                    pass
            raise RuntimeError(
                "Ja existe outro processo de envio em execucao para esta sessao. "
                "Feche o outro terminal ou aguarde ele terminar."
            )

def release_runtime_lock(lock_path):
    if lock_path and os.path.exists(lock_path):
        try:
            os.remove(lock_path)
        except OSError:
            pass

def create_run_download_dir(run_id):
    return ensure_directory(os.path.join("downloads", "runs", run_id))

def cleanup_run_download_dir(path):
    if not path or not os.path.exists(path):
        return
    try:
        shutil.rmtree(path, ignore_errors=True)
    except OSError:
        pass

def _list_existing_session_names():
    names = []
    for filename in os.listdir("."):
        if filename.endswith(".session"):
            names.append(filename[:-8])
    return sorted(set(names))

def _next_session_name(existing_names, default="user"):
    if default not in existing_names:
        return default

    index = 2
    while f"{default}_{index}" in existing_names:
        index += 1
    return f"{default}_{index}"

def build_lock_name(prefix, *parts):
    cleaned_parts = [limpar_nome_arquivo(str(part)) for part in parts if part is not None]
    return "_".join([prefix, *cleaned_parts])

def acquire_available_session(default="user", lock_prefix="session_forward"):
    existing_names = _list_existing_session_names()
    candidates = existing_names + [_next_session_name(existing_names, default)]

    for candidate in candidates:
        try:
            lock_path = acquire_runtime_lock(build_lock_name(lock_prefix, candidate))
            return candidate, lock_path
        except RuntimeError:
            continue

    raise RuntimeError("Nao foi possivel reservar uma sessao livre para esta execucao.")

def authenticate(session_name):
    # Get credentialas from user
    def get_credentials():
        api_id = input("Digite seu API ID: ")
        api_hash = input("Digite seu API Hash: ")
        return api_id, api_hash

    # if session file does not exists, obtain the credentials 
    if not os.path.exists(f"{session_name}.session"):
        api_id, api_hash = get_credentials()
        with Client(session_name, api_id, api_hash) as app:
            print("Você está autenticado!")

def rename_files(directory, chat_title):
    
    chat_directory = os.path.join(directory, limpar_nome_arquivo(chat_title))
    files = [f for f in os.listdir(chat_directory) if os.path.isfile(os.path.join(chat_directory, f))]      
    files.sort(key=lambda x: os.path.getctime(os.path.join(chat_directory, x)))    
    for idx, filename in enumerate(files, start=1):
        # Substituir underscores por espaços
        cleaned_name = filename.replace("_", " ")
        new_name = f"{idx:03}_{cleaned_name}"
        os.rename(os.path.join(chat_directory, filename), os.path.join(chat_directory, new_name))
