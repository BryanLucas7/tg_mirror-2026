@echo off
if exist "..\.venv\Scripts\activate.bat" (
    call "..\.venv\Scripts\activate.bat"
) else (
    echo Ambiente virtual ..\.venv nao encontrado. Usando o Python do sistema.
)
python tg_mirror.py
cmd /k
pause
