@echo off
setlocal
if exist "..\.venv\Scripts\activate.bat" (
    call "..\.venv\Scripts\activate.bat"
) else (
    echo Ambiente virtual ..\.venv nao encontrado. Usando o Python do sistema.
)
python foward_module.py
set "exit_code=%ERRORLEVEL%"
echo.
if not "%exit_code%"=="0" echo Execucao encerrada com codigo %exit_code%.
pause
exit /b %exit_code%
