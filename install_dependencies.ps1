$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

$PythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $PythonCmd) {
    throw "Python não encontrado no PATH."
}

Write-Host "Instalando dependências Python..." -ForegroundColor Cyan
python -m pip install -r requirements.txt --upgrade

$FfmpegBin = Join-Path $ProjectRoot "tools\ffmpeg\bin"
$FfmpegExe = Join-Path $FfmpegBin "ffmpeg.exe"
$FfprobeExe = Join-Path $FfmpegBin "ffprobe.exe"

if ((Test-Path $FfmpegExe) -and (Test-Path $FfprobeExe)) {
    Write-Host "ffmpeg portátil já está presente em tools\\ffmpeg\\bin." -ForegroundColor Green
    exit 0
}

$TempDir = Join-Path $env:TEMP ("tg_mirror_ffmpeg_" + [guid]::NewGuid().ToString("N"))
$ZipPath = Join-Path $TempDir "ffmpeg.zip"
$ExtractDir = Join-Path $TempDir "extract"

New-Item -ItemType Directory -Force -Path $TempDir | Out-Null
New-Item -ItemType Directory -Force -Path $ExtractDir | Out-Null
New-Item -ItemType Directory -Force -Path $FfmpegBin | Out-Null

$DownloadUrl = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"

Write-Host "Baixando ffmpeg portátil..." -ForegroundColor Cyan
Invoke-WebRequest -Uri $DownloadUrl -OutFile $ZipPath

Write-Host "Extraindo ffmpeg portátil..." -ForegroundColor Cyan
Expand-Archive -Path $ZipPath -DestinationPath $ExtractDir -Force

$ExtractedBin = Get-ChildItem -Path $ExtractDir -Directory | ForEach-Object {
    Join-Path $_.FullName "bin"
} | Where-Object { Test-Path (Join-Path $_ "ffmpeg.exe") } | Select-Object -First 1

if (-not $ExtractedBin) {
    throw "Não foi possível localizar ffmpeg.exe dentro do zip baixado."
}

Copy-Item (Join-Path $ExtractedBin "ffmpeg.exe") $FfmpegExe -Force
Copy-Item (Join-Path $ExtractedBin "ffprobe.exe") $FfprobeExe -Force

Remove-Item -LiteralPath $TempDir -Recurse -Force

Write-Host "Dependências instaladas com sucesso." -ForegroundColor Green
