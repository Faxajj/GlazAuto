# Скрипт упаковки Banks Dashboard для загрузки на сервер.
# Запуск: в PowerShell из папки banks-dashboard: .\pack_for_deploy.ps1
# Создаёт архив deploy-banks-dashboard.zip без лишнего (без accounts.db, __pycache__, .git).

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$zipName = "deploy-banks-dashboard.zip"
$zipPath = Join-Path $root $zipName

if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

$toPack = @(
    "requirements.txt",
    "run_server.sh",
    "README.md",
    "DEPLOY.md",
    "FILES_FOR_DEPLOY.txt",
    "app\__init__.py",
    "app\main.py",
    "app\database.py",
    "app\static\style.css",
    "app\templates\base.html",
    "app\templates\index.html",
    "app\templates\add_account.html",
    "app\templates\edit_account.html",
    "app\templates\receipt.html",
    "app\drivers\__init__.py",
    "app\drivers\personalpay.py",
    "app\drivers\universalcoins.py",
    "docs\PERSONALPAY_403_FIX.md",
    "scripts\reset_and_add_pp.py",
    "VPS_SETUP_glazauto.md",
    "СЕРВЕР_И_ДОМЕН.md"
)

Push-Location $root
try {
    Compress-Archive -Path $toPack -DestinationPath $zipName -Force
    Write-Host "Done: $zipPath"
} finally {
    Pop-Location
}
