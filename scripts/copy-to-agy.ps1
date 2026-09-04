param (
    [switch]$Build
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not $repoRoot) { $repoRoot = (Get-Location).Path }

Write-Host "==> Finding 'agy' executable..." -ForegroundColor Cyan

$agyPath = 
$agyCmd = Get-Command agy -ErrorAction SilentlyContinue
if ($agyCmd) {
    $agyPath = $agyCmd.Source
} else {
    $whereOutput = where.exe agy 2>$null | Select-Object -First 1
    if ($whereOutput) {
        $agyPath = $whereOutput
    }
}

if (-not $agyPath) {
    $defaultPath = Join-Path $env:LOCALAPPDATA 'agy\bin\agy.exe'
    if (Test-Path $defaultPath) {
        $agyPath = $defaultPath
    }
}

if (-not $agyPath) {
    Write-Error "Could not find 'agy' executable in PATH or default location. Please ensure agy is installed."
    exit 1
}

$targetDir = Split-Path $agyPath -Parent
Write-Host "    Found agy at: $agyPath" -ForegroundColor Gray
Write-Host "    Target destination folder: $targetDir" -ForegroundColor Gray

$releaseExe = Join-Path $repoRoot 'target\release\agy-gyro.exe'

if ($Build -or (-not (Test-Path $releaseExe))) {
    Write-Host "==> Building release binary (cargo build --release)..." -ForegroundColor Cyan
    Push-Location $repoRoot
    try {
        & cargo build --release
        if ($LASTEXITCODE -ne 0) {
            Write-Error "cargo build --release failed with exit code $LASTEXITCODE"
            exit $LASTEXITCODE
        }
    } finally {
        Pop-Location
    }
}

Write-Host "==> Copying $releaseExe to $targetDir..." -ForegroundColor Cyan
Copy-Item $releaseExe -Destination $targetDir -Force

$destExe = Join-Path $targetDir 'agy-gyro.exe'
if (Test-Path $destExe) {
    Write-Host "SUCCESS: Installed agy-gyro to: $destExe" -ForegroundColor Green
    & $destExe --version
} else {
    Write-Error "Failed to copy agy-gyro.exe to $targetDir"
    exit 1
}
