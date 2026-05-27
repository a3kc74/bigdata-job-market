Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$UvCacheDir = if ($env:UV_CACHE_DIR_HOME) {
    $env:UV_CACHE_DIR_HOME
} else {
    Join-Path $HOME ".cache\uv"
}

New-Item -ItemType Directory -Force -Path $UvCacheDir | Out-Null
[Environment]::SetEnvironmentVariable("UV_CACHE_DIR", $UvCacheDir, "User")
$env:UV_CACHE_DIR = $UvCacheDir

Write-Host "UV_CACHE_DIR configured for current process and User environment:"
Write-Host $UvCacheDir
