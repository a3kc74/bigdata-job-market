param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $RemainingArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$UvBin = if ($env:UV_BIN) { $env:UV_BIN } else { "uv" }
$UvCacheDir = if ($env:UV_CACHE_DIR_HOME) { $env:UV_CACHE_DIR_HOME } else { Join-Path $HOME ".cache\uv" }
$env:UV_CACHE_DIR = $UvCacheDir

$SparkPackages = if ($env:SPARK_PACKAGES) {
    $env:SPARK_PACKAGES
} else {
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
}

$ArgsList = @(
    "run",
    "--project", $ProjectRoot.Path,
    "spark-submit",
    "--packages", $SparkPackages,
    "apps/stream_etl/stream_main.py"
)

if ($RemainingArgs) {
    $ArgsList += $RemainingArgs
}

& $UvBin @ArgsList
