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

$InputPath = if ($env:FAKE_CRAWLER_INPUT) { $env:FAKE_CRAWLER_INPUT } else { "data/raw/raw_jobs_batch.jsonl" }
$Topic = if ($env:RAW_TOPIC) { $env:RAW_TOPIC } else { "jobs_raw" }
$BootstrapServers = if ($env:KAFKA_BOOTSTRAP_SERVERS) { $env:KAFKA_BOOTSTRAP_SERVERS } else { "localhost:9092" }
$SleepMs = if ($env:FAKE_CRAWLER_SLEEP_MS) { $env:FAKE_CRAWLER_SLEEP_MS } else { "1000" }
$EventTimeMode = if ($env:FAKE_CRAWLER_EVENT_TIME_MODE) { $env:FAKE_CRAWLER_EVENT_TIME_MODE } else { "now" }
$MaxRecords = if ($env:FAKE_CRAWLER_MAX_RECORDS) { $env:FAKE_CRAWLER_MAX_RECORDS } else { "0" }
$LogEvery = if ($env:FAKE_CRAWLER_LOG_EVERY) { $env:FAKE_CRAWLER_LOG_EVERY } else { "10" }

$ArgsList = @(
    "run",
    "--project", $ProjectRoot.Path,
    "python",
    "apps/producer/fake_crawler_producer.py",
    "--input", $InputPath,
    "--topic", $Topic,
    "--bootstrap-servers", $BootstrapServers,
    "--sleep-ms", $SleepMs,
    "--event-time-mode", $EventTimeMode,
    "--max-records", $MaxRecords,
    "--log-every", $LogEvery
)

if ($env:FAKE_CRAWLER_LOOP -match "^(1|true|yes)$") {
    $ArgsList += "--loop"
}

if ($RemainingArgs) {
    $ArgsList += $RemainingArgs
}

& $UvBin @ArgsList
