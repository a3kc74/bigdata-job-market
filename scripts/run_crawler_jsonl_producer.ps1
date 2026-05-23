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

$InputPath = if ($env:CRAWLER_JSONL_INPUT) { $env:CRAWLER_JSONL_INPUT } else { "data/raw/jobs/source=topcv/ingest_date=*/jobs_speed_*.jsonl" }
$Topic = if ($env:RAW_TOPIC) { $env:RAW_TOPIC } else { "jobs_raw" }
$BootstrapServers = if ($env:KAFKA_BOOTSTRAP_SERVERS) { $env:KAFKA_BOOTSTRAP_SERVERS } else { "localhost:9092" }
$SleepMs = if ($env:CRAWLER_JSONL_SLEEP_MS) { $env:CRAWLER_JSONL_SLEEP_MS } else { "0" }
$EventTimeMode = if ($env:CRAWLER_JSONL_EVENT_TIME_MODE) { $env:CRAWLER_JSONL_EVENT_TIME_MODE } else { "original" }
$MaxRecords = if ($env:CRAWLER_JSONL_MAX_RECORDS) { $env:CRAWLER_JSONL_MAX_RECORDS } else { "0" }
$LogEvery = if ($env:CRAWLER_JSONL_LOG_EVERY) { $env:CRAWLER_JSONL_LOG_EVERY } else { "10" }
$CheckpointFile = if ($env:CRAWLER_JSONL_CHECKPOINT_FILE) { $env:CRAWLER_JSONL_CHECKPOINT_FILE } else { "runtime/producer/crawler_jsonl_offsets.json" }
$PollSeconds = if ($env:CRAWLER_JSONL_POLL_SECONDS) { $env:CRAWLER_JSONL_POLL_SECONDS } else { "2" }
$Watch = if ($env:CRAWLER_JSONL_WATCH) { $env:CRAWLER_JSONL_WATCH } else { "true" }

$ArgsList = @(
    "run",
    "--project", $ProjectRoot.Path,
    "python",
    "-m", "apps.producer.crawler_jsonl_producer",
    "--input", $InputPath,
    "--topic", $Topic,
    "--bootstrap-servers", $BootstrapServers,
    "--sleep-ms", $SleepMs,
    "--event-time-mode", $EventTimeMode,
    "--max-records", $MaxRecords,
    "--log-every", $LogEvery,
    "--checkpoint-file", $CheckpointFile,
    "--poll-seconds", $PollSeconds
)

if ($Watch -match "^(1|true|yes)$") {
    $ArgsList += "--watch"
}

if ($RemainingArgs) {
    $ArgsList += $RemainingArgs
}

& $UvBin @ArgsList
