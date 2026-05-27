param(
    [ValidateSet("speed", "batch")]
    [string] $Mode = $(if ($env:CRAWLER_MODE) { $env:CRAWLER_MODE } else { "speed" }),
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $RemainingArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$UvBin = if ($env:UV_BIN) { $env:UV_BIN } else { "uv" }
$UvCacheDir = if ($env:UV_CACHE_DIR_HOME) { $env:UV_CACHE_DIR_HOME } else { Join-Path $HOME ".cache\uv" }
$env:UV_CACHE_DIR = $UvCacheDir

$ArgsList = @(
    "run",
    "--project", $ProjectRoot.Path,
    "python",
    "-m", "apps.ingestion.run_crawler",
    "--mode", $Mode
)

if ($env:CRAWLER_MAX_PAGES) {
    $ArgsList += @("--max-pages", $env:CRAWLER_MAX_PAGES)
}
if ($env:CRAWLER_UPDATED_WITHIN_MINUTES) {
    $ArgsList += @("--updated-within-minutes", $env:CRAWLER_UPDATED_WITHIN_MINUTES)
}
if ($env:CRAWLER_DETAIL_BATCH_SIZE) {
    $ArgsList += @("--detail-batch-size", $env:CRAWLER_DETAIL_BATCH_SIZE)
}
if ($env:CRAWLER_LIST_PAGES_PER_CHUNK) {
    $ArgsList += @("--list-pages-per-chunk", $env:CRAWLER_LIST_PAGES_PER_CHUNK)
}
if ($env:CRAWLER_PROCESSED_TTL_DAYS) {
    $ArgsList += @("--processed-ttl-days", $env:CRAWLER_PROCESSED_TTL_DAYS)
}
if ($env:CRAWLER_DEBUG_CARD_LINKS -match "^(1|true|yes)$") {
    $ArgsList += "--debug-card-links"
}
if ($RemainingArgs) {
    $ArgsList += $RemainingArgs
}

& $UvBin @ArgsList

