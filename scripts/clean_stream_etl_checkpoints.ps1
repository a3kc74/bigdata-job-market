param(
    [ValidateSet("auto", "local", "docker")]
    [string] $RunMode = $(if ($env:SPARK_RUN_MODE) { $env:SPARK_RUN_MODE } else { "auto" })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$ComposeFile = if ($env:COMPOSE_FILE) { $env:COMPOSE_FILE } else { "infra/docker-compose/docker-compose.speed.yml" }
$SparkService = if ($env:SPARK_SERVICE) { $env:SPARK_SERVICE } else { "spark-master" }

function Clean-DockerCheckpoint {
    $CheckpointDir = if ($env:CHECKPOINT_DIR -and $env:CHECKPOINT_DIR.StartsWith("/")) {
        $env:CHECKPOINT_DIR
    } else {
        "/checkpoints/speed"
    }

    if (-not $CheckpointDir.StartsWith("/checkpoints/")) {
        throw "Refusing to clean Docker checkpoint path outside /checkpoints: $CheckpointDir"
    }

    Write-Host "Cleaning Docker stream_etl checkpoints from ${SparkService}:${CheckpointDir}"

    docker compose -f $ComposeFile exec -T -u root $SparkService sh -lc `
        "if [ -d '$CheckpointDir' ]; then rm -rf '$CheckpointDir'/*; fi && mkdir -p '$CheckpointDir' && chmod -R 777 '$CheckpointDir'"

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to clean Docker stream_etl checkpoints from ${SparkService}:${CheckpointDir}"
    }
}

function Clean-LocalCheckpoint {
    $CheckpointDir = if ($env:CHECKPOINT_DIR -and -not $env:CHECKPOINT_DIR.StartsWith("/")) {
        $env:CHECKPOINT_DIR
    } else {
        Join-Path $ProjectRoot.Path ".checkpoints\speed"
    }

    $ResolvedProjectRoot = (Resolve-Path $ProjectRoot.Path).Path
    New-Item -ItemType Directory -Force -Path $CheckpointDir | Out-Null
    $ResolvedCheckpointDir = (Resolve-Path $CheckpointDir).Path

    if (-not $ResolvedCheckpointDir.StartsWith($ResolvedProjectRoot)) {
        throw "Refusing to clean local checkpoint path outside workspace: $ResolvedCheckpointDir"
    }

    Write-Host "Cleaning local stream_etl checkpoints from ${ResolvedCheckpointDir}"
    Get-ChildItem -LiteralPath $ResolvedCheckpointDir -Force | Remove-Item -Recurse -Force
}

if ($RunMode -eq "docker") {
    Clean-DockerCheckpoint
} elseif ($RunMode -eq "local") {
    Clean-LocalCheckpoint
} else {
    Clean-LocalCheckpoint
    Clean-DockerCheckpoint
}

Write-Host "stream_etl checkpoints cleaned."
