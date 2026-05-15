Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ComposeFile = if ($env:COMPOSE_FILE) { $env:COMPOSE_FILE } else { "infra/docker-compose/docker-compose.speed.yml" }
$BootstrapServer = if ($env:BOOTSTRAP_SERVER) { $env:BOOTSTRAP_SERVER } else { "kafka:29092" }
$Retention7dMs = if ($env:RETENTION_7D_MS) { $env:RETENTION_7D_MS } else { "604800000" }
$Retention14dMs = if ($env:RETENTION_14D_MS) { $env:RETENTION_14D_MS } else { "1209600000" }

function New-KafkaTopic {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Topic,

        [Parameter(Mandatory = $true)]
        [int] $Partitions,

        [Parameter(Mandatory = $true)]
        [string] $RetentionMs
    )

    docker compose -f $ComposeFile exec -T kafka kafka-topics `
        --bootstrap-server $BootstrapServer `
        --create `
        --if-not-exists `
        --topic $Topic `
        --partitions $Partitions `
        --replication-factor 1 `
        --config "retention.ms=$RetentionMs"
}

New-KafkaTopic -Topic "jobs_raw" -Partitions 3 -RetentionMs $Retention7dMs
New-KafkaTopic -Topic "jobs_clean" -Partitions 3 -RetentionMs $Retention7dMs
New-KafkaTopic -Topic "jobs_dead_letter" -Partitions 1 -RetentionMs $Retention14dMs

docker compose -f $ComposeFile exec -T kafka kafka-topics `
    --bootstrap-server $BootstrapServer `
    --list
