Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ComposeFile = if ($env:COMPOSE_FILE) { $env:COMPOSE_FILE } else { "infra/docker-compose/docker-compose.speed.yml" }
$BootstrapServer = if ($env:BOOTSTRAP_SERVER) { $env:BOOTSTRAP_SERVER } else { "kafka:29092" }

function Assert-LastExitCode {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Message
    )

    if ($LASTEXITCODE -ne 0) {
        throw $Message
    }
}

function Require-Service {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Service
    )

    $runningServices = docker compose -f $ComposeFile ps --status running --services
    Assert-LastExitCode "Failed to list running Docker Compose services"

    if ($runningServices -notcontains $Service) {
        throw "Service is not running: $Service"
    }
}

function Require-Topic {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Topic
    )

    docker compose -f $ComposeFile exec -T kafka kafka-topics `
        --bootstrap-server $BootstrapServer `
        --describe `
        --topic $Topic *> $null
    Assert-LastExitCode "Kafka topic is missing or unavailable: $Topic"
}

function Require-CassandraTable {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Table
    )

    docker compose -f $ComposeFile exec -T cassandra cqlsh `
        -e "DESCRIBE TABLE job_market_speed.$Table;" *> $null
    Assert-LastExitCode "Cassandra table is missing or unavailable: job_market_speed.$Table"
}

$services = @(
    "kafka",
    "kafka-ui",
    "spark-master",
    "spark-worker",
    "elasticsearch",
    "kibana",
    "cassandra",
    "prometheus",
    "grafana"
)

foreach ($service in $services) {
    Require-Service $service
}

$topics = @("jobs_raw", "jobs_clean", "jobs_dead_letter")
foreach ($topic in $topics) {
    Require-Topic $topic
}

$tables = @(
    "realtime_job_counts_10m",
    "realtime_skill_counts_hourly",
    "realtime_top_skills_hourly",
    "realtime_salary_bins_hourly",
    "jobs_realtime_by_id",
    "stream_dead_letter_by_day"
)

foreach ($table in $tables) {
    Require-CassandraTable $table
}

Invoke-WebRequest -Uri "http://localhost:9200" -UseBasicParsing | Out-Null
Invoke-WebRequest -Uri "http://localhost:8088" -UseBasicParsing | Out-Null
Invoke-WebRequest -Uri "http://localhost:9090/-/ready" -UseBasicParsing | Out-Null

Write-Host "Speed Layer phase 1 smoke test passed."
