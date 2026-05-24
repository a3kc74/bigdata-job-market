$ErrorActionPreference = "Stop"

$ComposeFile = "infra/docker-compose/docker-compose.speed.yml"

function Require-ServiceRunning {
    param([string]$Service)

    $running = docker compose -f $ComposeFile ps --status running --services
    if ($running -notcontains $Service) {
        throw "Service is not running: $Service"
    }

    Write-Host "[OK] Service running: $Service"
}

Write-Host "Checking core speed services..."

$coreServices = @(
    "zookeeper",
    "kafka",
    "spark-master",
    "spark-worker",
    "elasticsearch"
)

foreach ($service in $coreServices) {
    Require-ServiceRunning $service
}

Write-Host "Checking Kafka topics..."

$topics = docker compose -f $ComposeFile exec -T kafka kafka-topics `
    --bootstrap-server kafka:9092 `
    --list

$requiredTopics = @(
    "jobs_raw",
    "jobs_clean",
    "jobs_dead_letter"
)

foreach ($topic in $requiredTopics) {
    if ($topics -notcontains $topic) {
        throw "Kafka topic does not exist: $topic"
    }

    Write-Host "[OK] Kafka topic exists: $topic"
}

Write-Host "Checking Elasticsearch..."

$es = Invoke-RestMethod -Uri "http://localhost:9200" -Method Get
if (-not $es.cluster_name) {
    throw "Elasticsearch did not return cluster info"
}

Write-Host "[OK] Elasticsearch is reachable: $($es.cluster_name)"

Write-Host "Checking Spark master UI..."

$spark = Invoke-WebRequest -Uri "http://localhost:8080" -UseBasicParsing
if ($spark.StatusCode -ne 200) {
    throw "Spark master UI is not reachable"
}

Write-Host "[OK] Spark master UI is reachable"

Write-Host ""
Write-Host "ES-only speed smoke test passed."
