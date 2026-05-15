Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ComposeFile = if ($env:COMPOSE_FILE) { $env:COMPOSE_FILE } else { "infra/docker-compose/docker-compose.speed.yml" }
$CqlFile = if ($env:CQL_FILE) { $env:CQL_FILE } else { "scripts/init_cassandra.cql" }
$ContainerCqlFile = "/docker-entrypoint-initdb.d/init_cassandra.cql"

if (-not (Test-Path -LiteralPath $CqlFile)) {
    throw "CQL file not found: $CqlFile"
}

docker compose -f $ComposeFile exec -T cassandra cqlsh -f $ContainerCqlFile
if ($LASTEXITCODE -ne 0) {
    throw "Failed to apply Cassandra schema from $CqlFile"
}

docker compose -f $ComposeFile exec -T cassandra cqlsh `
    -e "DESCRIBE KEYSPACE job_market_speed;"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to describe Cassandra keyspace job_market_speed"
}
