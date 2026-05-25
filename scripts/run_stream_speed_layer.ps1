param(
    [ValidateSet("auto", "local", "docker")]
    [string] $RunMode = $(if ($env:SPARK_RUN_MODE) { $env:SPARK_RUN_MODE } else { "auto" }),

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $RemainingArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$UvBin = if ($env:UV_BIN) { $env:UV_BIN } else { "uv" }
$ComposeFile = if ($env:COMPOSE_FILE) { $env:COMPOSE_FILE } else { "infra/docker-compose/docker-compose.speed.yml" }
$SparkService = if ($env:SPARK_SERVICE) { $env:SPARK_SERVICE } else { "spark-master" }

function New-FirstWritableDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Candidates
    )

    foreach ($Candidate in $Candidates) {
        try {
            New-Item -ItemType Directory -Force -Path $Candidate | Out-Null
            return (Resolve-Path $Candidate).Path
        } catch {
            continue
        }
    }

    throw "Cannot create any writable directory from: $($Candidates -join ', ')"
}

function Get-WindowsShortPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    $ShortPath = & cmd /c "for %I in (`"$Path`") do @echo %~sI"
    if ($LASTEXITCODE -eq 0 -and $ShortPath -and -not ($ShortPath -match "\s")) {
        return $ShortPath.Trim()
    }

    return $Path
}

function Test-HadoopHomeReady {
    if (-not $env:HADOOP_HOME) {
        return $false
    }

    if (-not [System.IO.Path]::IsPathRooted($env:HADOOP_HOME)) {
        return $false
    }

    try {
        $HadoopHome = (Resolve-Path $env:HADOOP_HOME -ErrorAction Stop).Path
    } catch {
        return $false
    }

    return (Test-Path (Join-Path $HadoopHome "bin\winutils.exe"))
}

function Resolve-HadoopHome {
    if (-not (Test-HadoopHomeReady)) {
        return $null
    }

    return (Resolve-Path $env:HADOOP_HOME).Path
}

function Invoke-DockerSparkSubmit {
    param(
        [Parameter(Mandatory = $true)]
        [string] $SparkPackages,

        [string[]] $SparkArgs = @()
    )

    $KafkaBootstrapServers = if ($env:KAFKA_BOOTSTRAP_SERVERS) { $env:KAFKA_BOOTSTRAP_SERVERS } else { "kafka:29092" }
    $CassandraHost = if ($env:CASSANDRA_HOST) { $env:CASSANDRA_HOST } else { "cassandra" }
    $CassandraPort = if ($env:CASSANDRA_PORT) { $env:CASSANDRA_PORT } else { "9042" }
    $EsUrl = if ($env:ES_URL) { $env:ES_URL } else { "http://elasticsearch:9200" }
    $EnableSalaryPrediction = if ($env:ENABLE_SALARY_PREDICTION) { $env:ENABLE_SALARY_PREDICTION } else { "true" }
    $SalaryModelPath = if ($env:SALARY_MODEL_PATH) { $env:SALARY_MODEL_PATH } else { "hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest" }
    $CheckpointDir = if ($env:CHECKPOINT_DIR -and $env:CHECKPOINT_DIR.StartsWith("/")) {
        $env:CHECKPOINT_DIR
    } else {
        "/checkpoints/speed"
    }

    $dockerEnv = @(
        "export HOME='/tmp/spark-home'",
        "export PIP_CACHE_DIR='/tmp/spark-pip-cache'",
        "export KAFKA_BOOTSTRAP_SERVERS='$KafkaBootstrapServers'",
        "export CASSANDRA_HOST='$CassandraHost'",
        "export CASSANDRA_PORT='$CassandraPort'",
        "export ES_URL='$EsUrl'",
        "export ENABLE_SALARY_PREDICTION='$EnableSalaryPrediction'",
        "export SALARY_MODEL_PATH='$SalaryModelPath'",
        "export CHECKPOINT_DIR='$CheckpointDir'",
        "export PYTHONPATH='/tmp/spark-python-deps:/opt/spark/workspace'"
    ) -join "; "

    $quotedSparkArgs = if ($SparkArgs) {
        ($SparkArgs | ForEach-Object { "'$($_ -replace "'", "'\''")'" }) -join " "
    } else {
        ""
    }

    docker compose -f $ComposeFile exec -T -u root $SparkService sh -lc "mkdir -p '$CheckpointDir' && chmod -R 777 '$CheckpointDir'"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to prepare checkpoint directory in ${SparkService}:${CheckpointDir}"
    }

    $command = @(
        "cd /opt/spark/workspace",
        "mkdir -p /tmp/spark-home /tmp/spark-pip-cache /tmp/spark-ivy /tmp/spark-python-deps",
        "python3 -m pip install --upgrade --target /tmp/spark-python-deps -q python-dotenv cassandra-driver requests",
        $dockerEnv,
        "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --conf spark.executorEnv.PYTHONPATH=/opt/spark/workspace --packages '$SparkPackages' apps/stream_etl/stream_main.py $quotedSparkArgs"
    ) -join " && "

    docker compose -f $ComposeFile exec -T $SparkService bash -lc $command
    exit $LASTEXITCODE
}

$UvCacheDir = if ($env:UV_CACHE_DIR_HOME) {
    New-FirstWritableDirectory @($env:UV_CACHE_DIR_HOME)
} else {
    New-FirstWritableDirectory @(
        "C:\tmp\bigdata-job-market\uv-cache",
        (Join-Path $ProjectRoot.Path ".uv-cache")
    )
}
$env:UV_CACHE_DIR = $UvCacheDir

$SparkTempDir = if ($env:SPARK_WINDOWS_TEMP) {
    New-FirstWritableDirectory @($env:SPARK_WINDOWS_TEMP)
} else {
    New-FirstWritableDirectory @(
        "C:\tmp\bigdata-job-market\spark-temp",
        (Join-Path $ProjectRoot.Path ".tmp\spark-temp")
    )
}

# Spark's Windows .cmd launcher uses %TEMP% in unquoted batch statements.
# Use a no-space temp path when possible to support user profiles with spaces.
$SparkTempDirForCmd = Get-WindowsShortPath $SparkTempDir
$env:TEMP = $SparkTempDirForCmd
$env:TMP = $SparkTempDirForCmd

if ($env:JAVA_HOME -and -not (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
    $JavaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($JavaCommand) {
        $env:JAVA_HOME = Split-Path (Split-Path $JavaCommand.Source -Parent) -Parent
    } else {
        Remove-Item Env:\JAVA_HOME
    }
}

$PySparkHome = Join-Path $ProjectRoot.Path ".venv\Lib\site-packages\pyspark"
if (Test-Path (Join-Path $PySparkHome "jars")) {
    $env:SPARK_HOME = Get-WindowsShortPath $PySparkHome
}

if (-not $env:SPARK_SCALA_VERSION) {
    $env:SPARK_SCALA_VERSION = "2.12"
}

if (-not $env:PYSPARK_PYTHON) {
    $env:PYSPARK_PYTHON = "python"
}

if (-not $env:PYSPARK_DRIVER_PYTHON) {
    $env:PYSPARK_DRIVER_PYTHON = "python"
}

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

if ($RunMode -eq "docker" -or ($RunMode -eq "auto" -and -not (Test-HadoopHomeReady))) {
    if ($RunMode -eq "auto" -and -not (Test-HadoopHomeReady)) {
        Write-Host "HADOOP_HOME/bin/winutils.exe was not found. Running Spark inside the Docker spark-master service instead."
    }

    Invoke-DockerSparkSubmit -SparkPackages $SparkPackages -SparkArgs $RemainingArgs
}

if (-not (Test-HadoopHomeReady)) {
    $HadoopHomeMessage = @"
HADOOP_HOME/bin/winutils.exe was not found.

To run Spark locally on Windows, install a Hadoop winutils package and set:
  `$env:HADOOP_HOME = "C:\hadoop"
  `$env:Path = "`$env:HADOOP_HOME\bin;`$env:Path"

Or run through Docker:
  .\scripts\run_stream_speed_layer.ps1 -RunMode docker
"@
    [Console]::Error.WriteLine($HadoopHomeMessage)
    exit 1
}

$LocalCheckpointDir = if ($env:CHECKPOINT_DIR) {
    $env:CHECKPOINT_DIR
} else {
    Join-Path $ProjectRoot.Path ".checkpoints\speed"
}
$env:CHECKPOINT_DIR = New-FirstWritableDirectory @($LocalCheckpointDir)

$ResolvedHadoopHome = Resolve-HadoopHome
$env:HADOOP_HOME = $ResolvedHadoopHome
$HadoopHomeForJava = $ResolvedHadoopHome.Replace("\", "/")

$ExistingSparkSubmitOpts = if ($env:SPARK_SUBMIT_OPTS) {
    (($env:SPARK_SUBMIT_OPTS -split "\s+") | Where-Object { $_ -and ($_ -notmatch "^-Dhadoop\.home\.dir=") }) -join " "
} else {
    ""
}
$env:SPARK_SUBMIT_OPTS = "$ExistingSparkSubmitOpts -Dhadoop.home.dir=$HadoopHomeForJava".Trim()

& $UvBin @ArgsList
exit $LASTEXITCODE
