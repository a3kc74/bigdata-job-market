param(
    [string] $Namespace = "spark",
    [string] $Manifest = "infra/spark/speed-stream-es-job.yaml",
    [switch] $FollowLogs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[speed-k8s] Applying Spark RBAC..."
kubectl apply -f infra/spark/rbac.yaml

Write-Host "[speed-k8s] Applying Kafka topics..."
kubectl apply -f infra/kafka/jobs-topics.yaml

Write-Host "[speed-k8s] Restarting speed stream submit job..."
kubectl delete job speed-stream-es-submit -n $Namespace --ignore-not-found
kubectl apply -f $Manifest

Write-Host "[speed-k8s] Current pods:"
kubectl get pods -n $Namespace -l app=speed-stream

if ($FollowLogs) {
    Write-Host "[speed-k8s] Following submit job logs..."
    kubectl logs -n $Namespace job/speed-stream-es-submit -f
}