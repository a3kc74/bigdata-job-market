.PHONY: k8s-up k8s-down elastic-up kibana-up spark-operator-up build-spark-image apps-up status

k8s-up:
	kubectl apply -f infra/k8s/namespace.yaml
	kubectl apply -f infra/k8s/configmaps/
	kubectl apply -f infra/k8s/secrets/
	kubectl apply -f infra/k8s/pvc/
	kubectl apply -f infra/k8s/statefulsets/
	kubectl apply -f infra/k8s/deployments/
	kubectl apply -f infra/k8s/services/
	kubectl apply -f infra/k8s/jobs/
	kubectl apply -f infra/spark/10-rbac.yaml

k8s-down:
	kubectl delete -f infra/spark-operator/ --ignore-not-found
	kubectl delete -f infra/elastic/ --ignore-not-found
	kubectl delete -f infra/k8s/jobs/ --ignore-not-found
	kubectl delete -f infra/k8s/services/ --ignore-not-found
	kubectl delete -f infra/k8s/deployments/ --ignore-not-found
	kubectl delete -f infra/k8s/statefulsets/ --ignore-not-found
	kubectl delete -f infra/k8s/pvc/ --ignore-not-found
	kubectl delete -f infra/k8s/secrets/ --ignore-not-found
	kubectl delete -f infra/k8s/configmaps/ --ignore-not-found
	kubectl delete -f infra/spark/10-rbac.yaml --ignore-not-found
	kubectl delete -f infra/k8s/namespace.yaml --ignore-not-found

elastic-up:
	kubectl apply -f infra/elastic/10-elasticsearch.yaml

kibana-up:
	kubectl apply -f infra/elastic/20-kibana.yaml

spark-operator-up:
	helm repo add spark-operator https://kubeflow.github.io/spark-operator || true
	helm repo update
	helm install spark-operator spark-operator/spark-operator \
	  --namespace spark-operator \
	  --create-namespace || true

build-spark-image:
	docker build -t spark-job-market:latest -f infra/spark/Dockerfile .
	minikube image load spark-job-market:latest

apps-up:
	kubectl apply -f infra/spark-operator/speed-job.yaml
	kubectl apply -f infra/spark-operator/silver-job.yaml
	kubectl apply -f infra/spark-operator/gold-job.yaml
	kubectl apply -f infra/spark-operator/gold-sync-mongo-job.yaml
	kubectl apply -f infra/spark-operator/gold-sync-es-job.yaml

status:
	kubectl get all -n job-market
	kubectl get pods -n elastic-stack
	kubectl get sparkapplications -A
	kubectl get scheduledsparkapplications -A