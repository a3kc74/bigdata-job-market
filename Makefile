.PHONY: namespaces-up hdfs-up search-up serving-up spark-rbac-up spark-build raw-to-bronze-up bronze-to-silver-up silver-to-gold-up gold-to-elasticsearch-up kafka-up kafka-topics-up status platform-up speed-k8s-up

namespaces-up:
	kubectl apply -f infra/namespaces/all.yaml

hdfs-up:
	kubectl apply -f infra/hdfs/hdfs.yaml

search-up:
	kubectl apply -f infra/search/

api-build:
	minikube image build -f apps/api/Dockerfile -t job-search-api:latest .

serving-up:
	kubectl apply -f infra/serving/

spark-build:
	minikube image build -f infra/spark/Dockerfile -t spark-job-market:latest .

spark-rbac-up:
	kubectl apply -f infra/spark/rbac.yaml

raw-to-bronze-up:
	kubectl apply -f infra/spark/raw-to-bronze-cronjob.yaml

bronze-to-silver-up:
	kubectl apply -f infra/spark/bronze-to-silver-cronjob.yaml

silver-to-gold-up:
	kubectl apply -f infra/spark/silver-to-gold-cronjob.yaml

gold-to-elasticsearch-up:
	kubectl apply -f infra/spark/gold-to-elasticsearch-cronjob.yaml

kafka-up:
	kubectl apply -f infra/kafka/

kafka-topics-up:
	kubectl apply -f infra/kafka/topics.yaml

platform-up: namespaces-up hdfs-up search-up api-build serving-up spark-build spark-rbac-up raw-to-bronze-up bronze-to-silver-up silver-to-gold-up gold-to-elasticsearch-up

speed-k8s-up: namespaces-up search-up kafka-up kafka-topics-up spark-build spark-rbac-up

api-forward:
	kubectl port-forward -n serving svc/job-search-api 8001:8000

kibana-forward:
	kubectl port-forward -n search svc/kibana 5601:5601

hdfs-forward:
	kubectl port-forward -n hdfs svc/hdfs-namenode 9870:9870

status:
	kubectl get pods -n hdfs
	kubectl get pods -n spark
	kubectl get pods -n search
	kubectl get pods -n serving
	kubectl get pods -n kafka