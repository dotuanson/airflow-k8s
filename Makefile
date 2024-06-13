SHELL=/bin/bash

IMAGE="200299/airflow"
TAG="2.8.3"

build_image:
	docker buildx build --platform linux/amd64,linux/arm64 --pull --tag ${IMAGE}:${TAG} --push .

push_image:
	docker push ${IMAGE}:${TAG}