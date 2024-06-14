SHELL=/bin/bash

IMAGE="200299/airflow"
TAG="2.8.3"

build_and_push_image:
	docker buildx build --platform linux/amd64,linux/arm64 --pull --tag ${IMAGE}:${TAG} --push .
