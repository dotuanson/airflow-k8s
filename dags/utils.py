import pendulum


class DefaultConfig:
    DEFAULT_DAG_ARGS = {
        "owner": "Prep",
        "retries": 0,
        "retry_delay": pendulum.duration(seconds=20),
    }

    DEFAULT_DOCKER_OPERATOR_ARGS = {
        "image": f"nginx:latest",
        "api_version": "auto",
        "auto_remove": True,
        # "mounts": [
        #     # feature repo
        #     Mount(
        #         source=AppPath.FEATURE_REPO.absolute().as_posix(),
        #         target="/data_pipeline/feature_repo",
        #         type="bind",
        #     ),
        # ],
        # Fix a permission denied when using DockerOperator in Airflow
        # Ref: https://stackoverflow.com/a/70100729
        # "docker_url": "tcp://docker-proxy:2375",
    }
