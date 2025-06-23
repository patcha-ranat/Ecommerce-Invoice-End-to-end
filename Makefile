.EXPORT_ALL_VARIABLES:
.SILENT: venv clean start stop


# COMPOSE_CONVERT_WINDOWS_PATHS=1 # this allow components to use Windows path such as "C://", but instead we can use "/c/Users/..." for wsl

install:
	pip install -r requirements.txt
	pip install -r code/models/requirements.txt
	chmod 666 /var/run/docker.sock

start:
	docker compose -f docker/docker-compose.yml up --build

stop:
	docker compose -f docker/docker-compose.yml down -v

clean:
	docker builder prune

airflow-import:
	docker exec -it airflow-scheduler bash \
	&& airflow variables import config/variables.json -a overwrite \
	&& airflow connections import config/connections.json --overwrite\
	&& exit

test:
	PYTHONPATH="code/models" pytest tests/unit/test_ml_services.py --disable-pytest-warnings
