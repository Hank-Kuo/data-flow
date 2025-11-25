airflow:
	docker compose -f docker/docker-compose_airflow.yaml up -d



superset:
	uv run superset run -p 8088 --with-threads --reload --debugger
