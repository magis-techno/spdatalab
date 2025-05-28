# 自动检测 docker compose 命令名
COMPOSE_CMD := $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo docker compose)

up:
	$(COMPOSE_CMD) -f docker/docker-compose.yml up -d

down:
	$(COMPOSE_CMD) -f docker/docker-compose.yml down

psql:
	docker exec -it local_pg psql -U postgres

init-db:
	$(COMPOSE_CMD) exec -T local_pg psql -U postgres -f /workspace/sql/00_init_local_pg.sql