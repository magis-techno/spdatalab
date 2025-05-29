up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

psql:
	docker exec -it local_pg psql -U postgres

init-db:
	docker compose exec -T local_pg psql -U postgres -f /workspace/sql/00_init_local_pg.sql