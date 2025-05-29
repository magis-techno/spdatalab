up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

psql:
	docker exec -it local_pg psql -U postgres

init-db:
	docker compose -f docker/docker-compose.yml \
	  exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/00_init_local_pg.sql