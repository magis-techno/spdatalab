up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

psql:
	docker exec -it local_pg psql -U postgres

init-db:
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/00_init_local_pg.sql

fdw-init:
	docker compose -f docker/docker-compose.yml exec -T \
	  -e PGPASSWORD=postgres \
	  -e FDW_USER_ENV=$$FDW_USER \
	  -e FDW_PWD_ENV=$$FDW_PWD \
	  workspace \
	  sh -c 'psql -h local_pg -U postgres \
	    -v fdw_user_val="$$FDW_USER_ENV" \
	    -v fdw_pwd_val="$$FDW_PWD_ENV" \
	    -f sql/01_fdw_remote.sql'