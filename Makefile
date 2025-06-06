# Spatial-Data-Lab 开发环境管理
# 用途：本地开发、测试、环境搭建

# 启动开发环境（PostgreSQL + PgAdmin + workspace）
up:
	docker compose -f docker/docker-compose.yml up -d

# 停止开发环境
down:
	docker compose -f docker/docker-compose.yml down

# 进入PostgreSQL命令行（调试用）
psql:
	docker exec -it local_pg psql -U postgres

# 初始化本地数据库（仅首次使用）
init-db:
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/init.sql

# 清理clips_bbox表
clean-bbox:
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/cleanup.sql

# 显示帮助信息
help:
	@echo "Spatial-Data-Lab 开发环境管理"
	@echo ""
	@echo "常用命令："
	@echo "  make up        - 启动开发环境"
	@echo "  make down      - 停止开发环境"
	@echo "  make init-db   - 初始化数据库（首次使用）"
	@echo "  make clean-bbox- 清理clips_bbox表"
	@echo "  make psql      - 进入PostgreSQL命令行"
	@echo ""
	@echo "数据处理请使用命令行工具："
	@echo "  spdatalab build-dataset-with-bbox [参数]"

.PHONY: up down psql init-db clean-bbox help