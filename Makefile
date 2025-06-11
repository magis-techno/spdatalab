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

# 标准清理：删除clips_bbox相关数据，重建干净表
clean-bbox:
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/cleanup.sql

# 彻底清理：删除所有用户schema和数据（危险操作）
clean-deep:
	@echo "⚠️  警告：此操作将删除所有用户schema和数据！"
	@echo "按Ctrl+C取消，或按Enter继续..."
	@read dummy
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -v cleanup_level=2 -f sql/cleanup.sql

# 显示帮助信息
help:
	@echo "Spatial-Data-Lab 开发环境管理"
	@echo ""
	@echo "常用命令："
	@echo "  make up                  - 启动开发环境"
	@echo "  make down                - 停止开发环境"
	@echo "  make init-db             - 初始化数据库（首次使用）"
	@echo "  make clean-bbox          - 标准清理：删除clips_bbox相关数据，重建干净表"
	@echo "  make clean-deep          - 彻底清理：删除所有用户schema和数据（危险）"
	@echo "  make psql                - 进入PostgreSQL命令行"
	@echo ""
	@echo "数据处理请使用命令行工具："
	@echo "  spdatalab build-dataset-with-bbox [参数]"

.PHONY: up down psql init-db clean-bbox clean-deep help