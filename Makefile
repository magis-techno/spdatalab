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

# 初始化FDW连接（连接远程trajectory和map数据库）
init-fdw:
	@echo "初始化FDW连接..."
	@echo "请确保已经修改sql/init_fdw.sql中的连接参数"
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/init_fdw.sql

# 检查FDW连接状态
check-fdw:
	@echo "检查FDW连接状态..."
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -c \
	  "SELECT srvname, srvoptions FROM pg_foreign_server WHERE srvname IN ('traj_srv', 'map_srv');"
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -c \
	  "SELECT schemaname, tablename, servername FROM pg_foreign_tables WHERE tablename IN ('ddi_data_points', 'intersections');"

# 清理FDW连接
clean-fdw:
	@echo "清理FDW连接..."
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=postgres \
	  psql -h local_pg -U postgres -f sql/cleanup_fdw.sql

# 显示帮助信息
help:
	@echo "Spatial-Data-Lab 开发环境管理"
	@echo ""
	@echo "基础命令："
	@echo "  make up        - 启动开发环境"
	@echo "  make down      - 停止开发环境"
	@echo "  make init-db   - 初始化数据库（首次使用）"
	@echo "  make psql      - 进入PostgreSQL命令行"
	@echo ""
	@echo "FDW远程连接管理："
	@echo "  make init-fdw  - 初始化FDW连接（trajectory和map数据库）"
	@echo "  make check-fdw - 检查FDW连接状态"
	@echo "  make clean-fdw - 清理FDW连接"
	@echo ""
	@echo "数据管理："
	@echo "  make clean-bbox- 清理clips_bbox表"
	@echo ""
	@echo "数据处理请使用命令行工具："
	@echo "  spdatalab build-dataset-with-bbox [参数]"
	@echo ""
	@echo "注意："
	@echo "  使用init-fdw前请先修改sql/init_fdw.sql中的连接参数"

.PHONY: up down psql init-db clean-bbox init-fdw check-fdw clean-fdw help