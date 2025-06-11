# Spatial-Data-Lab 开发环境管理
# 用途：本地开发、测试、环境搭建

# 加载环境变量（如果.env文件存在）
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

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

# ============================================================================
# FDW 远程数据库连接管理
# ============================================================================

# 调试FDW配置
debug-fdw:
	@echo "🔍 调试FDW配置..."
	python debug_fdw_config.py

# 设置FDW连接（需要先配置.env文件）
setup-fdw:
	@if [ ! -f .env ]; then \
		echo "❌ .env文件不存在，请先创建配置文件："; \
		echo "   cp env.example .env"; \
		echo "   然后编辑.env文件设置你的数据库连接参数"; \
		exit 1; \
	fi
	@echo "🔧 设置FDW连接..."
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=$(LOCAL_POSTGRES_PASSWORD) \
	  psql -h $(LOCAL_POSTGRES_HOST) -U $(LOCAL_POSTGRES_USER) -d $(LOCAL_POSTGRES_DB) \
	  -v fdw_user=$(FDW_USER) \
	  -v fdw_pwd=$(FDW_PASSWORD) \
	  -v traj_host=$(REMOTE_TRAJ_HOST) \
	  -v traj_port=$(REMOTE_TRAJ_PORT) \
	  -v traj_db=$(REMOTE_TRAJ_DB) \
	  -v map_host=$(REMOTE_MAP_HOST) \
	  -v map_port=$(REMOTE_MAP_PORT) \
	  -v map_db=$(REMOTE_MAP_DB) \
	  -f sql/setup_fdw_with_params.sql

# 清理FDW连接
cleanup-fdw:
	@echo "🧹 清理FDW连接..."
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=$(LOCAL_POSTGRES_PASSWORD) \
	  psql -h $(LOCAL_POSTGRES_HOST) -U $(LOCAL_POSTGRES_USER) -d $(LOCAL_POSTGRES_DB) \
	  -f sql/cleanup_fdw.sql

# 测试FDW连接
test-fdw:
	@echo "🧪 测试FDW连接..."
	docker compose -f docker/docker-compose.yml exec -T workspace \
	  env PGPASSWORD=$(LOCAL_POSTGRES_PASSWORD) \
	  psql -h $(LOCAL_POSTGRES_HOST) -U $(LOCAL_POSTGRES_USER) -d $(LOCAL_POSTGRES_DB) \
	  -c "SELECT 'ddi_data_points' as table_name, COUNT(*) as row_count FROM ddi_data_points LIMIT 1;" \
	  -c "SELECT 'intersections' as table_name, COUNT(*) as row_count FROM intersections LIMIT 1;"

# ============================================================================
# Sprint 2 测试和演示
# ============================================================================

# 运行Sprint 2演示脚本
demo-sprint2:
	@echo "🎬 运行Sprint 2演示..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python demo_sprint2_commands.py

# 运行Sprint 2功能测试（需要测试数据）
test-sprint2:
	@if [ ! -f .env ]; then \
		echo "❌ .env文件不存在，请先配置环境变量"; \
		exit 1; \
	fi
	@if [ ! -f $(TEST_DATASET_FILE) ]; then \
		echo "❌ 测试数据文件不存在: $(TEST_DATASET_FILE)"; \
		echo "   请在.env中设置TEST_DATASET_FILE为有效的数据集文件路径"; \
		exit 1; \
	fi
	@echo "🧪 运行Sprint 2功能测试..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python test_sprint2.py --dataset-file $(TEST_DATASET_FILE)

# 运行Sprint 2完整测试（包括数据处理）
test-sprint2-full:
	@if [ ! -f .env ]; then \
		echo "❌ .env文件不存在，请先配置环境变量"; \
		exit 1; \
	fi
	@if [ ! -f $(TEST_DATASET_FILE) ]; then \
		echo "❌ 测试数据文件不存在: $(TEST_DATASET_FILE)"; \
		exit 1; \
	fi
	@echo "🚀 运行Sprint 2完整测试..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python test_sprint2.py --dataset-file $(TEST_DATASET_FILE) --test-processing

# 清理Sprint 2测试资源
cleanup-sprint2:
	@echo "🧹 清理Sprint 2测试资源..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python test_sprint2.py --dataset-file dummy.json --cleanup

# ============================================================================
# 分表模式CLI命令（使用.env配置）
# ============================================================================

# 列出所有bbox表
list-tables:
	@echo "📋 列出bbox表..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python -m spdatalab list-bbox-tables

# 创建统一视图
create-view:
	@echo "🔧 创建统一视图..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python -m spdatalab create-unified-view

# 维护统一视图
maintain-view:
	@echo "🔧 维护统一视图..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python -m spdatalab maintain-unified-view

# 分表模式处理数据（需要在.env中设置TEST_DATASET_FILE）
process-partitioned:
	@if [ ! -f .env ]; then \
		echo "❌ .env文件不存在，请先配置环境变量"; \
		exit 1; \
	fi
	@if [ ! -f $(TEST_DATASET_FILE) ]; then \
		echo "❌ 数据集文件不存在: $(TEST_DATASET_FILE)"; \
		exit 1; \
	fi
	@echo "🎯 使用分表模式处理数据..."
	docker compose -f docker/docker-compose.yml exec workspace \
	  env PYTHONPATH=$(PYTHONPATH) \
	  python -m spdatalab process-bbox \
	  --input $(TEST_DATASET_FILE) \
	  --use-partitioning \
	  --batch $(TEST_BATCH_SIZE) \
	  --insert-batch $(TEST_INSERT_BATCH_SIZE)

# 显示帮助信息
help:
	@echo "Spatial-Data-Lab 开发环境管理"
	@echo ""
	@echo "🐳 环境管理："
	@echo "  make up          - 启动开发环境"
	@echo "  make down        - 停止开发环境"
	@echo "  make init-db     - 初始化数据库（首次使用）"
	@echo "  make clean-bbox  - 清理clips_bbox表"
	@echo "  make psql        - 进入PostgreSQL命令行"
	@echo ""
	@echo "🔗 FDW远程连接管理："
	@echo "  make debug-fdw   - 调试FDW配置参数"
	@echo "  make setup-fdw   - 设置FDW连接（需要先配置.env）"
	@echo "  make cleanup-fdw - 清理FDW连接"
	@echo "  make test-fdw    - 测试FDW连接"
	@echo ""
	@echo "🎯 Sprint 2测试和演示："
	@echo "  make demo-sprint2     - 运行Sprint 2演示脚本"
	@echo "  make test-sprint2     - 运行Sprint 2功能测试"
	@echo "  make test-sprint2-full- 运行Sprint 2完整测试"
	@echo "  make cleanup-sprint2  - 清理Sprint 2测试资源"
	@echo ""
	@echo "📊 分表模式操作："
	@echo "  make list-tables      - 列出所有bbox表"
	@echo "  make create-view      - 创建统一视图"
	@echo "  make maintain-view    - 维护统一视图"
	@echo "  make process-partitioned - 分表模式处理数据"
	@echo ""
	@echo "⚙️  配置说明："
	@echo "  1. 复制配置模板: cp env.example .env"
	@echo "  2. 编辑.env文件设置你的参数"
	@echo "  3. 运行相应的make命令"

.PHONY: up down psql init-db clean-bbox help \
        debug-fdw setup-fdw cleanup-fdw test-fdw \
        demo-sprint2 test-sprint2 test-sprint2-full cleanup-sprint2 \
        list-tables create-view maintain-view process-partitioned