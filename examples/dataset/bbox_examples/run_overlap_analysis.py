#!/usr/bin/env python3
"""
Docker兼容的bbox叠置分析启动脚本

这个脚本专门为Docker环境设计，自动处理路径和依赖导入问题。

使用方法：
    # 在Docker容器中运行
    python examples/dataset/bbox_examples/run_overlap_analysis.py

    # 带参数运行
    python examples/dataset/bbox_examples/run_overlap_analysis.py \
        --city beijing --refresh-view --top-n 15
"""

import sys
import os
import signal
import atexit
from pathlib import Path

def setup_environment():
    """设置运行环境，处理路径和导入问题"""
    
    # 获取项目根目录
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent.parent
    
    print(f"🔧 脚本位置: {script_path}")
    print(f"🔧 项目根目录: {project_root}")
    
    # 添加可能的路径
    paths_to_add = [
        str(project_root),
        str(project_root / "src"),
        "/workspace",
        "/workspace/src"
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    print(f"🔧 Python路径:")
    for i, path in enumerate(sys.path[:5]):  # 只显示前5个
        print(f"   {i}: {path}")
    
    # 尝试导入测试
    try:
        # 尝试方式1：直接导入
        from spdatalab.dataset.bbox import LOCAL_DSN
        print("✅ 导入方式: 直接导入 spdatalab")
        return True
    except ImportError as e1:
        try:
            # 尝试方式2：从src导入
            from src.spdatalab.dataset.bbox import LOCAL_DSN
            print("✅ 导入方式: 从src导入 spdatalab")
            return True
        except ImportError as e2:
            print(f"❌ 导入失败:")
            print(f"   方式1错误: {e1}")
            print(f"   方式2错误: {e2}")
            
            # 显示当前目录结构以供调试
            print(f"\n🔍 当前目录结构:")
            cwd = Path.cwd()
            print(f"   当前工作目录: {cwd}")
            
            # 检查是否存在spdatalab模块
            possible_paths = [
                cwd / "spdatalab",
                cwd / "src" / "spdatalab", 
                project_root / "spdatalab",
                project_root / "src" / "spdatalab"
            ]
            
            for path in possible_paths:
                exists = path.exists()
                print(f"   {path}: {'存在' if exists else '不存在'}")
                
            return False

def main():
    """主函数"""
    print("🎯 Docker兼容的BBox叠置分析")
    print("=" * 50)
    
    # 设置优雅退出处理
    shutdown_requested = False
    current_connection = None
    analysis_start_time = None
    
    def signal_handler(signum, frame):
        nonlocal shutdown_requested, current_connection, analysis_start_time
        print(f"\n\n🛑 收到退出信号 ({signal.Signals(signum).name})")
        shutdown_requested = True
        print(f"🔄 正在安全退出...")
        
        if analysis_start_time:
            elapsed = datetime.now() - analysis_start_time
            print(f"⏱️ 已运行时间: {elapsed}")
        
        if current_connection:
            try:
                current_connection.close()
                print(f"✅ 数据库连接已关闭")
            except Exception as e:
                print(f"⚠️ 关闭连接时出错: {e}")
        
        print(f"✅ 优雅退出完成")
        sys.exit(0)
    
    def check_shutdown():
        if shutdown_requested:
            print(f"🛑 检测到退出请求，停止执行")
            sys.exit(0)
    
    # 注册信号处理器
    try:
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # 终止信号
        if hasattr(signal, 'SIGBREAK'):  # Windows
            signal.signal(signal.SIGBREAK, signal_handler)
    except ValueError:
        print("⚠️ 无法注册信号处理器")
    
    # 设置环境
    if not setup_environment():
        print("\n❌ 环境设置失败，无法继续")
        sys.exit(1)
    
    print("\n🚀 环境设置成功，开始导入模块...")
    
    try:
        # 导入必要的模块
        import argparse
        from datetime import datetime
        
        # 尝试导入分析器（使用统一视图）
        try:
            from spdatalab.dataset.bbox import (
                create_unified_view,
                list_bbox_tables,
                LOCAL_DSN
            )
        except ImportError:
            from src.spdatalab.dataset.bbox import (
                create_unified_view,
                list_bbox_tables,
                LOCAL_DSN
            )
        
        from sqlalchemy import create_engine, text
        import pandas as pd
        
        print("✅ 所有模块导入成功")
        
        # 解析命令行参数
        parser = argparse.ArgumentParser(description='Docker兼容的BBox叠置分析（优化版）')
        parser.add_argument('--city', required=False, help='城市过滤（强烈建议指定以避免性能问题）')
        parser.add_argument('--subdatasets', nargs='+', help='子数据集过滤')
        parser.add_argument('--min-overlap-area', type=float, default=0.0, help='最小重叠面积阈值（只在--calculate-area时生效）')
        parser.add_argument('--top-n', type=int, default=15, help='返回的热点数量')
        parser.add_argument('--analysis-id', help='自定义分析ID')
        parser.add_argument('--refresh-view', action='store_true', help='强制刷新统一视图')
        parser.add_argument('--test-only', action='store_true', help='只运行测试，不执行分析')
        parser.add_argument('--suggest-city', action='store_true', help='显示城市分析建议并退出')
        parser.add_argument('--estimate-time', action='store_true', help='估算分析时间并退出')
        # 🔥 网格化分析参数（默认启用）
        parser.add_argument('--grid-size', type=float, default=0.002, help='网格大小（度），默认0.002度约200米')
        parser.add_argument('--percentile', type=float, default=90, help='密度阈值分位数（0-100），默认90分位数')
        parser.add_argument('--top-percent', type=float, default=5, help='返回最密集的前X%网格，默认5%')
        parser.add_argument('--max-results', type=int, default=50, help='最大返回网格数量限制，默认50')
        parser.add_argument('--calculate-area', action='store_true', help='计算重叠面积并应用min-overlap-area阈值（默认只检查相交）')
        # 兼容旧参数
        parser.add_argument('--density-threshold', type=int, help='固定密度阈值（兼容旧版本，建议使用--percentile）')
        # 🧹 清理和诊断功能
        parser.add_argument('--diagnose', action='store_true', help='诊断bbox数据状态并退出')
        parser.add_argument('--cleanup-views', action='store_true', help='清理旧的bbox视图')
        
        args = parser.parse_args()
        
        # 参数兼容性处理
        if args.density_threshold:
            print(f"ℹ️ 使用兼容模式: 固定密度阈值 {args.density_threshold}")
        
        print(f"\n📋 分析参数:")
        print(f"   城市过滤: {args.city}")
        print(f"   强制刷新视图: {args.refresh_view}")
        print(f"   🔥 智能网格化分析: 已启用")
        print(f"   📏 网格大小: {args.grid_size}° × {args.grid_size}° (约200m×200m)")
        
        if args.density_threshold:
            print(f"   📊 固定密度阈值: {args.density_threshold} bbox/网格")
        else:
            print(f"   📊 动态密度阈值: {args.percentile}分位数")
        
        print(f"   📊 返回策略: 前{args.top_percent}%最密集网格")
        print(f"   🔢 最大返回数量: {args.max_results}个网格")
        print(f"   🎯 分析模式: {'面积计算模式' if args.calculate_area else '快速相交模式（默认）'}")
        if args.calculate_area and args.min_overlap_area > 0:
            print(f"   📐 最小重叠面积: {args.min_overlap_area}")
        elif args.calculate_area:
            print(f"   📐 计算面积但不过滤")
        
        # 创建数据库连接
        print(f"\n🔌 连接数据库...")
        engine = create_engine(LOCAL_DSN, future=True)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test;"))
            print(f"✅ 数据库连接成功")
        
        # 检查bbox表
        print(f"\n📊 检查bbox分表...")
        tables = list_bbox_tables(engine)
        bbox_tables = [t for t in tables if t.startswith('clips_bbox_') and t != 'clips_bbox']
        print(f"✅ 发现 {len(bbox_tables)} 个bbox分表")
        
        if len(bbox_tables) == 0:
            print("❌ 没有发现bbox分表，无法执行分析")
            return
        
        # 检查统一视图（使用标准视图）
        print(f"\n🔍 检查统一视图...")
        view_name = "clips_bbox_unified"
        
        check_view_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = '{view_name}'
            );
        """)
        
        # 创建持久连接用于整个分析过程
        conn = engine.connect()
        
        try:
            result = conn.execute(check_view_sql)
            view_exists = result.scalar()
            
            if not view_exists or args.refresh_view:
                if args.refresh_view:
                    print(f"🔄 强制刷新模式，重新创建视图...")
                else:
                    print(f"📌 视图不存在，创建新视图...")
                
                success = create_unified_view(engine, view_name)
                if not success:
                    print("❌ 统一视图创建失败")
                    return
                print(f"✅ 统一视图创建成功")
            else:
                print(f"✅ 统一视图已存在")
            
            # 检查数据量
            count_sql = text(f"SELECT COUNT(*) FROM {view_name};")
            count_result = conn.execute(count_sql)
            row_count = count_result.scalar()
            print(f"📊 统一视图包含 {row_count:,} 条bbox记录")
            
            if row_count == 0:
                print("⚠️ 统一视图为空，可能分表中没有数据")
                return
            
            # 如果只是测试模式，到这里就结束
            if args.test_only:
                print(f"\n✅ 测试模式完成，所有检查通过！")
                print(f"💡 移除 --test-only 参数可以执行完整分析")
                return
            
            # 如果用户想查看城市建议
            if args.suggest_city:
                print(f"\n🏙️ 城市分析建议")
                print("-" * 40)
                
                city_stats_sql = f"""
                SELECT 
                    city_id,
                    COUNT(*) as total_count,
                    COUNT(*) FILTER (WHERE all_good = true) as good_count,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*), 1) as good_percent,
                    CASE 
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 50000 THEN '> 10分钟'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 10000 THEN '2-10分钟'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 1000 THEN '< 2分钟'
                        ELSE '< 30秒'
                    END as estimated_time,
                    CASE 
                        WHEN COUNT(*) FILTER (WHERE all_good = true) BETWEEN 1000 AND 20000 AND 
                             100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*) > 90 THEN '⭐⭐⭐ 推荐'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) BETWEEN 500 AND 50000 AND 
                             100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*) > 85 THEN '⭐⭐ 较好'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 100 THEN '⭐ 可用'
                        ELSE '❌ 不建议'
                    END as recommendation
                FROM {view_name}
                WHERE city_id IS NOT NULL
                GROUP BY city_id
                HAVING COUNT(*) FILTER (WHERE all_good = true) > 0
                ORDER BY 
                    CASE 
                        WHEN COUNT(*) FILTER (WHERE all_good = true) BETWEEN 1000 AND 20000 AND 
                             100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*) > 90 THEN 1
                        WHEN COUNT(*) FILTER (WHERE all_good = true) BETWEEN 500 AND 50000 AND 
                             100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*) > 85 THEN 2
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 100 THEN 3
                        ELSE 4
                    END,
                    COUNT(*) FILTER (WHERE all_good = true) DESC;
                """
                
                city_df = pd.read_sql(city_stats_sql, engine)
                if not city_df.empty:
                    print("📊 城市分析建议表:")
                    print(city_df.to_string(index=False))
                    
                    recommended = city_df[city_df['recommendation'].str.contains('⭐⭐⭐')]
                    if not recommended.empty:
                        best_city = recommended.iloc[0]['city_id']
                        print(f"\n💡 推荐城市: {best_city}")
                        print(f"   - 建议命令: --city {best_city}")
                else:
                    print("❌ 未找到可用的城市数据")
                return
            
            # 如果用户想诊断数据状态
            if args.diagnose:
                print(f"\n🔍 数据状态诊断")
                print("-" * 40)
                
                # 检查分表数量和数据量
                print(f"📋 分表状态:")
                print(f"   发现 {len(bbox_tables)} 个bbox分表")
                print(f"   视图记录数: {row_count:,}")
                
                # 检查数据质量分布
                quality_sql = f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE all_good = true) as good,
                    COUNT(DISTINCT city_id) as cities,
                    COUNT(DISTINCT subdataset_name) as subdatasets
                FROM {view_name};
                """
                quality_result = conn.execute(text(quality_sql)).fetchone()
                good_percent = (quality_result.good / quality_result.total * 100) if quality_result.total > 0 else 0
                
                print(f"📊 数据质量:")
                print(f"   总记录数: {quality_result.total:,}")
                print(f"   优质记录: {quality_result.good:,} ({good_percent:.1f}%)")
                print(f"   城市数量: {quality_result.cities}")
                print(f"   数据集数量: {quality_result.subdatasets}")
                
                # 建议
                if quality_result.good < 1000:
                    print(f"\n💡 建议: 数据量较小，任何城市都可快速分析")
                elif quality_result.good > 100000:
                    print(f"\n💡 建议: 数据量较大，建议使用 --suggest-city 选择合适城市")
                else:
                    print(f"\n💡 建议: 数据状态良好，可以进行overlap分析")
                
                print(f"\n✅ 诊断完成")
                return
            
            # 如果用户想清理视图
            if args.cleanup_views:
                print(f"\n🧹 清理bbox相关视图")
                print("-" * 40)
                
                views_to_check = [
                    'clips_bbox_unified_qgis',
                    'clips_bbox_unified_mat', 
                    'qgis_bbox_overlap_hotspots'
                ]
                
                check_views_sql = text("""
                    SELECT table_name 
                    FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = ANY(:view_names);
                """)
                
                existing_views = conn.execute(check_views_sql, {'view_names': views_to_check}).fetchall()
                existing_view_names = [row[0] for row in existing_views]
                
                if not existing_view_names:
                    print("✅ 没有找到需要清理的视图")
                else:
                    print(f"📋 找到以下视图，将删除:")
                    for view_name_to_delete in existing_view_names:
                        print(f"   - {view_name_to_delete}")
                    
                    for view_name_to_delete in existing_view_names:
                        try:
                            drop_sql = text(f"DROP VIEW IF EXISTS {view_name_to_delete} CASCADE;")
                            conn.execute(drop_sql)
                            print(f"✅ 删除视图: {view_name_to_delete}")
                        except Exception as e:
                            print(f"❌ 删除失败 {view_name_to_delete}: {str(e)}")
                    
                    conn.commit()
                    print(f"✅ 视图清理完成")
                
                return
            
            # 如果用户想估算时间
            if args.estimate_time:
                print(f"\n⏱️ 分析时间估算")
                print("-" * 40)
                
                where_condition = f"WHERE city_id = '{args.city}'" if args.city else "WHERE city_id IS NOT NULL"
                time_estimate_sql = f"""
                SELECT 
                    COUNT(*) FILTER (WHERE all_good = true) as analyzable_count,
                    CASE 
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 100000 THEN '⚠️ 很长 (>30分钟)'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 50000 THEN '⏳ 较长 (10-30分钟)'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 10000 THEN '⏰ 中等 (2-10分钟)'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 1000 THEN '⚡ 较快 (<2分钟)'
                        ELSE '🚀 很快 (<30秒)'
                    END as time_estimate,
                    '{args.city if args.city else "全部城市"}' as scope
                FROM {view_name}
                {where_condition};
                """
                
                estimate_result = conn.execute(text(time_estimate_sql)).fetchone()
                print(f"📊 分析范围: {estimate_result.scope}")
                print(f"📈 可分析数据: {estimate_result.analyzable_count:,} 个bbox")
                print(f"⏱️ 预估时间: {estimate_result.time_estimate}")
                
                if estimate_result.analyzable_count > 50000:
                    print(f"💡 建议: 数据量较大，建议指定具体城市进行分析")
                return
        
        finally:
            # 确保连接总是会被关闭
            if conn:
                conn.close()
        
        # 创建分析结果表
        print(f"\n🛠️ 准备分析表...")
        
        analysis_table = "bbox_overlap_analysis_results"
        
        # 检查表是否存在
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{analysis_table}'
            );
        """)
        
        with engine.connect() as conn:
            result = conn.execute(check_table_sql)
            table_exists = result.scalar()
            
            if not table_exists:
                print(f"📌 分析表不存在，创建新表...")
                
                # 直接使用内置SQL创建表
                create_sql = f"""
                CREATE TABLE {analysis_table} (
                    id SERIAL PRIMARY KEY,
                    analysis_id VARCHAR(100) NOT NULL,
                    analysis_type VARCHAR(50) DEFAULT 'bbox_overlap',
                    analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    hotspot_rank INTEGER,
                    overlap_count INTEGER,
                    total_overlap_area NUMERIC,
                    subdataset_count INTEGER,
                    scene_count INTEGER,
                    involved_subdatasets TEXT[],
                    involved_scenes TEXT[],
                    analysis_params TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- 添加几何列
                SELECT AddGeometryColumn('public', '{analysis_table}', 'geometry', 4326, 'GEOMETRY', 2);
                
                -- 创建索引
                CREATE INDEX idx_{analysis_table}_analysis_id ON {analysis_table} (analysis_id);
                CREATE INDEX idx_{analysis_table}_rank ON {analysis_table} (hotspot_rank);
                CREATE INDEX idx_{analysis_table}_geom ON {analysis_table} USING GIST (geometry);
                """
                
                conn.execute(text(create_sql))
                conn.commit()
                print(f"✅ 分析表创建成功")
            else:
                print(f"✅ 分析表已存在")
        
        # 生成分析ID
        if not args.analysis_id:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            analysis_id = f"overlap_docker_{timestamp}"
        else:
            analysis_id = args.analysis_id
        
        print(f"\n🚀 开始叠置分析: {analysis_id}")
        
        # 🚨 强制城市过滤机制
        if not args.city:
            print("❌ 错误: 必须指定城市过滤条件以避免性能问题")
            print("💡 使用 --suggest-city 查看推荐的城市")
            print("💡 或使用 --city your_city_name 指定城市")
            print("💡 示例: --city A72")
            return
        
        print(f"🏙️ 城市过滤: {args.city}")
        
        # 构建额外过滤条件
        where_conditions = []
        if args.subdatasets:
            subdataset_list = "', '".join(args.subdatasets)
            where_conditions.append(f"a.subdataset_name IN ('{subdataset_list}') AND b.subdataset_name IN ('{subdataset_list}')")
            print(f"📦 子数据集过滤: {len(args.subdatasets)} 个")
        
        where_clause = "AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        # 🚀 执行网格化分析SQL（默认方法）
        print(f"🔥 使用网格化分析，避免连锁聚合问题...")
        
        # 🎯 新方法：只检查城市范围，不预估网格数量（因为我们按需生成）
        with engine.connect() as conn:
            city_check_sql = text(f"""
                WITH city_bbox AS (
                    SELECT ST_Envelope(ST_Union(geometry)) as city_envelope
                    FROM {view_name} 
                    WHERE city_id = '{args.city}' AND all_good = true
                )
                SELECT 
                    ST_XMax(city_envelope) - ST_XMin(city_envelope) as width_degrees,
                    ST_YMax(city_envelope) - ST_YMin(city_envelope) as height_degrees,
                    COUNT(*) as bbox_count
                FROM city_bbox, {view_name} 
                WHERE city_id = '{args.city}' AND all_good = true
                GROUP BY 1, 2;
            """)
            
            city_check = conn.execute(city_check_sql).fetchone()
            
            print(f"📏 城市范围: {city_check.width_degrees:.4f}° × {city_check.height_degrees:.4f}°")
            print(f"📦 bbox数量: {city_check.bbox_count:,} 个")
            print(f"📊 网格大小: {args.grid_size}° × {args.grid_size}° (约200m×200m)")
            print(f"🎯 分析方法: bbox密度分析")
        
        # 确定密度阈值（兼容旧参数）
        if args.density_threshold:
            density_threshold_sql = f"AND COUNT(*) >= {args.density_threshold}"
            print(f"📊 使用固定阈值: {args.density_threshold}")
        else:
            density_threshold_sql = f"AND COUNT(*) >= (SELECT PERCENTILE_CONT({args.percentile/100.0}) WITHIN GROUP (ORDER BY grid_density) FROM temp_grid_densities)"
            print(f"📊 使用动态阈值: {args.percentile}分位数")
        
        analysis_sql = f"""
            WITH bbox_bounds AS (
                -- 🚀 第1步：提取bbox边界（一次性几何计算）
                SELECT 
                    id,
                    subdataset_name,
                    scene_token,
                    ST_XMin(geometry) as xmin,
                    ST_XMax(geometry) as xmax,
                    ST_YMin(geometry) as ymin,
                    ST_YMax(geometry) as ymax
                FROM {view_name}
                WHERE city_id = '{args.city}' AND all_good = true
                {where_clause.replace('a.', '').replace('b.', '').replace(' AND  AND', ' AND')}
            ),
            bbox_grid_coverage AS (
                -- 🎯 第2步：计算每个bbox覆盖的网格范围（纯数学计算）
                SELECT 
                    id,
                    subdataset_name,
                    scene_token,
                    floor(xmin / {args.grid_size})::int as min_grid_x,
                    floor(xmax / {args.grid_size})::int as max_grid_x,
                    floor(ymin / {args.grid_size})::int as min_grid_y,
                    floor(ymax / {args.grid_size})::int as max_grid_y,
                    CASE 
                        WHEN {not args.calculate_area} THEN 1.0
                        ELSE (xmax - xmin) * (ymax - ymin)  -- bbox面积
                    END as bbox_area
                FROM bbox_bounds
            ),
            expanded_grid_coverage AS (
                -- 🔧 第3步：展开每个bbox到它覆盖的所有网格
                SELECT 
                    id,
                    subdataset_name,
                    scene_token,
                    bbox_area,
                    grid_x,
                    grid_y
                FROM bbox_grid_coverage,
                LATERAL generate_series(min_grid_x, max_grid_x) as grid_x,
                LATERAL generate_series(min_grid_y, max_grid_y) as grid_y
            ),
            all_grid_densities AS (
                -- 📊 第4a步：计算所有网格的密度（用于分位数计算）
                SELECT 
                    grid_x,
                    grid_y,
                    COUNT(*) as grid_density
                FROM expanded_grid_coverage
                GROUP BY grid_x, grid_y
            ),
            temp_grid_densities AS (
                -- 临时表存储密度值用于分位数计算
                SELECT grid_density FROM all_grid_densities
            ),
            high_density_grids AS (
                -- 📊 第4b步：筛选高密度网格
                SELECT 
                    g.grid_x,
                    g.grid_y,
                    g.grid_density as bbox_count_in_grid,
                    COUNT(DISTINCT e.subdataset_name) as subdataset_count,
                    COUNT(DISTINCT e.scene_token) as scene_count,
                    ARRAY_AGG(DISTINCT e.subdataset_name) as involved_subdatasets,
                    ARRAY_AGG(DISTINCT e.scene_token) as involved_scenes,
                    SUM(e.bbox_area) as total_bbox_area,
                    -- 🔧 生成网格几何
                    ST_MakeEnvelope(
                        g.grid_x * {args.grid_size}, 
                        g.grid_y * {args.grid_size},
                        (g.grid_x + 1) * {args.grid_size}, 
                        (g.grid_y + 1) * {args.grid_size}, 
                        4326
                    ) as grid_geom
                FROM all_grid_densities g
                JOIN expanded_grid_coverage e ON (g.grid_x = e.grid_x AND g.grid_y = e.grid_y)
                WHERE g.grid_density >= COALESCE(
                    {args.density_threshold if args.density_threshold else 'NULL'},
                    (SELECT PERCENTILE_CONT({args.percentile/100.0}) WITHIN GROUP (ORDER BY grid_density) FROM temp_grid_densities)
                )
                {'AND SUM(e.bbox_area) >= ' + str(args.min_overlap_area) if args.calculate_area and args.min_overlap_area > 0 else ''}
                GROUP BY g.grid_x, g.grid_y, g.grid_density
            ),
            final_grids AS (
                -- 🔗 第5步：按百分比选择最密集的网格
                SELECT 
                    grid_x,
                    grid_y,
                    bbox_count_in_grid,
                    subdataset_count,
                    scene_count,
                    involved_subdatasets,
                    involved_scenes,
                    total_bbox_area,
                    grid_geom,
                    ROW_NUMBER() OVER (ORDER BY bbox_count_in_grid DESC) as density_rank
                FROM high_density_grids
            ),
            top_percent_grids AS (
                -- 🎯 第6步：计算百分比阈值并限制数量
                SELECT 
                    grid_x,
                    grid_y,
                    bbox_count_in_grid,
                    subdataset_count,
                    scene_count,
                    involved_subdatasets,
                    involved_scenes,
                    total_bbox_area,
                    grid_geom,
                    density_rank
                FROM final_grids
                WHERE density_rank <= GREATEST(
                    ROUND((SELECT COUNT(*) FROM final_grids) * {args.top_percent} / 100.0),
                    1
                )
                AND density_rank <= {args.max_results}
            )
            INSERT INTO {analysis_table} 
            (analysis_id, hotspot_rank, overlap_count, total_overlap_area, 
             subdataset_count, scene_count, involved_subdatasets, involved_scenes, geometry, analysis_params)
            SELECT 
                '{analysis_id}' as analysis_id,
                density_rank as hotspot_rank,
                bbox_count_in_grid as overlap_count,
                total_bbox_area as total_overlap_area,
                subdataset_count,
                scene_count,
                involved_subdatasets,
                involved_scenes,
                grid_geom as geometry,
                json_build_object(
                    'analysis_type', 'top_percent_grids',
                    'city_filter', '{args.city}',
                    'grid_size', {args.grid_size},
                    'percentile_threshold', {args.percentile if not args.density_threshold else 'null'},
                    'fixed_threshold', {args.density_threshold if args.density_threshold else 'null'},
                    'top_percent', {args.top_percent},
                    'max_results', {args.max_results},
                    'calculate_area', {str(args.calculate_area).lower()},
                    'min_overlap_area', {args.min_overlap_area if args.calculate_area else 'null'}
                )::text as analysis_params
            FROM top_percent_grids
            ORDER BY density_rank;
            """
        
        print(f"⚡ 执行bbox密度分析SQL...")
        print(f"💡 可以使用 Ctrl+C 安全退出")
        
        analysis_start_time = datetime.now()
        check_shutdown()  # 执行前检查
        
        with engine.connect() as conn:
            current_connection = conn  # 保存连接引用
            
            print(f"🚀 开始执行SQL... ({analysis_start_time.strftime('%H:%M:%S')})")
            sql_start_time = datetime.now()
            
            conn.execute(text(analysis_sql))
            check_shutdown()  # SQL执行后检查
            
            sql_end_time = datetime.now()
            sql_duration = (sql_end_time - sql_start_time).total_seconds()
            
            conn.commit()
            commit_time = datetime.now()
            commit_duration = (commit_time - sql_end_time).total_seconds()
            
            print(f"✅ SQL执行完成，耗时: {sql_duration:.2f}秒")
            print(f"✅ 提交完成，耗时: {commit_duration:.2f}秒")
            print(f"🔍 正在统计结果...")
            current_connection = None  # 清除连接引用
            
            # 获取结果统计
            count_sql = text(f"SELECT COUNT(*) FROM {analysis_table} WHERE analysis_id = '{analysis_id}';")
            count_result = conn.execute(count_sql)
            inserted_count = count_result.scalar()
            
            # 计算总耗时和性能统计
            total_duration = (commit_time - analysis_start_time).total_seconds()
            
            print(f"✅ 网格密度分析完成，发现 {inserted_count} 个高密度网格")
            print(f"⏱️ 总耗时: {total_duration:.2f}秒 (SQL: {sql_duration:.2f}s + 提交: {commit_duration:.2f}s)")
            
            # 性能统计
            bbox_count = city_check.bbox_count if 'city_check' in locals() else 0
            if bbox_count > 0:
                bbox_per_sec = bbox_count / max(sql_duration, 0.001)  # 避免除零
                print(f"📊 处理速度: {bbox_per_sec:,.0f} bbox/秒")
                
            # 显示阈值信息
            if not args.density_threshold:
                threshold_info_sql = text(f"""
                    SELECT PERCENTILE_CONT({args.percentile/100.0}) WITHIN GROUP (ORDER BY grid_density) as threshold
                    FROM (
                        SELECT COUNT(*) as grid_density
                        FROM (
                            SELECT grid_x, grid_y
                            FROM (
                                SELECT 
                                    floor(ST_XMin(geometry) / {args.grid_size})::int as grid_x,
                                    floor(ST_YMin(geometry) / {args.grid_size})::int as grid_y
                                FROM {view_name}
                                WHERE city_id = '{args.city}' AND all_good = true
                                {where_clause.replace('a.', '').replace('b.', '').replace(' AND  AND', ' AND')}
                            ) g1
                        ) g2
                        GROUP BY grid_x, grid_y
                    ) densities;
                """)
                try:
                    threshold_result = conn.execute(threshold_info_sql).scalar()
                    if threshold_result:
                        print(f"📊 计算的动态阈值: {threshold_result:.1f} bbox/网格 ({args.percentile}分位数)")
                except Exception:
                    pass  # 如果查询失败，跳过阈值显示
            
            if inserted_count > 0:
                # 显示TOP结果
                summary_sql = text(f"""
                    SELECT 
                        hotspot_rank as region_rank,
                        overlap_count as total_bbox_count,
                        ROUND(total_overlap_area::numeric, 4) as region_area,
                        subdataset_count,
                        scene_count,
                        CAST(analysis_params::json->>'grid_count' AS INTEGER) as grid_count,
                        ROUND(CAST(analysis_params::json->>'avg_grid_density' AS NUMERIC), 1) as avg_density
                    FROM {analysis_table}
                    WHERE analysis_id = '{analysis_id}'
                    ORDER BY hotspot_rank
                    LIMIT 10;
                """)
                
                result_df = pd.read_sql(summary_sql, engine)
                print(f"\n📊 TOP {inserted_count} 高密度网格:")
                print(result_df.to_string(index=False))
                
                # 创建QGIS视图
                print(f"\n🎨 创建QGIS视图...")
                qgis_view = "qgis_bbox_overlap_hotspots"
                
                # 先删除旧视图，避免列名冲突
                drop_view_sql = f"DROP VIEW IF EXISTS {qgis_view} CASCADE;"
                conn.execute(text(drop_view_sql))
                
                view_sql = f"""
                CREATE VIEW {qgis_view} AS
                SELECT 
                    id,
                    analysis_id,
                    hotspot_rank,
                    overlap_count,
                    total_overlap_area,
                    subdataset_count,
                    scene_count,
                    involved_subdatasets,
                    involved_scenes,
                    CASE 
                        WHEN overlap_count >= 10 THEN 'High Density'
                        WHEN overlap_count >= 5 THEN 'Medium Density'
                        ELSE 'Low Density'
                    END as density_level,
                    geometry,
                    created_at
                FROM {analysis_table}
                WHERE analysis_type = 'bbox_overlap'
                ORDER BY hotspot_rank;
                """
                
                conn.execute(text(view_sql))
                conn.commit()
                print(f"✅ QGIS视图 {qgis_view} 创建成功")
                
                # 输出QGIS连接信息
                print(f"\n🎯 QGIS可视化指导")
                print(f"=" * 40)
                print(f"📋 数据库连接信息:")
                print(f"   host: local_pg")
                print(f"   port: 5432") 
                print(f"   database: postgres")
                print(f"   username: postgres")
                print(f"")
                print(f"📊 推荐加载的图层:")
                print(f"   1. {view_name} - 所有bbox数据（底图）")
                print(f"   2. {qgis_view} - 重叠热点区域")
                print(f"")
                print(f"🎨 可视化建议:")
                print(f"   • 主键: id")
                print(f"   • 几何列: geometry")
                print(f"   • 按 density_level 字段设置颜色")
                print(f"   • 显示 overlap_count 标签")
                print(f"   • 使用 analysis_id = '{analysis_id}' 过滤")
                
                print(f"\n🔥 智能区域分析特别提示:")
                print(f"   • 每个区域由多个相邻的高密度网格组成")
                print(f"   • overlap_count = 该区域内的总bbox数量")
                
                if args.density_threshold:
                    print(f"   • 使用固定阈值: >= {args.density_threshold} bbox/网格")
                else:
                    print(f"   • 使用动态阈值: {args.percentile}分位数")
                    
                print(f"   • 最小区域大小: >= {args.min_cluster_size} 个网格")
                print(f"   • 区域合并方法: {args.cluster_method}")
                if args.calculate_area and args.min_overlap_area > 0:
                    print(f"   • 面积阈值: >= {args.min_overlap_area} 平方度")
                print(f"   • 🎯 这是连通区域分析，识别有意义的空间聚集")
                print(f"   • 建议使用填充样式 + 边界线 + 透明度 60%")
                print(f"   • 可以叠加原始bbox数据对比查看")
                print(f"   • 区域内的网格信息存储在analysis_params中")
                
            else:
                print(f"⚠️ 未发现连通密集区域，建议:")
                if not args.density_threshold:
                    print(f"   • 降低分位数阈值: --percentile 75")
                else:
                    print(f"   • 降低固定阈值: --density-threshold 3")
                if args.min_cluster_size > 1:
                    print(f"   • 减小最小区域大小: --min-cluster-size 1")
                print(f"   • 增大网格尺寸: --grid-size 0.005")
                if args.calculate_area and args.min_overlap_area > 0:
                    print(f"   • 降低面积阈值: --min-overlap-area 0")
                print(f"   • 检查数据是否在同一区域")
                print(f"   • 尝试不同的城市过滤条件")
        
        print(f"\n✅ 分析完成！分析ID: {analysis_id}")
        
    except Exception as e:
        print(f"\n❌ 分析失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
