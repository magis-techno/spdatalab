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
        
        # 尝试导入分析器
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
        parser = argparse.ArgumentParser(description='Docker兼容的BBox叠置分析')
        parser.add_argument('--city', help='城市过滤')
        parser.add_argument('--subdatasets', nargs='+', help='子数据集过滤')
        parser.add_argument('--min-overlap-area', type=float, default=0.0001, help='最小重叠面积阈值')
        parser.add_argument('--top-n', type=int, default=15, help='返回的热点数量')
        parser.add_argument('--analysis-id', help='自定义分析ID')
        parser.add_argument('--refresh-view', action='store_true', help='强制刷新统一视图')
        parser.add_argument('--test-only', action='store_true', help='只运行测试，不执行分析')
        parser.add_argument('--suggest-city', action='store_true', help='显示城市分析建议并退出')
        parser.add_argument('--estimate-time', action='store_true', help='估算分析时间并退出')
        parser.add_argument('--debug', action='store_true', help='开启调试模式，显示详细分析信息')
        parser.add_argument('--intersect-only', action='store_true', help='仅检测相交（忽略面积阈值）')
        parser.add_argument('--sample-check', type=int, default=0, help='调试采样数量（配合--debug使用）')
        
        args = parser.parse_args()
        
        print(f"\n📋 分析参数:")
        print(f"   城市过滤: {args.city}")
        print(f"   最小重叠面积: {args.min_overlap_area}")
        print(f"   返回数量: {args.top_n}")
        print(f"   强制刷新视图: {args.refresh_view}")
        if args.debug:
            print(f"   调试模式: 开启")
            if args.sample_check > 0:
                print(f"   调试采样: {args.sample_check}")
        if args.intersect_only:
            print(f"   相交模式: 忽略面积阈值，只要相交就算重叠")
        
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
        
        # 检查统一视图
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
            
            # 跳过耗时的COUNT查询，直接开始分析
            print(f"📊 统一视图已就绪，开始分析...")
            
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
        
        # 构建过滤条件
        where_conditions = []
        if args.city:
            where_conditions.append(f"a.city_id = '{args.city}' AND b.city_id = '{args.city}'")
        
        if args.subdatasets:
            subdataset_list = "', '".join(args.subdatasets)
            where_conditions.append(f"a.subdataset_name IN ('{subdataset_list}') AND b.subdataset_name IN ('{subdataset_list}')")
        
        where_clause = "AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        # 调试模式：显示数据统计信息
        if args.debug:
            print(f"\n🔍 调试模式：分析数据分布")
            print("-" * 40)
            
            debug_sql = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(*) FILTER (WHERE all_good = true) as good_count,
                COUNT(DISTINCT city_id) as city_count,
                COUNT(DISTINCT subdataset_name) as subdataset_count,
                ROUND(AVG(ST_Area(geometry))::numeric, 10) as avg_area,
                ROUND(MIN(ST_Area(geometry))::numeric, 10) as min_area,
                ROUND(MAX(ST_Area(geometry))::numeric, 10) as max_area
            FROM {view_name}
            WHERE city_id IS NOT NULL
            {f"AND city_id = '{args.city}'" if args.city else ""};
            """
            
            debug_result = conn.execute(text(debug_sql)).fetchone()
            print(f"📊 数据统计:")
            print(f"   总数量: {debug_result.total_count:,}")
            print(f"   质量良好: {debug_result.good_count:,}")
            print(f"   城市数: {debug_result.city_count}")
            print(f"   子数据集数: {debug_result.subdataset_count}")
            print(f"   平均面积: {debug_result.avg_area}")
            print(f"   面积范围: {debug_result.min_area} ~ {debug_result.max_area}")
            
            # 采样检查
            if args.sample_check > 0:
                print(f"\n🎲 随机采样 ({args.sample_check} 个):")
                sample_sql = f"""
                SELECT 
                    qgis_id,
                    subdataset_name,
                    scene_token,
                    ROUND(ST_Area(geometry)::numeric, 10) as area,
                    ST_AsText(ST_Centroid(geometry)) as centroid
                FROM {view_name}
                WHERE all_good = true
                {f"AND city_id = '{args.city}'" if args.city else ""}
                ORDER BY RANDOM()
                LIMIT {args.sample_check};
                """
                
                sample_results = conn.execute(text(sample_sql)).fetchall()
                for i, row in enumerate(sample_results, 1):
                    print(f"   {i}. ID:{row.qgis_id} 面积:{row.area} 中心:{row.centroid}")
        
        # 根据模式决定面积条件
        if args.intersect_only:
            area_condition = "-- 相交模式：忽略面积阈值，只要相交就算重叠"
            print(f"🔍 使用相交模式：只要几何体相交就算重叠（忽略面积阈值）")
        else:
            area_condition = f"AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {args.min_overlap_area}"
            print(f"📏 使用面积模式：重叠面积必须大于 {args.min_overlap_area}")
        
        # 执行分析
        analysis_sql = f"""
        WITH overlapping_pairs AS (
            SELECT 
                a.qgis_id as bbox_a_id,
                b.qgis_id as bbox_b_id,
                a.subdataset_name as subdataset_a,
                b.subdataset_name as subdataset_b,
                a.scene_token as scene_a,
                b.scene_token as scene_b,
                ST_Intersection(a.geometry, b.geometry) as overlap_geometry,
                ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
            FROM {view_name} a
            JOIN {view_name} b ON a.qgis_id < b.qgis_id
            WHERE ST_Intersects(a.geometry, b.geometry)
            {area_condition}
            AND NOT ST_Equals(a.geometry, b.geometry)
            -- 🎯 只分析相同城市的bbox（性能和逻辑优化）
            AND a.city_id = b.city_id
            AND a.city_id IS NOT NULL
            -- 🎯 只分析质量合格的数据（all_good=true）
            AND a.all_good = true
            AND b.all_good = true
            {where_clause}
        ),
        -- 🔧 修复：使用真正的空间连通性聚类
        overlap_clusters AS (
            SELECT 
                overlap_geometry,
                overlap_area,
                subdataset_a,
                subdataset_b,
                scene_a,
                scene_b,
                -- 使用 ST_ClusterDBSCAN 进行空间聚类
                -- eps=0 表示只有直接相交的几何体才归为一组
                -- minpoints=1 表示单个重叠也可以形成热点
                ST_ClusterDBSCAN(overlap_geometry, eps := 0, minpoints := 1) OVER() as cluster_id
            FROM overlapping_pairs
        ),
        overlap_hotspots AS (
            SELECT 
                cluster_id,
                -- 对每个聚类，合并所有重叠区域
                ST_Union(overlap_geometry) as hotspot_geometry,
                COUNT(*) as overlap_count,
                ARRAY_AGG(DISTINCT subdataset_a) || ARRAY_AGG(DISTINCT subdataset_b) as involved_subdatasets,
                ARRAY_AGG(DISTINCT scene_a) || ARRAY_AGG(DISTINCT scene_b) as involved_scenes,
                SUM(overlap_area) as total_overlap_area
            FROM overlap_clusters
            WHERE cluster_id IS NOT NULL  -- 排除噪声点
            GROUP BY cluster_id
            HAVING COUNT(*) >= 1  -- 至少包含一个重叠区域
        )
        INSERT INTO {analysis_table} 
        (analysis_id, hotspot_rank, overlap_count, total_overlap_area, 
         subdataset_count, scene_count, involved_subdatasets, involved_scenes, geometry, analysis_params)
        SELECT 
            '{analysis_id}' as analysis_id,
            ROW_NUMBER() OVER (ORDER BY overlap_count DESC) as hotspot_rank,
            overlap_count,
            total_overlap_area,
            ARRAY_LENGTH(involved_subdatasets, 1) as subdataset_count,
            ARRAY_LENGTH(involved_scenes, 1) as scene_count,
            involved_subdatasets,
            involved_scenes,
            hotspot_geometry as geometry,
            '{{"city_filter": "{args.city}", "min_overlap_area": {args.min_overlap_area}, "top_n": {args.top_n}}}' as analysis_params
        FROM overlap_hotspots
        ORDER BY overlap_count DESC
        LIMIT {args.top_n};
        """
        
        print(f"⚡ 执行空间叠置分析SQL...")
        print(f"💡 可以使用 Ctrl+C 安全退出")
        analysis_start_time = datetime.now()
        check_shutdown()  # 执行前检查
        
        with engine.connect() as conn:
            current_connection = conn  # 保存连接引用
            
            conn.execute(text(analysis_sql))
            check_shutdown()  # SQL执行后检查
            
            conn.commit()
            print(f"✅ SQL执行完成，正在统计结果...")
            current_connection = None  # 清除连接引用
            
            # 获取结果统计
            count_sql = text(f"SELECT COUNT(*) FROM {analysis_table} WHERE analysis_id = '{analysis_id}';")
            count_result = conn.execute(count_sql)
            inserted_count = count_result.scalar()
            
            print(f"✅ 叠置分析完成，发现 {inserted_count} 个重叠热点")
            
            if inserted_count > 0:
                # 显示TOP结果
                summary_sql = text(f"""
                    SELECT 
                        hotspot_rank,
                        overlap_count,
                        ROUND(total_overlap_area::numeric, 4) as total_overlap_area,
                        subdataset_count,
                        scene_count
                    FROM {analysis_table}
                    WHERE analysis_id = '{analysis_id}'
                    ORDER BY hotspot_rank
                    LIMIT 5;
                """)
                
                result_df = pd.read_sql(summary_sql, engine)
                print(f"\n📊 TOP 5 重叠热点:")
                print(result_df.to_string(index=False))
                
                # 创建QGIS视图
                print(f"\n🎨 创建QGIS视图...")
                qgis_view = "qgis_bbox_overlap_hotspots"
                
                view_sql = f"""
                CREATE OR REPLACE VIEW {qgis_view} AS
                SELECT 
                    id as qgis_id,
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
                print(f"   • 主键: qgis_id")
                print(f"   • 几何列: geometry")
                print(f"   • 按 density_level 字段设置颜色")
                print(f"   • 显示 overlap_count 标签")
                print(f"   • 使用 analysis_id = '{analysis_id}' 过滤")
                
            else:
                print(f"⚠️ 未发现重叠热点，建议:")
                print(f"   • 降低 --min-overlap-area 阈值")
                print(f"   • 检查数据是否在同一区域")
                print(f"   • 尝试不同的城市过滤条件")
        
        print(f"\n✅ 分析完成！分析ID: {analysis_id}")
        
    except Exception as e:
        print(f"\n❌ 分析失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
