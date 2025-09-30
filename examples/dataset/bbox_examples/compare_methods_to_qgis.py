#!/usr/bin/env python3
"""
多方法热点分析对比 - QGIS可视化版本
=====================================

将网格、DBSCAN、分层聚类三种方法的结果都输出到数据库表，
便于在QGIS中对比可视化

使用方法：
    python examples/dataset/bbox_examples/compare_methods_to_qgis.py --city A263
"""

import sys
import time
from pathlib import Path
import argparse
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from sqlalchemy import create_engine, text
import pandas as pd

def create_comparison_table(engine):
    """创建方法对比结果表"""
    
    create_table_sql = text("""
        -- 创建方法对比结果表
        CREATE TABLE IF NOT EXISTS hotspot_method_comparison (
            id SERIAL PRIMARY KEY,
            analysis_id VARCHAR(100) NOT NULL,
            city_id VARCHAR(50) NOT NULL,
            method_name VARCHAR(50) NOT NULL,  -- 'grid', 'dbscan', 'hierarchical'
            hotspot_rank INTEGER,
            hotspot_count INTEGER,             -- 该热点包含的bbox数量
            hotspot_area NUMERIC,              -- 热点覆盖面积
            method_params TEXT,                -- JSON格式的方法参数
            analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- 添加几何列
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'hotspot_method_comparison' 
                AND column_name = 'geometry'
            ) THEN
                PERFORM AddGeometryColumn('public', 'hotspot_method_comparison', 'geometry', 4326, 'GEOMETRY', 2);
            END IF;
        END $$;
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_hotspot_comparison_analysis ON hotspot_method_comparison (analysis_id);
        CREATE INDEX IF NOT EXISTS idx_hotspot_comparison_method ON hotspot_method_comparison (method_name);
        CREATE INDEX IF NOT EXISTS idx_hotspot_comparison_geom ON hotspot_method_comparison USING GIST (geometry);
    """)
    
    with engine.connect() as conn:
        conn.execute(create_table_sql)
        conn.commit()
        print("✅ 对比结果表创建成功")

def run_grid_analysis(engine, city_id, analysis_id):
    """运行网格密度分析"""
    print("🔲 运行网格密度分析...")
    
    start_time = time.time()
    
    grid_sql = text(f"""
        WITH grid_density AS (
            SELECT 
                floor(ST_X(ST_Centroid(geometry)) / 0.002)::int as grid_x,
                floor(ST_Y(ST_Centroid(geometry)) / 0.002)::int as grid_y,
                COUNT(*) as bbox_count,
                ST_MakeEnvelope(
                    floor(ST_X(ST_Centroid(geometry)) / 0.002) * 0.002,
                    floor(ST_Y(ST_Centroid(geometry)) / 0.002) * 0.002,
                    (floor(ST_X(ST_Centroid(geometry)) / 0.002) + 1) * 0.002,
                    (floor(ST_Y(ST_Centroid(geometry)) / 0.002) + 1) * 0.002,
                    4326
                ) as grid_geom
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
            GROUP BY grid_x, grid_y
            HAVING COUNT(*) >= 5
        )
        INSERT INTO hotspot_method_comparison 
        (analysis_id, city_id, method_name, hotspot_rank, hotspot_count, hotspot_area, geometry, method_params)
        SELECT 
            '{analysis_id}' as analysis_id,
            '{city_id}' as city_id,
            'grid' as method_name,
            ROW_NUMBER() OVER (ORDER BY bbox_count DESC) as hotspot_rank,
            bbox_count as hotspot_count,
            ST_Area(grid_geom) as hotspot_area,
            grid_geom as geometry,
            '{{"grid_size": 0.002, "min_density": 5}}' as method_params
        FROM grid_density
        ORDER BY bbox_count DESC;
    """)
    
    with engine.connect() as conn:
        conn.execute(grid_sql)
        conn.commit()
    
    grid_time = time.time() - start_time
    print(f"   ✅ 网格分析完成，耗时: {grid_time:.2f}秒")
    return grid_time

def run_dbscan_analysis(engine, city_id, analysis_id):
    """运行DBSCAN聚类分析"""
    print("🎯 运行DBSCAN聚类分析...")
    
    start_time = time.time()
    
    dbscan_sql = text(f"""
        WITH dbscan_clusters AS (
            SELECT 
                id,
                geometry,
                ST_ClusterDBSCAN(geometry, 0.002, 5) OVER() as cluster_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        cluster_stats AS (
            SELECT 
                cluster_id,
                COUNT(*) as bbox_count,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_geom,
                ST_Area(ST_ConvexHull(ST_Collect(geometry))) as cluster_area
            FROM dbscan_clusters 
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
        )
        INSERT INTO hotspot_method_comparison 
        (analysis_id, city_id, method_name, hotspot_rank, hotspot_count, hotspot_area, geometry, method_params)
        SELECT 
            '{analysis_id}' as analysis_id,
            '{city_id}' as city_id,
            'dbscan' as method_name,
            ROW_NUMBER() OVER (ORDER BY bbox_count DESC) as hotspot_rank,
            bbox_count as hotspot_count,
            cluster_area as hotspot_area,
            cluster_geom as geometry,
            '{{"eps": 0.002, "min_samples": 5}}' as method_params
        FROM cluster_stats
        ORDER BY bbox_count DESC;
    """)
    
    with engine.connect() as conn:
        conn.execute(dbscan_sql)
        conn.commit()
    
    dbscan_time = time.time() - start_time
    print(f"   ✅ DBSCAN分析完成，耗时: {dbscan_time:.2f}秒")
    return dbscan_time

def run_hierarchical_analysis(engine, city_id, analysis_id):
    """运行分层聚类分析"""
    print("🏗️ 运行分层聚类分析...")
    
    start_time = time.time()
    
    hierarchical_sql = text(f"""
        WITH coarse_clusters AS (
            SELECT 
                id,
                geometry,
                ST_ClusterDBSCAN(geometry, 0.01, 20) OVER() as coarse_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        fine_clusters AS (
            SELECT 
                id,
                geometry,
                coarse_id,
                ST_ClusterDBSCAN(geometry, 0.002, 5) OVER(PARTITION BY coarse_id) as fine_id
            FROM coarse_clusters 
            WHERE coarse_id IS NOT NULL
        ),
        final_clusters AS (
            SELECT 
                coarse_id,
                fine_id,
                COUNT(*) as bbox_count,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_geom,
                ST_Area(ST_ConvexHull(ST_Collect(geometry))) as cluster_area
            FROM fine_clusters
            WHERE fine_id IS NOT NULL
            GROUP BY coarse_id, fine_id
        )
        INSERT INTO hotspot_method_comparison 
        (analysis_id, city_id, method_name, hotspot_rank, hotspot_count, hotspot_area, geometry, method_params)
        SELECT 
            '{analysis_id}' as analysis_id,
            '{city_id}' as city_id,
            'hierarchical' as method_name,
            ROW_NUMBER() OVER (ORDER BY bbox_count DESC) as hotspot_rank,
            bbox_count as hotspot_count,
            cluster_area as hotspot_area,
            cluster_geom as geometry,
            '{{"coarse_eps": 0.01, "coarse_min_samples": 20, "fine_eps": 0.002, "fine_min_samples": 5}}' as method_params
        FROM final_clusters
        ORDER BY bbox_count DESC;
    """)
    
    with engine.connect() as conn:
        conn.execute(hierarchical_sql)
        conn.commit()
    
    hierarchical_time = time.time() - start_time
    print(f"   ✅ 分层分析完成，耗时: {hierarchical_time:.2f}秒")
    return hierarchical_time

def create_qgis_views(engine, analysis_id):
    """创建QGIS可视化视图"""
    print("🎨 创建QGIS可视化视图...")
    
    # 主对比视图
    main_view_sql = text(f"""
        CREATE OR REPLACE VIEW qgis_hotspot_method_comparison AS
        SELECT 
            id,
            analysis_id,
            city_id,
            method_name,
            hotspot_rank,
            hotspot_count,
            ROUND(hotspot_area::numeric, 6) as hotspot_area,
            
            -- 方法颜色编码
            CASE method_name
                WHEN 'grid' THEN '#FF6B6B'        -- 红色
                WHEN 'dbscan' THEN '#4ECDC4'      -- 青色  
                WHEN 'hierarchical' THEN '#45B7D1' -- 蓝色
            END as method_color,
            
            -- 密度等级
            CASE 
                WHEN hotspot_count >= 50 THEN 'Very High'
                WHEN hotspot_count >= 20 THEN 'High'
                WHEN hotspot_count >= 10 THEN 'Medium'
                ELSE 'Low'
            END as density_level,
            
            -- 显示标签
            method_name || '_' || hotspot_rank || ' (' || hotspot_count || ')' as display_label,
            
            geometry,
            method_params,
            created_at
        FROM hotspot_method_comparison
        WHERE analysis_id = '{analysis_id}'
        ORDER BY method_name, hotspot_rank;
    """)
    
    # 方法统计视图
    stats_view_sql = text(f"""
        CREATE OR REPLACE VIEW qgis_method_statistics AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY method_name) as id,
            analysis_id,
            city_id,
            method_name,
            COUNT(*) as total_hotspots,
            MAX(hotspot_count) as max_density,
            ROUND(AVG(hotspot_count)::numeric, 2) as avg_density,
            SUM(hotspot_area) as total_coverage_area,
            
            -- 创建方法统计的代表性几何（所有热点的外包络）
            ST_ConvexHull(ST_Collect(geometry)) as method_boundary,
            
            MIN(created_at) as analysis_time
        FROM hotspot_method_comparison
        WHERE analysis_id = '{analysis_id}'
        GROUP BY analysis_id, city_id, method_name;
    """)
    
    with engine.connect() as conn:
        conn.execute(main_view_sql)
        conn.execute(stats_view_sql)
        conn.commit()
    
    print("✅ QGIS视图创建成功")

def generate_comparison_report(engine, analysis_id):
    """生成对比分析报告"""
    print("\n📊 生成对比分析报告...")
    
    # 获取统计数据
    stats_sql = text(f"""
        SELECT 
            method_name,
            COUNT(*) as hotspot_count,
            MAX(hotspot_count) as max_density,
            ROUND(AVG(hotspot_count)::numeric, 2) as avg_density,
            ROUND(SUM(hotspot_area)::numeric, 6) as total_area
        FROM hotspot_method_comparison
        WHERE analysis_id = '{analysis_id}'
        GROUP BY method_name
        ORDER BY method_name;
    """)
    
    with engine.connect() as conn:
        stats_df = pd.read_sql(stats_sql, conn)
    
    print("\n📈 方法对比统计:")
    print("=" * 60)
    for _, row in stats_df.iterrows():
        print(f"{row['method_name'].upper():>12}: {row['hotspot_count']:>3}个热点, "
              f"最高密度{row['max_density']:>3}, 平均密度{row['avg_density']:>5}")
    
    # 重叠分析
    overlap_sql = text(f"""
        WITH method_pairs AS (
            SELECT 
                a.method_name as method_a,
                b.method_name as method_b,
                COUNT(*) as overlap_count,
                ROUND(AVG(ST_Area(ST_Intersection(a.geometry, b.geometry)))::numeric, 6) as avg_overlap_area
            FROM hotspot_method_comparison a
            JOIN hotspot_method_comparison b ON a.analysis_id = b.analysis_id
            WHERE a.analysis_id = '{analysis_id}'
            AND a.method_name < b.method_name  -- 避免重复比较
            AND ST_Intersects(a.geometry, b.geometry)
            GROUP BY a.method_name, b.method_name
        )
        SELECT * FROM method_pairs;
    """)
    
    with engine.connect() as conn:
        overlap_df = pd.read_sql(overlap_sql, conn)
    
    if not overlap_df.empty:
        print(f"\n🔄 方法间重叠分析:")
        print("-" * 40)
        for _, row in overlap_df.iterrows():
            print(f"{row['method_a']} ↔ {row['method_b']}: {row['overlap_count']}个重叠热点")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='多方法热点分析对比 - QGIS版本')
    parser.add_argument('--city', default='A263', help='分析城市ID')
    
    args = parser.parse_args()
    
    print("🎯 多方法热点分析对比")
    print("=" * 50)
    print(f"分析城市: {args.city}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    engine = create_engine(LOCAL_DSN, future=True)
    analysis_id = f"method_comparison_{args.city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # 1. 创建对比表
        create_comparison_table(engine)
        
        # 2. 清理旧数据
        with engine.connect() as conn:
            cleanup_sql = text(f"DELETE FROM hotspot_method_comparison WHERE city_id = '{args.city}';")
            conn.execute(cleanup_sql)
            conn.commit()
        
        # 3. 运行三种分析方法
        total_start = time.time()
        
        grid_time = run_grid_analysis(engine, args.city, analysis_id)
        dbscan_time = run_dbscan_analysis(engine, args.city, analysis_id)
        hierarchical_time = run_hierarchical_analysis(engine, args.city, analysis_id)
        
        total_time = time.time() - total_start
        
        # 4. 创建QGIS视图
        create_qgis_views(engine, analysis_id)
        
        # 5. 生成对比报告
        generate_comparison_report(engine, analysis_id)
        
        # 6. 输出QGIS使用指南
        print(f"\n🎨 QGIS可视化指南")
        print("=" * 50)
        print(f"📋 数据库连接信息:")
        print(f"   Host: local_pg")
        print(f"   Port: 5432")
        print(f"   Database: postgres")
        print(f"   Username: postgres")
        print(f"")
        print(f"📊 推荐加载的图层:")
        print(f"   1. qgis_hotspot_method_comparison - 主对比视图")
        print(f"   2. qgis_method_statistics - 方法统计视图")
        print(f"   3. clips_bbox_unified - 原始bbox数据（底图）")
        print(f"")
        print(f"🎨 可视化建议:")
        print(f"   • 主键: id")
        print(f"   • 几何列: geometry")
        print(f"   • 按 method_name 字段分类显示")
        print(f"   • 按 method_color 字段设置颜色")
        print(f"   • 显示 display_label 标签")
        print(f"   • 使用 analysis_id = '{analysis_id}' 过滤")
        print(f"")
        print(f"⏱️ 性能总结:")
        print(f"   网格方法:   {grid_time:.2f}秒")
        print(f"   DBSCAN:     {dbscan_time:.2f}秒") 
        print(f"   分层聚类:   {hierarchical_time:.2f}秒")
        print(f"   总耗时:     {total_time:.2f}秒")
        
        print(f"\n✅ 分析完成！分析ID: {analysis_id}")
        
    except Exception as e:
        print(f"\n❌ 分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
