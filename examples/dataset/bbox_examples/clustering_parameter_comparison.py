#!/usr/bin/env python3
"""
聚类参数对比脚本
===============

测试不同eps参数对DBSCAN和分层聚类效果的影响
固定min_samples=10，对比多个eps值

使用方法：
    python examples/dataset/bbox_examples/clustering_parameter_comparison.py --city A263
"""

import sys
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

def test_eps_parameters(city_id='A263'):
    """测试不同eps参数的聚类效果"""
    
    print("🔍 聚类参数对比测试")
    print("=" * 50)
    print(f"测试城市: {city_id}")
    print(f"固定参数: min_samples = 10")
    print(f"测试参数: eps = [0.0005, 0.001, 0.0015, 0.002, 0.003, 0.005]")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    # 测试的eps参数列表（从细到粗）
    eps_values = [0.0005, 0.001, 0.0015, 0.002, 0.003, 0.005]
    min_samples = 10
    
    results = []
    
    with engine.connect() as conn:
        
        # 创建参数对比结果表
        create_comparison_table(conn)
        
        # 清理旧结果
        cleanup_sql = text(f"""
            DELETE FROM clustering_parameter_comparison 
            WHERE city_id = '{city_id}' 
            AND test_timestamp < NOW() - INTERVAL '1 hour';
        """)
        conn.execute(cleanup_sql)
        
        print(f"\n📊 开始参数测试...")
        
        for i, eps in enumerate(eps_values):
            print(f"\n🔬 测试 {i+1}/{len(eps_values)}: eps={eps}, min_samples={min_samples}")
            
            # 测试DBSCAN
            dbscan_result = test_dbscan_params(conn, city_id, eps, min_samples)
            
            # 测试分层聚类  
            hierarchical_result = test_hierarchical_params(conn, city_id, eps, min_samples)
            
            results.append({
                'eps': eps,
                'dbscan': dbscan_result,
                'hierarchical': hierarchical_result
            })
            
            print(f"   DBSCAN: {dbscan_result['cluster_count']} 聚类, {dbscan_result['noise_rate']:.1f}% 噪声")
            print(f"   分层聚类: {hierarchical_result['cluster_count']} 聚类, 最大簇{hierarchical_result['max_cluster_size']}")
        
        # 创建QGIS可视化视图
        create_parameter_comparison_views(conn, city_id)
        
    return results

def create_comparison_table(conn):
    """创建参数对比结果表"""
    create_sql = text("""
        CREATE TABLE IF NOT EXISTS clustering_parameter_comparison (
            id SERIAL PRIMARY KEY,
            city_id VARCHAR(50),
            method_name VARCHAR(50),
            eps_value FLOAT,
            min_samples INTEGER,
            cluster_id INTEGER,
            bbox_count INTEGER,
            cluster_rank INTEGER,
            noise_points INTEGER,
            test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- 添加几何列
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'clustering_parameter_comparison' 
                AND column_name = 'geometry'
            ) THEN
                PERFORM AddGeometryColumn('public', 'clustering_parameter_comparison', 'geometry', 4326, 'GEOMETRY', 2);
            END IF;
        END $$;
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_param_comparison_city_method_eps 
        ON clustering_parameter_comparison (city_id, method_name, eps_value);
        CREATE INDEX IF NOT EXISTS idx_param_comparison_geom 
        ON clustering_parameter_comparison USING GIST (geometry);
    """)
    
    conn.execute(create_sql)
    conn.commit()

def test_dbscan_params(conn, city_id, eps, min_samples):
    """测试DBSCAN参数"""
    
    dbscan_sql = text(f"""
        WITH dbscan_clusters AS (
            SELECT 
                id,
                geometry,
                ST_ClusterDBSCAN(geometry, {eps}, {min_samples}) OVER() as cluster_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        cluster_stats AS (
            SELECT 
                cluster_id,
                COUNT(*) as bbox_count,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_boundary,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as cluster_rank
            FROM dbscan_clusters 
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
        ),
        summary_stats AS (
            SELECT 
                COUNT(DISTINCT cluster_id) FILTER (WHERE cluster_id IS NOT NULL) as cluster_count,
                COUNT(*) FILTER (WHERE cluster_id IS NULL) as noise_points,
                COUNT(*) as total_points,
                MAX(bbox_count) as max_cluster_size,
                ROUND(AVG(bbox_count)::numeric, 2) as avg_cluster_size
            FROM dbscan_clusters
            LEFT JOIN cluster_stats USING (cluster_id)
        )
        -- 保存聚类结果
        INSERT INTO clustering_parameter_comparison 
        (city_id, method_name, eps_value, min_samples, cluster_id, bbox_count, cluster_rank, noise_points, geometry)
        SELECT 
            '{city_id}',
            'DBSCAN',
            {eps},
            {min_samples},
            cluster_id,
            bbox_count,
            cluster_rank,
            (SELECT noise_points FROM summary_stats),
            cluster_boundary
        FROM cluster_stats
        WHERE bbox_count >= {min_samples};  -- 只保存有效聚类
    """)
    
    conn.execute(dbscan_sql)
    
    # 获取统计信息
    stats_sql = text(f"""
        WITH dbscan_test AS (
            SELECT 
                ST_ClusterDBSCAN(geometry, {eps}, {min_samples}) OVER() as cluster_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        )
        SELECT 
            COUNT(DISTINCT cluster_id) FILTER (WHERE cluster_id IS NOT NULL) as cluster_count,
            COUNT(*) FILTER (WHERE cluster_id IS NULL) as noise_points,
            COUNT(*) as total_points
        FROM dbscan_test;
    """)
    
    stats = conn.execute(stats_sql).fetchone()
    
    return {
        'cluster_count': stats.cluster_count,
        'noise_points': stats.noise_points,
        'noise_rate': (stats.noise_points / stats.total_points * 100) if stats.total_points > 0 else 0
    }

def test_hierarchical_params(conn, city_id, eps, min_samples):
    """测试分层聚类参数"""
    
    # 分层聚类：粗聚类用eps*5，细聚类用eps
    coarse_eps = eps * 5
    
    hierarchical_sql = text(f"""
        WITH coarse_clusters AS (
            SELECT 
                geometry,
                ST_ClusterDBSCAN(geometry, {coarse_eps}, {min_samples * 2}) OVER() as coarse_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        fine_clusters AS (
            SELECT 
                coarse_id,
                geometry,
                ST_ClusterDBSCAN(geometry, {eps}, {min_samples}) OVER(PARTITION BY coarse_id) as fine_id
            FROM coarse_clusters 
            WHERE coarse_id IS NOT NULL
        ),
        hierarchical_stats AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as hierarchical_id,
                COUNT(*) as bbox_count,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_boundary,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as cluster_rank
            FROM fine_clusters
            WHERE fine_id IS NOT NULL
            GROUP BY coarse_id, fine_id
        )
        -- 保存分层聚类结果
        INSERT INTO clustering_parameter_comparison 
        (city_id, method_name, eps_value, min_samples, cluster_id, bbox_count, cluster_rank, noise_points, geometry)
        SELECT 
            '{city_id}',
            'Hierarchical',
            {eps},
            {min_samples},
            hierarchical_id,
            bbox_count,
            cluster_rank,
            0,  -- 分层聚类噪声点计算复杂，暂设为0
            cluster_boundary
        FROM hierarchical_stats
        WHERE bbox_count >= {min_samples};
    """)
    
    conn.execute(hierarchical_sql)
    
    # 获取统计信息
    stats_sql = text(f"""
        WITH coarse_test AS (
            SELECT 
                geometry,
                ST_ClusterDBSCAN(geometry, {coarse_eps}, {min_samples * 2}) OVER() as coarse_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        fine_test AS (
            SELECT 
                ST_ClusterDBSCAN(geometry, {eps}, {min_samples}) OVER(PARTITION BY coarse_id) as fine_id,
                coarse_id
            FROM coarse_test 
            WHERE coarse_id IS NOT NULL
        ),
        cluster_sizes AS (
            SELECT COUNT(*) as size
            FROM fine_test
            WHERE fine_id IS NOT NULL
            GROUP BY coarse_id, fine_id
        )
        SELECT 
            COUNT(*) as cluster_count,
            COALESCE(MAX(size), 0) as max_cluster_size
        FROM cluster_sizes;
    """)
    
    stats = conn.execute(stats_sql).fetchone()
    
    return {
        'cluster_count': stats.cluster_count,
        'max_cluster_size': stats.max_cluster_size
    }

def create_parameter_comparison_views(conn, city_id):
    """创建参数对比的QGIS视图"""
    
    print(f"\n🎨 创建参数对比视图...")
    
    # 先检查基础表是否有数据
    check_sql = text(f"""
        SELECT COUNT(*) as record_count 
        FROM clustering_parameter_comparison 
        WHERE city_id = '{city_id}';
    """)
    
    result = conn.execute(check_sql).fetchone()
    record_count = result.record_count if result else 0
    
    if record_count == 0:
        print(f"   ⚠️ 警告: clustering_parameter_comparison 表中没有数据 (city_id='{city_id}')")
        return
    
    print(f"   📊 找到 {record_count} 条记录")
    
    try:
        # 分别创建两个视图，便于调试
        view1_sql = text(f"""
            CREATE OR REPLACE VIEW qgis_parameter_comparison AS
            SELECT 
                id,
                city_id,
                method_name,
                eps_value,
                min_samples,
                cluster_id,
                bbox_count,
                cluster_rank,
                CASE 
                    WHEN eps_value <= 0.001 THEN 'Fine (≤0.001)'
                    WHEN eps_value <= 0.002 THEN 'Medium (≤0.002)'
                    ELSE 'Coarse (>0.002)'
                END as eps_category,
                CASE 
                    WHEN bbox_count >= 100 THEN 'Very Large'
                    WHEN bbox_count >= 50 THEN 'Large'
                    WHEN bbox_count >= 20 THEN 'Medium'
                    ELSE 'Small'
                END as cluster_size_category,
                geometry,
                test_timestamp
            FROM clustering_parameter_comparison
            WHERE city_id = '{city_id}'
            ORDER BY method_name, eps_value, cluster_rank;
        """)
        
        conn.execute(view1_sql)
        print(f"   ✅ 创建视图: qgis_parameter_comparison")
        
        view2_sql = text(f"""
            CREATE OR REPLACE VIEW qgis_parameter_stats AS
            SELECT 
                method_name,
                eps_value,
                COUNT(*) as total_clusters,
                MAX(bbox_count) as max_cluster_size,
                ROUND(AVG(bbox_count)::numeric, 2) as avg_cluster_size,
                COUNT(*) FILTER (WHERE bbox_count >= 50) as large_clusters,
                COUNT(*) FILTER (WHERE bbox_count >= 20) as medium_plus_clusters,
                ST_ConvexHull(ST_Collect(geometry)) as method_coverage
            FROM clustering_parameter_comparison
            WHERE city_id = '{city_id}'
            GROUP BY method_name, eps_value
            ORDER BY method_name, eps_value;
        """)
        
        conn.execute(view2_sql)
        print(f"   ✅ 创建视图: qgis_parameter_stats")
        
        conn.commit()
        
    except Exception as e:
        print(f"   ❌ 创建视图失败: {str(e)}")
        raise

def print_comparison_summary(results):
    """打印对比总结"""
    
    print(f"\n" + "=" * 60)
    print("📈 参数对比总结")
    print("=" * 60)
    
    print(f"\n{'eps值':<8} {'DBSCAN聚类':<12} {'噪声率':<8} {'分层聚类':<12} {'最大簇':<8}")
    print("-" * 60)
    
    for result in results:
        eps = result['eps']
        dbscan = result['dbscan']
        hierarchical = result['hierarchical']
        
        print(f"{eps:<8} {dbscan['cluster_count']:<12} {dbscan['noise_rate']:<8.1f}% {hierarchical['cluster_count']:<12} {hierarchical['max_cluster_size']:<8}")
    
    print(f"\n💡 参数选择建议:")
    
    # 找出最佳参数
    best_dbscan_eps = None
    best_hierarchical_eps = None
    
    for result in results:
        dbscan = result['dbscan']
        hierarchical = result['hierarchical']
        
        # DBSCAN最佳：聚类数适中且噪声率不太高
        if (dbscan['cluster_count'] >= 10 and dbscan['cluster_count'] <= 50 and 
            dbscan['noise_rate'] < 30):
            if best_dbscan_eps is None:
                best_dbscan_eps = result['eps']
        
        # 分层聚类最佳：聚类数多且最大簇不太大
        if (hierarchical['cluster_count'] >= 20 and hierarchical['max_cluster_size'] < 200):
            if best_hierarchical_eps is None:
                best_hierarchical_eps = result['eps']
    
    if best_dbscan_eps:
        print(f"   🎯 DBSCAN推荐: eps={best_dbscan_eps} (平衡聚类数和噪声率)")
    else:
        print(f"   ⚠️ DBSCAN: 可能需要调整参数范围")
        
    if best_hierarchical_eps:
        print(f"   🎯 分层聚类推荐: eps={best_hierarchical_eps} (细粒度聚类)")
    else:
        print(f"   ⚠️ 分层聚类: 可能需要调整参数范围")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='聚类参数对比测试')
    parser.add_argument('--city', default='A263', help='测试城市ID')
    
    args = parser.parse_args()
    
    try:
        results = test_eps_parameters(args.city)
        
        print_comparison_summary(results)
        
        print(f"\n✅ 参数对比测试完成！")
        print(f"💾 结果已保存到数据库表: clustering_parameter_comparison")
        print(f"🎨 QGIS可视化视图:")
        print(f"   • qgis_parameter_comparison - 参数对比视图")
        print(f"   • qgis_parameter_stats - 参数统计视图")
        print(f"\n🎯 QGIS使用建议:")
        print(f"   1. 加载 qgis_parameter_comparison 图层")
        print(f"   2. 按 method_name 和 eps_value 分类显示")
        print(f"   3. 按 cluster_size_category 设置符号大小")
        print(f"   4. 对比不同eps值的聚类效果")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
