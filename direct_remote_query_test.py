#!/usr/bin/env python3
"""
直接远端查询测试 - 不推送数据，直接在远端执行空间查询
测试这种策略是否比推送数据到远端更快
"""

import time
import logging
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

# 测试配置
class TestConfig:
    def __init__(self, num_bbox=10, buffer_meters=100, city_filter=None):
        self.num_bbox = num_bbox
        self.buffer_meters = buffer_meters
        self.city_filter = city_filter
        self.buffer_degrees = buffer_meters / 111000  # 转换为度数

def test_direct_remote_query_strategy(config):
    """
    测试直接远端查询策略：
    1. 从本地获取clips_bbox的坐标点
    2. 直接在远端查询这些点与full_intersection的空间关系
    3. 不推送任何几何数据到远端
    """
    
    logger.info("=== 直接远端查询策略测试 ===")
    logger.info(f"测试配置: {config.num_bbox}个bbox, 缓冲区{config.buffer_meters}米")
    
    try:
        # 连接本地和远端数据库
        local_engine = create_engine(LOCAL_DSN, future=True)
        remote_engine = create_engine(
            REMOTE_DSN, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        # 步骤1: 从本地获取测试数据（只获取坐标，不获取几何）
        logger.info("步骤1: 从本地获取clips_bbox坐标数据")
        start_time = time.time()
        
        # 构建查询SQL（支持城市过滤）
        where_clause = ""
        if config.city_filter:
            where_clause = f"WHERE city_id = '{config.city_filter}'"
        
        # 获取坐标点（提取几何中心点坐标）
        local_sql = text(f"""
            SELECT 
                scene_token,
                city_id,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM clips_bbox 
            {where_clause}
            ORDER BY scene_token
            LIMIT {config.num_bbox}
        """)
        
        with local_engine.connect() as conn:
            local_data = pd.read_sql(local_sql, conn)
        
        local_fetch_time = time.time() - start_time
        logger.info(f"本地数据获取耗时: {local_fetch_time:.2f}秒")
        logger.info(f"获取到{len(local_data)}条记录")
        
        if local_data.empty:
            logger.error("本地数据为空，无法进行测试")
            return
        
        # 显示前几条数据
        for i, row in local_data.head(3).iterrows():
            logger.info(f"  {row['scene_token']}: ({row['lon']:.6f}, {row['lat']:.6f})")
        
        # 步骤2: 构建远端查询SQL（不创建临时表）
        logger.info("步骤2: 构建直接远端查询")
        start_time = time.time()
        
        # 方法A: 使用UNION ALL为每个点构建查询
        point_queries = []
        
        for _, row in local_data.iterrows():
            lon, lat = row['lon'], row['lat']
            scene_token = row['scene_token']
            
            point_query = f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    COUNT(*) as intersection_count,
                    MIN(ST_Distance(
                        ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                        wkb_geometry::geography
                    )) as nearest_distance
                FROM full_intersection 
                WHERE wkb_geometry && ST_MakeEnvelope(
                    {lon - config.buffer_degrees}, 
                    {lat - config.buffer_degrees},
                    {lon + config.buffer_degrees}, 
                    {lat + config.buffer_degrees}, 
                    4326
                )
                AND ST_DWithin(
                    ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                    wkb_geometry::geography,
                    {config.buffer_meters}
                )
            """
            point_queries.append(point_query)
        
        # 合并所有查询
        remote_sql = text(" UNION ALL ".join(point_queries))
        
        # 在远端执行查询
        with remote_engine.connect() as conn:
            remote_results = pd.read_sql(remote_sql, conn)
        
        remote_query_time = time.time() - start_time
        logger.info(f"远端查询耗时: {remote_query_time:.2f}秒")
        logger.info(f"返回{len(remote_results)}条结果")
        
        # 步骤3: 本地合并结果
        logger.info("步骤3: 合并结果")
        start_time = time.time()
        
        # 合并本地数据和远端查询结果
        final_result = local_data.merge(
            remote_results, 
            on='scene_token', 
            how='left'
        )
        
        merge_time = time.time() - start_time
        logger.info(f"结果合并耗时: {merge_time:.2f}秒")
        
        # 总耗时
        total_time = local_fetch_time + remote_query_time + merge_time
        logger.info(f"总耗时: {total_time:.2f}秒")
        
        # 显示结果
        logger.info("空间连接结果:")
        for _, row in final_result.iterrows():
            count = row['intersection_count'] if pd.notna(row['intersection_count']) else 0
            distance = row['nearest_distance'] if pd.notna(row['nearest_distance']) else 'N/A'
            logger.info(f"  {row['scene_token']}: {count}个相交, 最近距离: {distance}")
        
        return final_result, total_time
        
    except Exception as e:
        logger.error(f"直接远端查询测试失败: {str(e)}")
        raise

def test_batch_vs_individual_remote_queries(config):
    """
    对比批量查询 vs 逐个查询的性能
    """
    
    logger.info("\n=== 批量 vs 逐个查询对比测试 ===")
    logger.info(f"测试配置: {config.num_bbox}个bbox")
    
    try:
        local_engine = create_engine(LOCAL_DSN, future=True)
        remote_engine = create_engine(
            REMOTE_DSN, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        # 构建查询SQL（支持城市过滤）
        where_clause = ""
        if config.city_filter:
            where_clause = f"WHERE city_id = '{config.city_filter}'"
        
        # 获取测试数据
        local_sql = text(f"""
            SELECT 
                scene_token,
                city_id,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM clips_bbox 
            {where_clause}
            ORDER BY scene_token
            LIMIT {config.num_bbox}
        """)
        
        with local_engine.connect() as conn:
            test_data = pd.read_sql(local_sql, conn)
        
        logger.info(f"测试数据: {len(test_data)}条记录")
        
        # 方法1: 逐个查询
        logger.info("方法1: 逐个查询")
        individual_start = time.time()
        individual_results = []
        
        with remote_engine.connect() as conn:
            for _, row in test_data.iterrows():
                lon, lat = row['lon'], row['lat']
                scene_token = row['scene_token']
                
                sql = text(f"""
                    SELECT 
                        '{scene_token}' as scene_token,
                        COUNT(*) as intersection_count
                    FROM full_intersection 
                    WHERE ST_DWithin(
                        ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                        wkb_geometry::geography,
                        {config.buffer_meters}
                    )
                """)
                
                result = conn.execute(sql).fetchone()
                individual_results.append({
                    'scene_token': scene_token,
                    'intersection_count': result[1]
                })
        
        individual_time = time.time() - individual_start
        logger.info(f"逐个查询耗时: {individual_time:.2f}秒")
        logger.info(f"平均每次查询: {individual_time/len(test_data):.3f}秒")
        
        # 方法2: 批量查询（已在上面的函数中实现）
        logger.info("方法2: 批量查询")
        batch_result, batch_time = test_direct_remote_query_strategy(config)
        
        # 对比结果
        logger.info(f"\n=== 性能对比 ===")
        logger.info(f"逐个查询: {individual_time:.2f}秒 (平均{individual_time/len(test_data):.3f}秒/点)")
        logger.info(f"批量查询: {batch_time:.2f}秒 (平均{batch_time/len(test_data):.3f}秒/点)")
        
        if batch_time < individual_time:
            speedup = individual_time / batch_time
            logger.info(f"批量查询加速比: {speedup:.1f}x")
        else:
            logger.info("批量查询未显示性能优势")
        
        # 与推送数据方式对比
        logger.info(f"\n=== 与数据推送方式对比 ===")
        logger.info(f"直接查询方式: {batch_time:.2f}秒 ({len(test_data)}个点)")
        logger.info("推送数据方式: ~120秒 (4个点) - 根据你的报告")
        
        estimated_push_time_for_same = 120 * (len(test_data) / 4)
        improvement = estimated_push_time_for_same / batch_time
        
        logger.info(f"预估推送方式需要: {estimated_push_time_for_same:.0f}秒")
        logger.info(f"性能提升: {improvement:.0f}x")
        
        return {
            'individual_time': individual_time,
            'batch_time': batch_time,
            'num_points': len(test_data),
            'speedup': individual_time / batch_time if batch_time > 0 else 0,
            'improvement_vs_push': improvement
        }
        
    except Exception as e:
        logger.error(f"对比测试失败: {str(e)}")
        raise

def run_performance_tests(configs):
    """运行多种配置的性能测试"""
    results = []
    
    for i, config in enumerate(configs, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"运行测试配置 {i}/{len(configs)}")
        logger.info(f"配置: {config.num_bbox}个bbox, {config.buffer_meters}米缓冲区" + 
                   (f", 城市:{config.city_filter}" if config.city_filter else ""))
        logger.info(f"{'='*60}")
        
        try:
            # 运行对比测试
            result = test_batch_vs_individual_remote_queries(config)
            result['config'] = {
                'num_bbox': config.num_bbox,
                'buffer_meters': config.buffer_meters,
                'city_filter': config.city_filter
            }
            results.append(result)
            
        except Exception as e:
            logger.error(f"配置{i}测试失败: {str(e)}")
            
    return results

def print_summary(results):
    """打印测试结果汇总"""
    if not results:
        logger.warning("没有测试结果可汇总")
        return
        
    logger.info(f"\n{'='*60}")
    logger.info("测试结果汇总")
    logger.info(f"{'='*60}")
    
    logger.info(f"{'配置':<20} {'bbox数':<8} {'批量耗时':<10} {'加速比':<8} {'vs推送提升':<12}")
    logger.info("-" * 60)
    
    for result in results:
        config = result['config']
        config_desc = f"{config['num_bbox']}个/{config['buffer_meters']}m"
        if config['city_filter']:
            config_desc += f"/{config['city_filter']}"
            
        logger.info(f"{config_desc:<20} {result['num_points']:<8} {result['batch_time']:<10.2f} "
                   f"{result['speedup']:<8.1f} {result['improvement_vs_push']:<12.0f}x")
    
    # 性能建议
    logger.info(f"\n{'='*60}")
    logger.info("性能分析建议")
    logger.info(f"{'='*60}")
    
    avg_batch_time = sum(r['batch_time'] for r in results) / len(results)
    avg_speedup = sum(r['speedup'] for r in results) / len(results)
    avg_improvement = sum(r['improvement_vs_push'] for r in results) / len(results)
    
    logger.info(f"平均批量查询时间: {avg_batch_time:.2f}秒")
    logger.info(f"平均批量加速比: {avg_speedup:.1f}x")
    logger.info(f"相比推送数据平均提升: {avg_improvement:.0f}x")
    
    if avg_improvement > 10:
        logger.info("✅ 强烈建议使用直接远端查询策略替代数据推送方式")
    elif avg_improvement > 3:
        logger.info("✅ 建议使用直接远端查询策略")
    else:
        logger.info("⚠️  直接查询优势不明显，需要进一步分析")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='直接远端查询性能测试')
    parser.add_argument('--num-bbox', type=int, nargs='+', default=[4, 10, 20], 
                       help='测试的bbox数量列表 (默认: 4 10 20)')
    parser.add_argument('--buffer-meters', type=int, default=100,
                       help='缓冲区半径（米）(默认: 100)')
    parser.add_argument('--city', type=str, default=None,
                       help='指定城市ID过滤 (默认: 不过滤)')
    parser.add_argument('--quick', action='store_true',
                       help='快速测试模式，只测试4个bbox')
    
    args = parser.parse_args()
    
    # 根据参数生成测试配置
    if args.quick:
        bbox_counts = [4]
    else:
        bbox_counts = args.num_bbox
    
    configs = []
    for num_bbox in bbox_counts:
        config = TestConfig(
            num_bbox=num_bbox,
            buffer_meters=args.buffer_meters,
            city_filter=args.city
        )
        configs.append(config)
    
    logger.info(f"开始性能测试，共{len(configs)}个配置")
    
    # 运行测试
    results = run_performance_tests(configs)
    
    # 打印汇总
    print_summary(results) 