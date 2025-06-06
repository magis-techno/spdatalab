#!/usr/bin/env python3
"""
简化的点相交测试脚本
测试单个点与 full_intersection 表的相交查询性能
"""

import time
import logging
import random
import math
import statistics
from sqlalchemy import create_engine, text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 远端数据库连接
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

def generate_random_points_around(center_lon, center_lat, radius_km=1.0, num_points=10):
    """
    在指定中心点周围生成随机点
    
    Args:
        center_lon: 中心点经度
        center_lat: 中心点纬度  
        radius_km: 半径（公里）
        num_points: 生成点的数量
    
    Returns:
        List of (lon, lat) tuples
    """
    points = []
    
    # 将公里转换为大约的度数
    # 1度纬度 ≈ 111km
    # 1度经度 ≈ 111km * cos(latitude)
    lat_range = radius_km / 111.0  # 纬度范围
    lon_range = radius_km / (111.0 * math.cos(math.radians(center_lat)))  # 经度范围
    
    for _ in range(num_points):
        # 在圆形范围内生成随机点
        angle = random.uniform(0, 2 * math.pi)
        distance_ratio = math.sqrt(random.uniform(0, 1))  # 开平方保证均匀分布
        
        # 计算随机点坐标
        lon_offset = lon_range * distance_ratio * math.cos(angle)
        lat_offset = lat_range * distance_ratio * math.sin(angle)
        
        new_lon = center_lon + lon_offset
        new_lat = center_lat + lat_offset
        
        points.append((new_lon, new_lat))
    
    return points

def test_single_point_intersection():
    """测试单个点与 full_intersection 的相交查询"""
    
    # 测试点坐标 (经度, 纬度)
    test_lon = 121.62530652
    test_lat = 31.26092271
    
    logger.info(f"开始测试点相交查询")
    logger.info(f"测试点坐标: ({test_lon}, {test_lat})")
    
    try:
        # 连接远端数据库
        engine = create_engine(
            REMOTE_DSN, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        with engine.connect() as conn:
            # 先测试基本连接和表信息
            logger.info("=== 测试0: 基本连接和表信息 ===")
            start_time = time.time()
            
            # 检查表存在和记录数
            total_count = conn.execute(text("SELECT COUNT(*) FROM full_intersection")).scalar()
            
            # 检查几何列的数据类型
            geom_info = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'full_intersection' 
                AND column_name LIKE '%geom%'
            """)).fetchall()
            
            time0 = time.time() - start_time
            logger.info(f"连接测试耗时: {time0:.2f} 秒")
            logger.info(f"full_intersection 总记录数: {total_count:,}")
            logger.info("几何列信息:")
            for col in geom_info:
                logger.info(f"  列名: {col[0]}, 类型: {col[1]}")
            
            # 测试1: 简单的边界框查询（避免复杂几何操作）
            logger.info("=== 测试1: 边界框范围查询 ===")
            start_time = time.time()
            
            # 使用边界框查询，避免ST_Intersects
            buffer_degrees = 0.001  # 大约100米
            
            sql1 = text(f"""
                SELECT COUNT(*) as count_in_bbox
                FROM full_intersection 
                WHERE wkb_geometry && ST_MakeEnvelope(
                    {test_lon - buffer_degrees}, 
                    {test_lat - buffer_degrees},
                    {test_lon + buffer_degrees}, 
                    {test_lat + buffer_degrees}, 
                    4326
                )
            """)
            
            result1 = conn.execute(sql1).fetchone()
            time1 = time.time() - start_time
            
            logger.info(f"边界框查询结果: {result1[0]} 个要素")
            logger.info(f"边界框查询耗时: {time1:.2f} 秒")
            
            # 测试2: 尝试简单的几何查询（如果边界框查询成功）
            if result1[0] > 0:
                logger.info("=== 测试2: 简单几何查询 ===")
                start_time = time.time()
                
                try:
                    # 只获取基本信息，避免复杂的几何计算
                    sql2 = text(f"""
                        SELECT intersection_id, road_type
                        FROM full_intersection 
                        WHERE wkb_geometry && ST_MakeEnvelope(
                            {test_lon - buffer_degrees}, 
                            {test_lat - buffer_degrees},
                            {test_lon + buffer_degrees}, 
                            {test_lat + buffer_degrees}, 
                            4326
                        )
                        LIMIT 5
                    """)
                    
                    result2 = conn.execute(sql2).fetchall()
                    time2 = time.time() - start_time
                    
                    logger.info(f"几何查询耗时: {time2:.2f} 秒")
                    logger.info("附近要素:")
                    for row in result2:
                        logger.info(f"  ID: {row[0]}, 类型: {row[1]}")
                        
                except Exception as e:
                    logger.error(f"几何查询失败: {str(e)}")
                    time2 = -1
            else:
                logger.info("=== 测试2: 跳过（无数据） ===")
                time2 = 0
            
            # 测试3: 检查空间索引
            logger.info("=== 测试3: 检查空间索引 ===")
            start_time = time.time()
            
            try:
                # 检查索引信息
                index_info = conn.execute(text("""
                    SELECT indexname, indexdef 
                    FROM pg_indexes 
                    WHERE tablename = 'full_intersection'
                """)).fetchall()
                
                time3 = time.time() - start_time
                
                logger.info(f"索引查询耗时: {time3:.2f} 秒")
                logger.info("表索引信息:")
                spatial_index_found = False
                for idx in index_info:
                    logger.info(f"  索引: {idx[0]}")
                    if 'gist' in idx[1].lower() or 'spatial' in idx[1].lower():
                        spatial_index_found = True
                        logger.info(f"    -> 空间索引: {idx[1]}")
                
                if not spatial_index_found:
                    logger.warning("  未找到空间索引！这可能是性能问题的原因")
                else:
                    logger.info("  找到空间索引")
                    
            except Exception as e:
                logger.error(f"索引查询失败: {str(e)}")
                time3 = -1
            
            # 汇总
            logger.info("=== 性能测试汇总 ===")
            logger.info(f"边界框查询: {time1:.2f}秒 (返回{result1[0]}条记录)")
            if time2 >= 0:
                logger.info(f"几何查询: {time2:.2f}秒")
            if time3 >= 0:
                logger.info(f"索引查询: {time3:.2f}秒")
            
            # 性能建议
            if time1 > 1.0:
                logger.warning("边界框查询超过1秒，数据库性能可能有问题")
            elif time1 > 0.1:
                logger.info("边界框查询超过0.1秒，建议检查空间索引")
            else:
                logger.info("查询性能良好")
                
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        raise

def test_multiple_points_performance():
    """测试多个随机点的查询性能"""
    
    # 中心点坐标
    center_lon = 121.62530652
    center_lat = 31.26092271
    
    logger.info("=== 开始多点性能测试 ===")
    logger.info(f"中心点坐标: ({center_lon}, {center_lat})")
    
    try:
        # 连接远端数据库
        engine = create_engine(
            REMOTE_DSN, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        # 生成不同数量的随机点进行测试
        test_cases = [
            {"num_points": 5, "radius_km": 1.0, "description": "5个点 (1km范围)"},
            {"num_points": 10, "radius_km": 1.0, "description": "10个点 (1km范围)"},
            {"num_points": 20, "radius_km": 1.0, "description": "20个点 (1km范围)"},
            {"num_points": 10, "radius_km": 2.0, "description": "10个点 (2km范围)"},
        ]
        
        performance_results = []
        
        with engine.connect() as conn:
            for test_case in test_cases:
                logger.info(f"\n=== 测试案例: {test_case['description']} ===")
                
                # 生成随机点
                random_points = generate_random_points_around(
                    center_lon, center_lat, 
                    test_case['radius_km'], 
                    test_case['num_points']
                )
                
                logger.info(f"生成了{len(random_points)}个随机点")
                for i, (lon, lat) in enumerate(random_points[:3], 1):  # 只显示前3个
                    logger.info(f"  点{i}: ({lon:.6f}, {lat:.6f})")
                if len(random_points) > 3:
                    logger.info(f"  ... 还有{len(random_points)-3}个点")
                
                # 方法1: 逐个点查询
                logger.info("方法1: 逐个点查询")
                individual_times = []
                individual_results = []
                
                start_time = time.time()
                for i, (lon, lat) in enumerate(random_points):
                    point_start = time.time()
                    
                    buffer_degrees = test_case['radius_km'] / 111.0 * 0.1  # 小的查询范围
                    sql = text(f"""
                        SELECT COUNT(*) as count_in_bbox
                        FROM full_intersection 
                        WHERE wkb_geometry && ST_MakeEnvelope(
                            {lon - buffer_degrees}, 
                            {lat - buffer_degrees},
                            {lon + buffer_degrees}, 
                            {lat + buffer_degrees}, 
                            4326
                        )
                    """)
                    
                    result = conn.execute(sql).fetchone()
                    point_time = time.time() - point_start
                    
                    individual_times.append(point_time)
                    individual_results.append(result[0])
                
                total_individual_time = time.time() - start_time
                
                logger.info(f"逐个查询总耗时: {total_individual_time:.2f}秒")
                logger.info(f"平均每点耗时: {statistics.mean(individual_times):.3f}秒")
                logger.info(f"最快/最慢: {min(individual_times):.3f}s / {max(individual_times):.3f}s")
                logger.info(f"找到要素总数: {sum(individual_results)}")
                
                # 方法2: 批量查询（使用UNION ALL）
                logger.info("方法2: 批量查询 (UNION ALL)")
                
                start_time = time.time()
                
                # 构建批量查询SQL
                union_queries = []
                buffer_degrees = test_case['radius_km'] / 111.0 * 0.1
                
                for i, (lon, lat) in enumerate(random_points):
                    union_queries.append(f"""
                        SELECT {i} as point_id, COUNT(*) as count_in_bbox
                        FROM full_intersection 
                        WHERE wkb_geometry && ST_MakeEnvelope(
                            {lon - buffer_degrees}, 
                            {lat - buffer_degrees},
                            {lon + buffer_degrees}, 
                            {lat + buffer_degrees}, 
                            4326
                        )
                    """)
                
                batch_sql = text(" UNION ALL ".join(union_queries))
                batch_results = conn.execute(batch_sql).fetchall()
                batch_time = time.time() - start_time
                
                batch_total_features = sum(row[1] for row in batch_results)
                
                logger.info(f"批量查询总耗时: {batch_time:.2f}秒")
                logger.info(f"平均每点耗时: {batch_time/len(random_points):.3f}秒")
                logger.info(f"找到要素总数: {batch_total_features}")
                
                # 方法3: 单一复杂查询（使用多个点的组合边界框）
                logger.info("方法3: 组合边界框查询")
                
                start_time = time.time()
                
                # 计算所有点的外包矩形
                all_lons = [p[0] for p in random_points]
                all_lats = [p[1] for p in random_points]
                
                min_lon = min(all_lons) - buffer_degrees
                max_lon = max(all_lons) + buffer_degrees
                min_lat = min(all_lats) - buffer_degrees
                max_lat = max(all_lats) + buffer_degrees
                
                combined_sql = text(f"""
                    SELECT COUNT(*) as total_in_combined_bbox
                    FROM full_intersection 
                    WHERE wkb_geometry && ST_MakeEnvelope(
                        {min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326
                    )
                """)
                
                combined_result = conn.execute(combined_sql).fetchone()
                combined_time = time.time() - start_time
                
                logger.info(f"组合查询耗时: {combined_time:.2f}秒")  
                logger.info(f"组合边界框内要素: {combined_result[0]}")
                
                # 记录性能结果
                performance_results.append({
                    'description': test_case['description'],
                    'num_points': test_case['num_points'],
                    'individual_total_time': total_individual_time,
                    'individual_avg_time': statistics.mean(individual_times),
                    'batch_time': batch_time,
                    'batch_avg_time': batch_time/len(random_points),
                    'combined_time': combined_time,
                    'speedup_batch': total_individual_time / batch_time if batch_time > 0 else 0,
                    'speedup_combined': total_individual_time / combined_time if combined_time > 0 else 0
                })
        
        # 性能汇总
        logger.info("\n=== 性能测试汇总 ===")
        for result in performance_results:
            logger.info(f"\n{result['description']}:")
            logger.info(f"  逐个查询: {result['individual_total_time']:.2f}s (平均{result['individual_avg_time']:.3f}s/点)")
            logger.info(f"  批量查询: {result['batch_time']:.2f}s (平均{result['batch_avg_time']:.3f}s/点)")
            logger.info(f"  组合查询: {result['combined_time']:.2f}s")
            logger.info(f"  批量加速比: {result['speedup_batch']:.1f}x")
            logger.info(f"  组合加速比: {result['speedup_combined']:.1f}x")
        
        # 性能建议
        logger.info("\n=== 性能建议 ===")
        avg_individual_time = statistics.mean([r['individual_avg_time'] for r in performance_results])
        avg_batch_time = statistics.mean([r['batch_avg_time'] for r in performance_results])
        
        logger.info(f"平均单点查询时间: {avg_individual_time:.3f}秒")
        logger.info(f"平均批量查询时间: {avg_batch_time:.3f}秒")
        
        if avg_individual_time > 0.1:
            logger.warning("单点查询超过0.1秒，建议优化空间索引")
        if avg_batch_time < avg_individual_time:
            logger.info("批量查询性能更优，建议在实际应用中使用批量模式")
        else:
            logger.info("批量查询未显示明显优势，可能需要进一步优化")
                
    except Exception as e:
        logger.error(f"多点性能测试失败: {str(e)}")
        raise

if __name__ == "__main__":
    # 运行单点测试
    logger.info("开始单点测试...")
    test_single_point_intersection()
    
    logger.info("\n" + "="*50)
    
    # 运行多点性能测试
    logger.info("开始多点性能测试...")
    test_multiple_points_performance() 