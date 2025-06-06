#!/usr/bin/env python3
"""
超简化版本测试
验证FastSpatialJoin的性能是否与direct_remote_query_test.py相当
"""

import time
import logging
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.spdatalab.fusion.spatial_join_ultra_simple import FastSpatialJoin, quick_spatial_join

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_ultra_simple_performance():
    """测试超简化版本的性能"""
    
    logger.info("=== 超简化版本性能测试 ===")
    
    test_cases = [
        {"num_bbox": 4, "desc": "4个bbox (对标你之前的测试)"},
        {"num_bbox": 10, "desc": "10个bbox"},
        {"num_bbox": 20, "desc": "20个bbox"},
    ]
    
    joiner = FastSpatialJoin()
    
    results = []
    
    for test_case in test_cases:
        logger.info(f"\n测试: {test_case['desc']}")
        
        start_time = time.time()
        try:
            result = joiner.spatial_join(
                num_bbox=test_case['num_bbox'],
                buffer_meters=100
            )
            elapsed_time = time.time() - start_time
            
            logger.info(f"✅ 成功: {len(result)}条结果, 耗时{elapsed_time:.2f}秒")
            
            results.append({
                'num_bbox': test_case['num_bbox'],
                'time': elapsed_time,
                'result_count': len(result),
                'avg_time_per_bbox': elapsed_time / test_case['num_bbox']
            })
            
        except Exception as e:
            logger.error(f"❌ 失败: {str(e)}")
            results.append({
                'num_bbox': test_case['num_bbox'],
                'time': -1,
                'result_count': 0,
                'avg_time_per_bbox': -1
            })
    
    # 性能汇总
    logger.info(f"\n=== 性能汇总 ===")
    logger.info(f"{'bbox数':<8} {'总耗时':<8} {'结果数':<8} {'平均耗时/bbox':<12}")
    logger.info("-" * 40)
    
    for result in results:
        if result['time'] > 0:
            logger.info(f"{result['num_bbox']:<8} {result['time']:<8.2f} {result['result_count']:<8} {result['avg_time_per_bbox']:<12.3f}")
        else:
            logger.info(f"{result['num_bbox']:<8} {'FAIL':<8} {'-':<8} {'-':<12}")
    
    # 与之前测试对比
    if results and results[0]['time'] > 0:
        bbox_4_time = results[0]['time']
        logger.info(f"\n=== 与之前对比 ===")
        logger.info(f"超简化版本 (4个bbox): {bbox_4_time:.2f}秒")
        logger.info(f"之前版本 (4个bbox): ~120秒")
        if bbox_4_time < 120:
            improvement = 120 / bbox_4_time
            logger.info(f"性能提升: {improvement:.0f}x ⚡")
        
        # 性能建议
        if bbox_4_time < 5:
            logger.info("🎉 性能优异！建议采用此版本")
        elif bbox_4_time < 20:
            logger.info("✅ 性能良好，显著优于之前版本")
        else:
            logger.info("⚠️  性能有提升，但仍需优化")

def test_convenience_function():
    """测试便捷函数"""
    
    logger.info("\n=== 便捷函数测试 ===")
    
    try:
        start_time = time.time()
        result = quick_spatial_join(
            num_bbox=5,
            buffer_meters=50
        )
        elapsed_time = time.time() - start_time
        
        logger.info(f"便捷函数测试: {len(result)}条结果, 耗时{elapsed_time:.2f}秒")
        
        # 显示结果样例
        if not result.empty:
            logger.info("结果样例:")
            for _, row in result.head(3).iterrows():
                count = row.get('intersection_count', 0)
                distance = row.get('nearest_distance', 999999)
                logger.info(f"  {row['scene_token']}: {count}个相交, 距离{distance:.1f}米")
        
    except Exception as e:
        logger.error(f"便捷函数测试失败: {str(e)}")

if __name__ == "__main__":
    # 测试超简化版本性能
    test_ultra_simple_performance()
    
    # 测试便捷函数
    test_convenience_function() 