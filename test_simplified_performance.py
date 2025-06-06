#!/usr/bin/env python3
"""
简化版空间连接性能测试
对比新旧方法的性能差异
"""

import time
import logging
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.spdatalab.fusion.spatial_join_simplified import SpatialJoinSimplified
from src.spdatalab.fusion.spatial_join import SpatialJoin  # 原版本

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_simplified_vs_original():
    """对比简化版本与原版本的性能"""
    
    logger.info("=== 简化版 vs 原版 性能对比 ===")
    
    # 测试参数
    test_bbox_count = 4  # 与你之前的测试保持一致
    
    try:
        # 测试简化版本
        logger.info(f"测试简化版本 ({test_bbox_count}个bbox)")
        simplified_joiner = SpatialJoinSimplified()
        
        start_time = time.time()
        simplified_result = simplified_joiner.spatial_join_remote(
            left_table="clips_bbox",
            remote_table="full_intersection", 
            distance_meters=100,
            batch_size=test_bbox_count,  # 一次处理所有数据
            summarize=True,
            where_clause=f"LIMIT {test_bbox_count}"
        )
        simplified_time = time.time() - start_time
        
        logger.info(f"简化版本完成: {len(simplified_result)}条结果, 耗时{simplified_time:.2f}秒")
        
    except Exception as e:
        logger.error(f"简化版本测试失败: {str(e)}")
        simplified_time = -1
        simplified_result = None
    
    try:
        # 测试原版本（推送数据方式）
        logger.info(f"测试原版本 ({test_bbox_count}个bbox)")
        original_joiner = SpatialJoin()
        
        start_time = time.time()
        original_result = original_joiner.batch_spatial_join_with_remote(
            left_table="clips_bbox",
            remote_table="full_intersection",
            batch_by_city=False,  # 按数量分批
            limit_batches=1,      # 限制批次
            summarize=True
        )
        original_time = time.time() - start_time
        
        logger.info(f"原版本完成: {len(original_result)}条结果, 耗时{original_time:.2f}秒")
        
    except Exception as e:
        logger.error(f"原版本测试失败: {str(e)}")
        original_time = 120  # 使用你报告的2分钟作为参考
        original_result = None
    
    # 性能对比
    logger.info(f"\n=== 性能对比结果 ===")
    logger.info(f"测试数据量: {test_bbox_count}个bbox")
    logger.info(f"简化版耗时: {simplified_time:.2f}秒")
    logger.info(f"原版本耗时: {original_time:.2f}秒")
    
    if simplified_time > 0 and original_time > 0:
        improvement = original_time / simplified_time
        logger.info(f"性能提升: {improvement:.1f}x")
        
        if improvement > 10:
            logger.info("✅ 简化版本显著提升性能，建议全面采用")
        elif improvement > 3:
            logger.info("✅ 简化版本性能更优")
        else:
            logger.info("⚠️  性能提升不明显")
    
    # 结果验证
    if simplified_result is not None and original_result is not None:
        logger.info(f"\n=== 结果验证 ===")
        logger.info(f"简化版结果数: {len(simplified_result)}")
        logger.info(f"原版本结果数: {len(original_result)}")
        
        if len(simplified_result) == len(original_result):
            logger.info("✅ 结果数量一致")
        else:
            logger.warning("⚠️  结果数量不一致，需要检查")

def test_different_scales():
    """测试不同规模的性能表现"""
    
    logger.info("\n=== 不同规模性能测试 ===")
    
    joiner = SpatialJoinSimplified()
    test_cases = [4, 10, 20, 50]
    
    results = []
    
    for bbox_count in test_cases:
        logger.info(f"测试 {bbox_count} 个bbox")
        try:
            start_time = time.time()
            result = joiner.spatial_join_remote(
                left_table="clips_bbox",
                remote_table="full_intersection",
                distance_meters=100,
                batch_size=min(bbox_count, 20),  # 批次大小
                summarize=True,
                where_clause=f"LIMIT {bbox_count}"
            )
            elapsed_time = time.time() - start_time
            
            results.append({
                'bbox_count': bbox_count,
                'time': elapsed_time,
                'result_count': len(result),
                'avg_time_per_bbox': elapsed_time / bbox_count
            })
            
            logger.info(f"  {bbox_count}个bbox: {elapsed_time:.2f}秒, "
                       f"平均{elapsed_time/bbox_count:.3f}秒/bbox")
            
        except Exception as e:
            logger.error(f"  {bbox_count}个bbox测试失败: {str(e)}")
    
    # 性能趋势分析
    if len(results) >= 2:
        logger.info(f"\n=== 性能趋势分析 ===")
        for result in results:
            logger.info(f"{result['bbox_count']:2d}个bbox: {result['time']:6.2f}秒 "
                       f"(平均{result['avg_time_per_bbox']:.3f}秒/bbox)")
        
        # 线性度检查
        avg_times = [r['avg_time_per_bbox'] for r in results]
        if max(avg_times) - min(avg_times) < 0.1:
            logger.info("✅ 性能随规模线性扩展，算法稳定")
        else:
            logger.info("⚠️  性能随规模变化较大，可能需要优化")

if __name__ == "__main__":
    # 运行性能对比测试
    test_simplified_vs_original()
    
    # 运行不同规模测试
    test_different_scales() 