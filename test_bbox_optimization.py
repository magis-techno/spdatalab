#!/usr/bin/env python3
"""
bbox优化策略对比测试
对比中心点 vs bbox边界策略的性能和准确性
"""

import time
import logging
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.spdatalab.fusion.spatial_join_ultra_simple import FastSpatialJoin
from src.spdatalab.fusion.spatial_join_bbox_optimized import BboxOptimizedSpatialJoin

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_center_vs_bbox_strategies():
    """对比中心点策略 vs bbox边界策略"""
    
    logger.info("=== 中心点 vs bbox边界策略对比 ===")
    
    test_params = {
        'num_bbox': 4,
        'buffer_meters': 100
    }
    
    results = {}
    
    # 测试1: 中心点策略（ultra_simple）
    logger.info("\n测试1: 中心点策略")
    try:
        joiner_center = FastSpatialJoin()
        start_time = time.time()
        
        result_center = joiner_center.spatial_join(**test_params)
        time_center = time.time() - start_time
        
        logger.info(f"中心点策略: {len(result_center)}条结果, 耗时{time_center:.2f}秒")
        results['center'] = {
            'time': time_center,
            'count': len(result_center),
            'result': result_center
        }
        
    except Exception as e:
        logger.error(f"中心点策略失败: {str(e)}")
        results['center'] = {'time': -1, 'count': 0, 'result': None}
    
    # 测试2: bbox边界策略（优化版）
    logger.info("\n测试2: bbox边界策略")
    try:
        joiner_bbox = BboxOptimizedSpatialJoin()
        start_time = time.time()
        
        result_bbox = joiner_bbox.spatial_join(
            use_bbox_boundary=True,
            **test_params
        )
        time_bbox = time.time() - start_time
        
        logger.info(f"bbox边界策略: {len(result_bbox)}条结果, 耗时{time_bbox:.2f}秒")
        results['bbox'] = {
            'time': time_bbox,
            'count': len(result_bbox),
            'result': result_bbox
        }
        
    except Exception as e:
        logger.error(f"bbox边界策略失败: {str(e)}")
        results['bbox'] = {'time': -1, 'count': 0, 'result': None}
    
    # 测试3: bbox策略但使用中心点（对照组）
    logger.info("\n测试3: bbox策略+中心点模式")
    try:
        start_time = time.time()
        
        result_bbox_center = joiner_bbox.spatial_join(
            use_bbox_boundary=False,  # 使用中心点
            **test_params
        )
        time_bbox_center = time.time() - start_time
        
        logger.info(f"bbox策略+中心点: {len(result_bbox_center)}条结果, 耗时{time_bbox_center:.2f}秒")
        results['bbox_center'] = {
            'time': time_bbox_center,
            'count': len(result_bbox_center),
            'result': result_bbox_center
        }
        
    except Exception as e:
        logger.error(f"bbox策略+中心点失败: {str(e)}")
        results['bbox_center'] = {'time': -1, 'count': 0, 'result': None}
    
    # 性能对比
    logger.info(f"\n=== 性能对比汇总 ===")
    logger.info(f"{'策略':<15} {'耗时(秒)':<10} {'结果数':<8} {'备注':<20}")
    logger.info("-" * 55)
    
    for strategy, data in results.items():
        if data['time'] > 0:
            strategy_name = {
                'center': '中心点',
                'bbox': 'bbox边界',
                'bbox_center': 'bbox+中心点'
            }.get(strategy, strategy)
            
            notes = ""
            if strategy == 'bbox' and data['time'] < results.get('center', {}).get('time', 999):
                notes = "更快"
            elif strategy == 'bbox' and data['count'] != results.get('center', {}).get('count', 0):
                notes = "结果数不同"
            
            logger.info(f"{strategy_name:<15} {data['time']:<10.2f} {data['count']:<8} {notes:<20}")
        else:
            logger.info(f"{strategy:<15} {'FAIL':<10} {'-':<8} {'失败':<20}")
    
    # 详细结果对比
    if all(results[k]['result'] is not None for k in ['center', 'bbox']):
        logger.info(f"\n=== 详细结果对比 ===")
        
        center_result = results['center']['result']
        bbox_result = results['bbox']['result']
        
        # 对比前几条记录
        logger.info("前3条记录对比:")
        for i in range(min(3, len(center_result), len(bbox_result))):
            scene_token = center_result.iloc[i]['scene_token']
            
            center_count = center_result.iloc[i].get('intersection_count', 0)
            center_distance = center_result.iloc[i].get('nearest_distance', 999999)
            
            bbox_count = bbox_result.iloc[i].get('intersection_count', 0)
            bbox_distance_center = bbox_result.iloc[i].get('nearest_distance_to_center', 999999)
            bbox_distance_bbox = bbox_result.iloc[i].get('nearest_distance_to_bbox', 999999)
            
            logger.info(f"  {scene_token}:")
            logger.info(f"    中心点策略: {center_count}个相交, 距离{center_distance:.1f}米")
            logger.info(f"    bbox策略: {bbox_count}个相交, 中心距离{bbox_distance_center:.1f}米, bbox距离{bbox_distance_bbox:.1f}米")
            
            if center_count != bbox_count:
                logger.info(f"    ⚠️ 相交数量不同！")
            if abs(center_distance - bbox_distance_center) > 1:
                logger.info(f"    ⚠️ 中心点距离计算不同！")

def test_different_buffer_sizes():
    """测试不同缓冲区大小的影响"""
    
    logger.info("\n=== 不同缓冲区大小测试 ===")
    
    joiner_bbox = BboxOptimizedSpatialJoin()
    buffer_sizes = [50, 100, 200, 500]
    
    for buffer_size in buffer_sizes:
        logger.info(f"\n测试缓冲区: {buffer_size}米")
        
        try:
            start_time = time.time()
            result = joiner_bbox.spatial_join(
                num_bbox=4,
                buffer_meters=buffer_size,
                use_bbox_boundary=True
            )
            elapsed_time = time.time() - start_time
            
            total_intersections = result['intersection_count'].sum() if not result.empty else 0
            avg_distance = result['nearest_distance_to_bbox'].mean() if not result.empty and 'nearest_distance_to_bbox' in result else 0
            
            logger.info(f"  {buffer_size}米缓冲区: {len(result)}条bbox, {total_intersections}个总相交, "
                       f"平均距离{avg_distance:.1f}米, 耗时{elapsed_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"  {buffer_size}米缓冲区测试失败: {str(e)}")

if __name__ == "__main__":
    # 运行策略对比测试
    test_center_vs_bbox_strategies()
    
    # 运行不同缓冲区大小测试
    test_different_buffer_sizes() 