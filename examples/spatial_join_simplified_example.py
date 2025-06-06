#!/usr/bin/env python3
"""
简化版空间连接使用示例
展示如何使用直接远端查询策略进行高效的空间连接
"""

import logging
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from spdatalab.fusion.spatial_join_simplified import SpatialJoinSimplified, SpatialRelation

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """主函数 - 演示简化版空间连接的使用"""
    
    logger.info("=== 简化版空间连接示例 ===")
    
    # 创建空间连接器
    joiner = SpatialJoinSimplified()
    
    # 示例1: 基础的bbox与交叉口相交分析
    logger.info("示例1: bbox与交叉口相交分析")
    try:
        result1 = joiner.bbox_intersect_features(
            feature_table="full_intersection",
            distance_meters=100,
            city_ids=None  # 所有城市
        )
        
        logger.info(f"示例1完成，返回{len(result1)}条结果")
        if not result1.empty:
            logger.info("前5条结果:")
            for _, row in result1.head().iterrows():
                logger.info(f"  {row['scene_token']}: {row.get('intersection_count', 0)}个相交, "
                           f"最近距离: {row.get('nearest_distance', 'N/A'):.2f}米")
    
    except Exception as e:
        logger.error(f"示例1失败: {str(e)}")
    
    # 示例2: 指定城市的空间连接
    logger.info("\n示例2: 指定城市的空间连接")
    try:
        result2 = joiner.spatial_join_remote(
            left_table="clips_bbox",
            remote_table="full_intersection",
            spatial_relation=SpatialRelation.DWITHIN,
            distance_meters=50,
            batch_size=20,  # 小批次测试
            city_ids=["boston-seaport"],  # 指定城市
            summarize=True,
            summary_fields={
                "nearby_count": "count",
                "min_distance": "distance"
            }
        )
        
        logger.info(f"示例2完成，返回{len(result2)}条结果")
        if not result2.empty:
            logger.info(f"城市boston-seaport的空间连接结果:")
            avg_count = result2['nearby_count'].mean()
            avg_distance = result2['min_distance'].mean()
            logger.info(f"  平均附近要素数: {avg_count:.1f}")
            logger.info(f"  平均最近距离: {avg_distance:.1f}米")
    
    except Exception as e:
        logger.error(f"示例2失败: {str(e)}")
    
    # 示例3: 详细模式（获取具体匹配的要素）
    logger.info("\n示例3: 详细模式查询")
    try:
        result3 = joiner.spatial_join_remote(
            left_table="clips_bbox",
            remote_table="full_intersection",
            spatial_relation=SpatialRelation.DWITHIN,
            distance_meters=200,
            batch_size=10,
            summarize=False,  # 详细模式
            where_clause="scene_token LIKE 'scene-%' LIMIT 5"  # 限制测试数据
        )
        
        logger.info(f"示例3完成，返回{len(result3)}条结果")
        if not result3.empty:
            logger.info("详细匹配结果:")
            for _, row in result3.head(10).iterrows():
                logger.info(f"  {row['scene_token']} -> {row.get('intersection_id', 'N/A')} "
                           f"({row.get('road_type', 'N/A')}) 距离: {row.get('distance_meters', 'N/A'):.1f}米")
    
    except Exception as e:
        logger.error(f"示例3失败: {str(e)}")

if __name__ == "__main__":
    main() 