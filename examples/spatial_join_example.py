#!/usr/bin/env python3
"""
空间连接使用示例 - 类似QGIS的join attributes by location

演示如何使用简化的空间连接功能进行bbox与各种要素的叠置分析
"""

import sys
from pathlib import Path
import logging

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spdatalab.fusion.spatial_join import SpatialJoin, SpatialRelation, JoinType, SummaryMethod

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def basic_spatial_join_example():
    """基础空间连接示例"""
    logger.info("=== 基础空间连接示例 ===")
    
    # 初始化空间连接器
    spatial_join = SpatialJoin()
    
    # 1. 最简单的用法：bbox与路口相交分析
    logger.info("1. bbox与路口相交分析...")
    
    results = spatial_join.bbox_intersect_features(
        feature_table="intersections",
        feature_type="intersections",
        buffer_meters=20.0,  # 20米缓冲区
        output_table="bbox_intersections_results"
    )
    
    if len(results) > 0:
        logger.info(f"找到 {len(results)} 个bbox-路口关联")
        logger.info(f"涉及 {results['scene_token'].nunique()} 个唯一场景")
        logger.info(f"涉及 {results['city_id'].nunique()} 个城市")
    else:
        logger.warning("没有找到相交结果")
    
    return results

def advanced_spatial_join_example():
    """高级空间连接示例"""
    logger.info("=== 高级空间连接示例 ===")
    
    spatial_join = SpatialJoin()
    
    # 2. 自定义字段选择的空间连接
    logger.info("2. 自定义字段的空间连接...")
    
    # 选择特定的路口属性并重命名
    custom_results = spatial_join.join_attributes_by_location(
        left_table="clips_bbox",
        right_table="intersections",
        spatial_relation=SpatialRelation.DWITHIN,
        distance_meters=50.0,
        select_fields={
            "intersection_id": "inter_id",           # 重命名路口ID
            "intersection_type": "inter_type",       # 重命名路口类型
            "intersection_count": SummaryMethod.COUNT,  # 统计相交路口数量
            "min_distance_to_intersection": "distance_meters|min"  # 最近路口距离
        },
        where_clause="r.inter_type IS NOT NULL",  # 只考虑有类型的路口
        output_table="bbox_custom_intersections"
    )
    
    if len(custom_results) > 0:
        logger.info(f"自定义连接结果: {len(custom_results)} 条记录")
        logger.info("样例数据:")
        print(custom_results[['scene_token', 'intersection_id', 'intersection_type', 
                           'intersection_count', 'min_distance_to_intersection']].head())
    
    return custom_results

def multi_feature_analysis_example():
    """多要素分析示例"""
    logger.info("=== 多要素分析示例 ===")
    
    spatial_join = SpatialJoin()
    
    # 3. 与不同类型要素的分析
    feature_analyses = {}
    
    # 假设有这些要素表（根据实际情况调整）
    feature_configs = [
        {
            "table": "intersections",
            "type": "intersections", 
            "buffer": 25.0,
            "description": "路口"
        },
        # 可以添加更多要素类型
        # {
        #     "table": "roads", 
        #     "type": "roads",
        #     "buffer": 10.0,
        #     "description": "道路"
        # },
        # {
        #     "table": "pois",
        #     "type": "pois", 
        #     "buffer": 100.0,
        #     "description": "兴趣点"
        # }
    ]
    
    for config in feature_configs:
        logger.info(f"3.{len(feature_analyses)+1} 分析bbox与{config['description']}的关系...")
        
        try:
            result = spatial_join.bbox_intersect_features(
                feature_table=config["table"],
                feature_type=config["type"],
                buffer_meters=config["buffer"],
                summary_fields={
                    f"{config['type']}_count": "count",
                    f"nearest_{config['type']}_distance": "min_distance"
                },
                output_table=f"bbox_{config['type']}_analysis"
            )
            
            feature_analyses[config['type']] = result
            
            if len(result) > 0:
                logger.info(f"  - {config['description']}分析完成: {len(result)} 条记录")
            else:
                logger.warning(f"  - {config['description']}没有找到相交结果")
                
        except Exception as e:
            logger.error(f"  - {config['description']}分析失败: {str(e)}")
    
    return feature_analyses

def spatial_relationship_comparison():
    """不同空间关系对比"""
    logger.info("=== 空间关系对比示例 ===")
    
    spatial_join = SpatialJoin()
    
    # 4. 对比不同空间关系的结果
    relationships = [
        (SpatialRelation.INTERSECTS, "相交"),
        (SpatialRelation.WITHIN, "包含于"),
        (SpatialRelation.CONTAINS, "包含"),
        (SpatialRelation.DWITHIN, "距离范围内")
    ]
    
    comparison_results = {}
    
    for relation, description in relationships:
        logger.info(f"4. 测试 {description} 关系...")
        
        try:
            if relation == SpatialRelation.DWITHIN:
                # DWITHIN需要距离参数
                result = spatial_join.join_attributes_by_location(
                    left_table="clips_bbox",
                    right_table="intersections", 
                    spatial_relation=relation,
                    distance_meters=30.0,
                    select_fields={"count": SummaryMethod.COUNT}
                )
            else:
                result = spatial_join.join_attributes_by_location(
                    left_table="clips_bbox",
                    right_table="intersections",
                    spatial_relation=relation,
                    select_fields={"count": SummaryMethod.COUNT}
                )
            
            comparison_results[description] = len(result)
            logger.info(f"  - {description}: {len(result)} 条记录")
            
        except Exception as e:
            logger.error(f"  - {description} 分析失败: {str(e)}")
            comparison_results[description] = 0
    
    # 显示对比结果
    logger.info("空间关系对比结果:")
    for relation_desc, count in comparison_results.items():
        logger.info(f"  {relation_desc}: {count} 条记录")
    
    return comparison_results

def export_and_summary_example():
    """导出和汇总示例"""
    logger.info("=== 导出和汇总示例 ===")
    
    spatial_join = SpatialJoin()
    
    # 5. 生成分析结果并导出
    logger.info("5. 生成完整分析结果...")
    
    # 执行完整的bbox-路口分析
    full_results = spatial_join.join_attributes_by_location(
        left_table="clips_bbox",
        right_table="intersections",
        spatial_relation=SpatialRelation.DWITHIN,
        distance_meters=30.0,
        select_fields={
            "inter_id": "inter_id",
            "inter_type": "inter_type", 
            "distance_m": "distance_meters|min",
            "intersection_count": SummaryMethod.COUNT
        },
        output_table="full_bbox_intersection_analysis"
    )
    
    if len(full_results) > 0:
        logger.info(f"完整分析结果: {len(full_results)} 条记录")
        
        # 基础统计
        logger.info("基础统计信息:")
        logger.info(f"  - 唯一场景数: {full_results['scene_token'].nunique()}")
        logger.info(f"  - 涉及城市数: {full_results['city_id'].nunique()}")
        
        if 'distance_m' in full_results.columns:
            logger.info(f"  - 平均距离: {full_results['distance_m'].mean():.2f} 米")
            logger.info(f"  - 最小距离: {full_results['distance_m'].min():.2f} 米")
            logger.info(f"  - 最大距离: {full_results['distance_m'].max():.2f} 米")
        
        # 按城市汇总
        if 'city_id' in full_results.columns:
            city_summary = full_results.groupby('city_id').agg({
                'scene_token': 'count',
                'distance_m': 'mean' if 'distance_m' in full_results.columns else 'size'
            }).round(2)
            
            logger.info("按城市汇总:")
            print(city_summary)
        
        # 保存为不同格式（这里只是示例，实际需要geopandas）
        logger.info("结果已保存到数据库表: full_bbox_intersection_analysis")
        
    return full_results

def main():
    """主函数，运行所有示例"""
    logger.info("开始运行空间连接示例...")
    
    try:
        # 1. 基础示例
        basic_results = basic_spatial_join_example()
        
        # 2. 高级示例
        advanced_results = advanced_spatial_join_example()
        
        # 3. 多要素分析
        multi_feature_results = multi_feature_analysis_example()
        
        # 4. 空间关系对比
        relationship_comparison = spatial_relationship_comparison()
        
        # 5. 导出和汇总
        full_results = export_and_summary_example()
        
        logger.info("所有空间连接示例运行完成！")
        
        # 总结
        logger.info("=== 运行总结 ===")
        logger.info(f"基础分析结果: {len(basic_results) if basic_results is not None else 0} 条记录")
        logger.info(f"高级分析结果: {len(advanced_results) if advanced_results is not None else 0} 条记录")
        logger.info(f"多要素分析: {len(multi_feature_results)} 种要素类型")
        
        if relationship_comparison:
            best_relation = max(relationship_comparison.items(), key=lambda x: x[1])
            logger.info(f"最有效的空间关系: {best_relation[0]} ({best_relation[1]} 条记录)")
        
    except Exception as e:
        logger.error(f"示例运行过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 