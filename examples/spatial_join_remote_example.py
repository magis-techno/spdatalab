"""
空间连接远端计算示例

演示如何使用新的分批推送到远端计算的空间连接功能
"""

import logging
from spdatalab.fusion.spatial_join import SpatialJoin, SpatialRelation

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """主函数演示远端空间连接"""
    
    # 创建空间连接器
    joiner = SpatialJoin()
    
    print("=== 空间连接远端计算示例 ===\n")
    
    # 示例1：基本的路口相交分析
    print("1. 基本相交分析（前2批次测试）")
    try:
        result1 = joiner.batch_spatial_join_with_remote(
            left_table="clips_bbox",
            remote_table="full_intersection", 
            batch_by_city=True,
            spatial_relation=SpatialRelation.INTERSECTS,
            limit_batches=2,  # 只处理前2个城市用于测试
            summarize=True,
            summary_fields={
                "intersection_count": "count",
                "nearest_distance": "distance"
            },
            output_table="bbox_intersection_summary_test"
        )
        print(f"结果：{len(result1)} 条记录")
        if not result1.empty:
            print(f"示例数据：\n{result1.head()}")
        
    except Exception as e:
        print(f"示例1失败: {str(e)}")
    
    print("\n" + "="*50 + "\n")
    
    # 示例2：带缓冲区的距离分析
    print("2. 50米缓冲区距离分析")
    try:
        result2 = joiner.batch_spatial_join_with_remote(
            left_table="clips_bbox",
            remote_table="full_intersection",
            batch_by_city=True, 
            spatial_relation=SpatialRelation.DWITHIN,
            distance_meters=50.0,
            limit_batches=1,  # 只处理1个城市用于测试
            summarize=True,
            summary_fields={
                "nearby_intersections": "count",
                "min_distance": "distance"
            },
            output_table="bbox_intersection_buffer50_test"
        )
        print(f"结果：{len(result2)} 条记录")
        if not result2.empty:
            print(f"示例数据：\n{result2.head()}")
            
    except Exception as e:
        print(f"示例2失败: {str(e)}")
    
    print("\n" + "="*50 + "\n")
    
    # 示例3：获取详细字段信息（非汇总）
    print("3. 获取详细路口信息（非汇总模式）")
    try:
        result3 = joiner.batch_spatial_join_with_remote(
            left_table="clips_bbox",
            remote_table="full_intersection",
            batch_by_city=True,
            spatial_relation=SpatialRelation.INTERSECTS,
            limit_batches=1,
            summarize=False,  # 不汇总，获取详细信息
            fields_to_add=["intersection_id", "road_type"],
            output_table="bbox_intersection_details_test"
        )
        print(f"结果：{len(result3)} 条记录")
        if not result3.empty:
            print(f"示例数据：\n{result3.head()}")
            
    except Exception as e:
        print(f"示例3失败: {str(e)}")
    
    print("\n" + "="*50 + "\n")
    
    # 示例4：指定特定城市进行处理
    print("4. 指定特定城市进行处理")
    try:
        result4 = joiner.batch_spatial_join_with_remote(
            left_table="clips_bbox",
            remote_table="full_intersection",
            batch_by_city=True,
            city_ids=["singapore", "boston"],  # 只处理新加坡和波士顿
            spatial_relation=SpatialRelation.INTERSECTS,
            summarize=True,
            summary_fields={
                "intersection_count": "count",
                "nearest_distance": "distance"
            },
            output_table="bbox_intersection_specific_cities_test"
        )
        print(f"结果：{len(result4)} 条记录")
        if not result4.empty:
            print(f"处理的城市：{result4['city_id'].unique()}")
            print(f"示例数据：\n{result4.head()}")
            
    except Exception as e:
        print(f"示例4失败: {str(e)}")
    
    print("\n=== 示例完成 ===")

if __name__ == "__main__":
    main() 