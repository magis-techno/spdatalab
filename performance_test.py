#!/usr/bin/env python3
"""
性能测试脚本 - 测试优化后的性能
"""

import sys
import time
from pathlib import Path
import logging

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from spdatalab.fusion.spatial_join import SpatialJoin, SpatialRelation
from sqlalchemy import text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def performance_test():
    """性能测试"""
    print("🚀 开始性能测试...")
    
    try:
        joiner = SpatialJoin()
        
        # 获取一个小城市进行测试
        with joiner.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT city_id, COUNT(*) as count
                FROM clips_bbox 
                GROUP BY city_id 
                HAVING COUNT(*) <= 10  -- 选择记录较少的城市
                ORDER BY count ASC 
                LIMIT 1
            """)).fetchone()
            
            if not result:
                # 如果没有小城市，选择最小的
                result = conn.execute(text("""
                    SELECT city_id, COUNT(*) as count
                    FROM clips_bbox 
                    GROUP BY city_id 
                    ORDER BY count ASC 
                    LIMIT 1
                """)).fetchone()
            
            test_city = result[0]
            city_count = result[1]
        
        print(f"🏙️  性能测试城市: {test_city} ({city_count}条记录)")
        
        # 测试1：基本相交查询
        print("\n📊 测试1: 基本相交查询")
        start_time = time.time()
        
        result1 = joiner.batch_spatial_join_with_remote(
            batch_by_city=True,
            city_ids=[test_city],
            spatial_relation=SpatialRelation.INTERSECTS,
            summarize=True,
            summary_fields={
                "intersection_count": "count"
            }
        )
        
        elapsed1 = time.time() - start_time
        print(f"✅ 基本相交查询完成:")
        print(f"   - 耗时: {elapsed1:.2f}秒")
        print(f"   - 结果: {len(result1)}条记录")
        print(f"   - 平均处理速度: {city_count/elapsed1:.1f}条记录/秒")
        
        # 如果基本测试很快，测试更复杂的查询
        if elapsed1 < 5:  # 如果基本查询少于5秒
            print("\n📊 测试2: 50米缓冲区查询")
            start_time = time.time()
            
            result2 = joiner.batch_spatial_join_with_remote(
                batch_by_city=True,
                city_ids=[test_city],
                spatial_relation=SpatialRelation.DWITHIN,
                distance_meters=50.0,
                summarize=True,
                summary_fields={
                    "nearby_intersections": "count"
                }
            )
            
            elapsed2 = time.time() - start_time
            print(f"✅ 缓冲区查询完成:")
            print(f"   - 耗时: {elapsed2:.2f}秒")
            print(f"   - 结果: {len(result2)}条记录")
            print(f"   - 平均处理速度: {city_count/elapsed2:.1f}条记录/秒")
        
        # 性能评估
        print(f"\n🎯 性能评估:")
        if elapsed1 < 1:
            print("🔥 性能优秀！处理速度很快")
        elif elapsed1 < 5:
            print("✅ 性能良好，处理速度合理")
        elif elapsed1 < 15:
            print("⚠️  性能一般，可能需要进一步优化")
        else:
            print("❌ 性能较差，需要检查优化效果")
        
        print(f"\n💡 建议:")
        print(f"   - 当前单城市处理速度: {city_count/elapsed1:.1f}条记录/秒")
        
        # 预估大规模处理时间
        if city_count > 0:
            # 假设平均每个城市1000条记录
            avg_city_size = 1000
            estimated_time_per_city = elapsed1 * (avg_city_size / city_count)
            print(f"   - 预估1000条记录的城市处理时间: {estimated_time_per_city:.1f}秒")
            
            # 预估处理所有数据的时间
            with joiner.engine.connect() as conn:
                total_cities = conn.execute(text("SELECT COUNT(DISTINCT city_id) FROM clips_bbox")).scalar()
                total_estimated_time = estimated_time_per_city * total_cities
                print(f"   - 预估处理全部{total_cities}个城市的时间: {total_estimated_time/60:.1f}分钟")
        
    except Exception as e:
        print(f"❌ 性能测试失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    performance_test() 