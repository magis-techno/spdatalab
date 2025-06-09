"""
测试字段修复后的功能
==================

验证intersectiontype字段的使用是否正确
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

def test_field_fix():
    """测试字段修复"""
    print("🔧 测试字段修复后的功能")
    print("=" * 50)
    
    try:
        from src.spdatalab.fusion.spatial_join_production import (
            ProductionSpatialJoin, 
            SpatialJoinConfig,
            quick_spatial_join,
            build_cache,
            analyze_cached_intersections
        )
        
        # 1. 基本查询测试
        print("📊 测试基本查询...")
        result, stats = quick_spatial_join(3)
        print(f"✅ 基本查询成功: {len(result)} 条结果")
        
        # 2. 构建小规模缓存
        print("\n💾 测试缓存构建...")
        cached_count, build_stats = build_cache(
            num_bbox=3,
            force_rebuild=True
        )
        
        if cached_count > 0:
            print(f"✅ 缓存构建成功: {cached_count} 条记录")
            
            # 3. 测试按intersectiontype分组
            print("\n🎯 测试intersectiontype分组...")
            type_analysis = analyze_cached_intersections(group_by=["intersectiontype"])
            
            if not type_analysis.empty:
                print(f"✅ 分组查询成功")
                print("路口类型分布:")
                for _, row in type_analysis.iterrows():
                    print(f"  类型 {row['intersectiontype']}: {row['intersection_count']} 个相交")
                
                # 4. 测试特定类型过滤
                unique_types = type_analysis['intersectiontype'].tolist()
                if unique_types:
                    print(f"\n🔍 测试类型过滤...")
                    first_type = unique_types[0]
                    filtered_result = analyze_cached_intersections(
                        intersection_types=[first_type]
                    )
                    
                    if not filtered_result.empty:
                        print(f"✅ 类型过滤成功: 类型{first_type}有{filtered_result.iloc[0]['total_intersections']}个相交")
                    else:
                        print("⚠️  类型过滤结果为空")
            else:
                print("⚠️  分组查询结果为空")
        else:
            print("⚠️  没有生成缓存数据")
        
        print(f"\n🎉 字段修复测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_field_fix() 