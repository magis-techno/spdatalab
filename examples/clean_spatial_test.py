"""
简洁的空间连接测试
==================

减少冗余输出，专注于验证核心功能
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

# 设置简洁的日志级别
logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin, 
    SpatialJoinConfig,
    build_cache,
    analyze_cached_intersections
)

def check_data_availability():
    """检查数据可用性"""
    print("📊 检查数据可用性...")
    
    try:
        config = SpatialJoinConfig(enable_cache_table=False)
        spatial_join = ProductionSpatialJoin(config)
        
        from sqlalchemy import text
        
        # 检查bbox数据
        with spatial_join.local_engine.connect() as conn:
            bbox_count_sql = text("SELECT COUNT(*) FROM clips_bbox")
            bbox_count = conn.execute(bbox_count_sql).fetchone()[0]
            print(f"  📦 bbox数据: {bbox_count} 条")
        
        # 检查路口数据
        with spatial_join.remote_engine.connect() as conn:
            intersection_count_sql = text("SELECT COUNT(*) FROM full_intersection")
            intersection_count = conn.execute(intersection_count_sql).fetchone()[0]
            print(f"  🚦 路口数据: {intersection_count} 条")
        
        return bbox_count > 0 and intersection_count > 0
        
    except Exception as e:
        print(f"❌ 数据检查失败: {e}")
        return False

def test_basic_spatial_join():
    """测试基本空间连接功能"""
    print("\n🔗 测试基本空间连接...")
    
    try:
        from src.spdatalab.fusion.spatial_join_production import quick_spatial_join
        
        # 测试小规模查询
        result, stats = quick_spatial_join(5)  # 只测试5个bbox
        
        print(f"✅ 基本查询成功")
        print(f"  - 处理: {stats['bbox_count']} 个bbox")
        print(f"  - 策略: {stats['strategy']}")
        print(f"  - 耗时: {stats['total_time']:.2f}秒")
        print(f"  - 结果: {len(result)} 条记录")
        
        return len(result) > 0
        
    except Exception as e:
        print(f"❌ 基本查询失败: {e}")
        return False

def test_cache_functionality():
    """测试缓存功能"""
    print("\n💾 测试缓存功能...")
    
    try:
        # 1. 构建小规模缓存
        print("  📊 构建缓存...")
        cached_count, build_stats = build_cache(
            num_bbox=5,  # 只测试5个bbox
            city_filter=None,
            force_rebuild=True
        )
        
        if cached_count == 0:
            print("⚠️  没有生成缓存数据，可能bbox和路口没有相交")
            return False
        
        print(f"✅ 缓存构建成功")
        print(f"  - 缓存记录: {cached_count} 条")
        print(f"  - 构建耗时: {build_stats['build_time']:.2f}秒")
        
        # 2. 测试缓存查询
        print("  📈 测试缓存查询...")
        analysis_result = analyze_cached_intersections()
        
        if not analysis_result.empty:
            print(f"✅ 缓存查询成功")
            print(f"  - 总相交记录: {analysis_result.iloc[0]['total_intersections']}")
            print(f"  - 唯一路口: {analysis_result.iloc[0]['unique_intersections']}")
            print(f"  - 唯一场景: {analysis_result.iloc[0]['unique_scenes']}")
        
        # 3. 测试分组查询
        print("  🎯 测试分组查询...")
        type_analysis = analyze_cached_intersections(group_by=["intersectiontype"])
        
        if not type_analysis.empty:
            print(f"✅ 分组查询成功")
            print(f"  - 路口类型数: {len(type_analysis)}")
            print("  - 前3个类型:")
            for _, row in type_analysis.head(3).iterrows():
                print(f"    * 类型{row['intersectiontype']}: {row['intersection_count']}个相交")
        
        return True
        
    except Exception as e:
        print(f"❌ 缓存功能测试失败: {e}")
        return False

def test_performance_comparison():
    """简单的性能对比"""
    print("\n⚡ 性能对比测试...")
    
    try:
        import time
        from src.spdatalab.fusion.spatial_join_production import quick_spatial_join
        
        # 实时查询测试
        print("  🔄 实时查询测试...")
        start_time = time.time()
        result, stats = quick_spatial_join(3)  # 只测试3个bbox
        realtime_time = time.time() - start_time
        
        print(f"    实时查询: {realtime_time:.2f}秒")
        
        # 缓存查询测试（如果有缓存）
        print("  💾 缓存查询测试...")
        start_time = time.time()
        try:
            cached_result = analyze_cached_intersections()
            cache_time = time.time() - start_time
            print(f"    缓存查询: {cache_time:.4f}秒")
            
            if cache_time > 0:
                speedup = realtime_time / cache_time
                print(f"  💡 性能提升: {speedup:.1f}倍")
        except:
            print("    缓存查询跳过（无缓存数据）")
        
        return True
        
    except Exception as e:
        print(f"❌ 性能对比失败: {e}")
        return False

def main():
    """主测试流程"""
    print("🌟 空间连接功能测试")
    print("=" * 50)
    
    # 测试1: 数据可用性
    data_ok = check_data_availability()
    if not data_ok:
        print("❌ 数据不可用，请检查数据库连接和数据")
        return
    
    # 测试2: 基本功能
    basic_ok = test_basic_spatial_join()
    
    # 测试3: 缓存功能
    cache_ok = test_cache_functionality() if basic_ok else False
    
    # 测试4: 性能对比
    perf_ok = test_performance_comparison() if basic_ok else False
    
    # 总结
    print(f"\n" + "=" * 50)
    print("📊 测试总结:")
    print(f"  ✅ 数据可用性: {'通过' if data_ok else '失败'}")
    print(f"  ✅ 基本功能: {'通过' if basic_ok else '失败'}")
    print(f"  ✅ 缓存功能: {'通过' if cache_ok else '失败'}")
    print(f"  ✅ 性能对比: {'通过' if perf_ok else '失败'}")
    
    if all([data_ok, basic_ok, cache_ok]):
        print(f"\n🎉 所有功能正常！空间连接模块可以使用。")
        print(f"\n📋 下一步可以:")
        print(f"  1. 使用 build_cache(num_bbox, force_rebuild=True) 构建大规模缓存")
        print(f"  2. 使用 analyze_cached_intersections() 进行各种分析")
        print(f"  3. 根据需要调整 group_by 参数进行不同维度的统计")
    else:
        print(f"\n⚠️  部分功能存在问题，建议:")
        if not data_ok:
            print(f"  - 检查数据库连接和数据表")
        if not basic_ok:
            print(f"  - 检查空间查询功能")
        if not cache_ok:
            print(f"  - 检查缓存表创建和权限")

if __name__ == "__main__":
    main() 