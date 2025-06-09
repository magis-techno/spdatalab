"""
快速测试脚本
============

验证基本的空间连接功能是否能正常工作
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

def test_import():
    """测试基本导入"""
    print("📦 测试导入...")
    try:
        from src.spdatalab.fusion.spatial_join_production import (
            ProductionSpatialJoin, 
            SpatialJoinConfig,
            quick_spatial_join
        )
        print("✅ 导入成功")
        return True
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_basic_query():
    """测试基本查询功能"""
    print("\n🔍 测试基本查询...")
    try:
        from src.spdatalab.fusion.spatial_join_production import quick_spatial_join
        
        # 测试小规模查询（不使用city_filter）
        result, stats = quick_spatial_join(5)  # 只测试5个bbox
        
        print(f"✅ 查询成功！")
        print(f"  - 处理了 {stats['bbox_count']} 个bbox")
        print(f"  - 耗时 {stats['total_time']:.2f} 秒")
        print(f"  - 使用策略: {stats['strategy']}")
        print(f"  - 返回结果: {len(result)} 条")
        
        if len(result) > 0:
            print("📊 前3个结果:")
            print(result.head(3).to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_cache_table():
    """测试缓存表创建"""
    print("\n💾 测试缓存表...")
    try:
        from src.spdatalab.fusion.spatial_join_production import ProductionSpatialJoin, SpatialJoinConfig
        
        config = SpatialJoinConfig(enable_cache_table=True)
        spatial_join = ProductionSpatialJoin(config)
        
        print("✅ 缓存表初始化成功")
        
        # 测试基本缓存操作
        count = spatial_join._get_cached_count()
        print(f"  - 当前缓存记录: {count}")
        
        return True
        
    except Exception as e:
        print(f"❌ 缓存表测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主测试函数"""
    print("🚀 快速功能测试")
    print("=" * 40)
    
    # 测试1: 导入
    import_ok = test_import()
    if not import_ok:
        print("❌ 导入失败，无法继续测试")
        return
    
    # 测试2: 基本查询
    query_ok = test_basic_query()
    
    # 测试3: 缓存表
    cache_ok = test_cache_table()
    
    # 总结
    print(f"\n" + "=" * 40)
    print("📊 测试总结:")
    print(f"  ✅ 导入: {'通过' if import_ok else '失败'}")
    print(f"  ✅ 基本查询: {'通过' if query_ok else '失败'}")
    print(f"  ✅ 缓存表: {'通过' if cache_ok else '失败'}")
    
    if all([import_ok, query_ok, cache_ok]):
        print(f"\n🎉 基本功能正常！可以开始使用空间连接功能。")
    else:
        print(f"\n⚠️  部分功能存在问题，请根据错误信息进行调试。")

if __name__ == "__main__":
    main() 