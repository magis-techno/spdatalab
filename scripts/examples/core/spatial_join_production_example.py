"""
生产级空间连接使用示例

展示如何使用 spatial_join_production.py 进行高效的polygon相交查询
"""

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin, 
    SpatialJoinConfig, 
    quick_spatial_join
)
import logging
import time

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def example_basic_usage():
    """基础使用示例"""
    print("🚀 基础使用示例")
    print("=" * 50)
    
    # 方式1: 最简单的调用
    result, stats = quick_spatial_join(num_bbox=50)
    
    print(f"处理了 {stats['bbox_count']} 个bbox")
    print(f"使用策略: {stats['strategy']}")
    print(f"总耗时: {stats['total_time']:.2f}秒")
    print(f"处理速度: {stats['speed_bbox_per_sec']:.1f} bbox/秒")
    print(f"结果数量: {stats['result_count']}")
    
    if len(result) > 0:
        print("\n前5个结果:")
        print(result.head().to_string(index=False))

def example_custom_config():
    """自定义配置示例"""
    print("\n🔧 自定义配置示例")
    print("=" * 50)
    
    # 自定义配置
    custom_config = SpatialJoinConfig(
        batch_threshold=150,  # 150个bbox以下用批量查询
        chunk_size=30,        # 分块大小30
        max_timeout_seconds=600  # 10分钟超时
    )
    
    # 使用自定义配置
    spatial_join = ProductionSpatialJoin(custom_config)
    result, stats = spatial_join.polygon_intersect(
        num_bbox=200,
        city_filter=None  # 可以指定城市过滤
    )
    
    print(f"自定义配置结果:")
    print(f"  策略: {stats['strategy']}")
    print(f"  分块大小: {stats.get('chunk_size', 'N/A')}")
    print(f"  耗时: {stats['total_time']:.2f}秒")

def example_city_filtering():
    """城市过滤示例"""
    print("\n🏙️ 城市过滤示例")
    print("=" * 50)
    
    # 指定城市进行查询
    city_result, city_stats = quick_spatial_join(
        num_bbox=100,
        city_filter="boston-seaport"  # 替换为你的城市ID
    )
    
    print(f"城市过滤查询:")
    print(f"  目标城市: {city_stats['city_filter']}")
    print(f"  实际处理: {city_stats['bbox_count']} 个bbox")
    print(f"  策略: {city_stats['strategy']}")
    print(f"  耗时: {city_stats['total_time']:.2f}秒")

def example_performance_comparison():
    """性能对比示例"""
    print("\n📊 性能对比示例")
    print("=" * 50)
    
    test_sizes = [10, 50, 100, 200, 500]
    
    for size in test_sizes:
        print(f"\n测试 {size} 个bbox:")
        
        start_time = time.time()
        result, stats = quick_spatial_join(size)
        
        print(f"  策略: {stats['strategy']:<12} | "
              f"耗时: {stats['total_time']:.2f}s | "
              f"速度: {stats['speed_bbox_per_sec']:.1f} bbox/s | "
              f"结果: {stats['result_count']}条")
        
        # 如果单次查询超过30秒就停止
        if stats['total_time'] > 30:
            print("  ⚠️ 耗时过长，停止更大规模测试")
            break

def example_error_handling():
    """错误处理示例"""
    print("\n🛡️ 错误处理示例")
    print("=" * 50)
    
    try:
        # 尝试一个可能失败的查询
        result, stats = quick_spatial_join(
            num_bbox=1000000,  # 极大数量可能导致问题
            city_filter="non-existent-city"
        )
        
        if stats['result_count'] == 0:
            print("⚠️ 查询成功但未返回结果，请检查过滤条件")
        else:
            print(f"✅ 成功处理大规模查询: {stats['result_count']}条结果")
            
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        print("💡 建议：检查数据库连接、减少查询规模或调整参数")

def example_production_workflow():
    """生产环境工作流示例"""
    print("\n🏭 生产环境工作流示例")
    print("=" * 50)
    
    # 生产环境配置
    prod_config = SpatialJoinConfig(
        batch_threshold=200,
        chunk_size=50,
        max_timeout_seconds=300
    )
    
    spatial_join = ProductionSpatialJoin(prod_config)
    
    # 模拟不同业务场景
    scenarios = [
        ("小批量实时查询", 20),
        ("中等批量分析", 150),
        ("大批量处理", 800)
    ]
    
    for scenario_name, num_bbox in scenarios:
        print(f"\n📋 场景: {scenario_name} ({num_bbox} bbox)")
        
        try:
            result, stats = spatial_join.polygon_intersect(num_bbox)
            
            # 业务逻辑判断
            if stats['total_time'] < 5:
                performance_grade = "🟢 优秀"
            elif stats['total_time'] < 15:
                performance_grade = "🟡 良好"
            else:
                performance_grade = "🔴 需优化"
            
            print(f"  性能评级: {performance_grade}")
            print(f"  处理策略: {stats['strategy']}")
            print(f"  处理时间: {stats['total_time']:.2f}秒")
            print(f"  处理速度: {stats['speed_bbox_per_sec']:.1f} bbox/秒")
            
            # 保存结果到文件（示例）
            if len(result) > 0:
                output_file = f"spatial_join_result_{scenario_name.replace(' ', '_')}.csv"
                result.to_csv(output_file, index=False)
                print(f"  结果已保存: {output_file}")
                
        except Exception as e:
            print(f"  ❌ 场景失败: {e}")

if __name__ == "__main__":
    print("🎯 生产级空间连接解决方案示例")
    print("="*80)
    
    # 运行所有示例
    try:
        example_basic_usage()
        example_custom_config()
        example_city_filtering()
        example_performance_comparison()
        example_error_handling()
        example_production_workflow()
        
    except KeyboardInterrupt:
        print("\n\n⏹️ 示例被用户中断")
    except Exception as e:
        print(f"\n\n❌ 示例运行失败: {e}")
    
    print("\n✅ 示例运行完成！")
    print("💡 现在你可以根据需要使用 spatial_join_production.py 模块") 