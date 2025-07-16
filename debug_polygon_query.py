#!/usr/bin/env python3
"""
调试脚本：诊断 HighPerformancePolygonTrajectoryQuery.process_complete_workflow 方法问题
"""

import logging
import sys
import inspect
from pathlib import Path

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def debug_class_structure():
    """调试类结构和方法"""
    print("=" * 80)
    print("🔍 开始调试 HighPerformancePolygonTrajectoryQuery 类")
    print("=" * 80)
    
    try:
        # 导入模块
        print("📥 步骤1: 导入模块...")
        from src.spdatalab.dataset.polygon_trajectory_query import (
            HighPerformancePolygonTrajectoryQuery,
            PolygonTrajectoryConfig,
            process_polygon_trajectory_query
        )
        print("✅ 模块导入成功")
        
        # 检查类定义
        print("\n📋 步骤2: 检查类定义...")
        print(f"类名: {HighPerformancePolygonTrajectoryQuery.__name__}")
        print(f"类文件: {inspect.getfile(HighPerformancePolygonTrajectoryQuery)}")
        print(f"类源码行数: {len(inspect.getsourcelines(HighPerformancePolygonTrajectoryQuery)[0])}")
        
        # 列出所有方法
        print("\n📝 步骤3: 列出所有类方法...")
        all_methods = dir(HighPerformancePolygonTrajectoryQuery)
        public_methods = [method for method in all_methods if not method.startswith('_')]
        private_methods = [method for method in all_methods if method.startswith('_') and not method.startswith('__')]
        
        print(f"公共方法 ({len(public_methods)}): {public_methods}")
        print(f"私有方法 ({len(private_methods)}): {private_methods}")
        
        # 特别检查 process_complete_workflow
        print("\n🎯 步骤4: 检查 process_complete_workflow 方法...")
        has_method = hasattr(HighPerformancePolygonTrajectoryQuery, 'process_complete_workflow')
        print(f"process_complete_workflow 方法存在: {has_method}")
        
        if has_method:
            method = getattr(HighPerformancePolygonTrajectoryQuery, 'process_complete_workflow')
            print(f"方法类型: {type(method)}")
            print(f"是否可调用: {callable(method)}")
            print(f"方法签名: {inspect.signature(method)}")
            
            # 获取方法源码的前几行
            try:
                source_lines = inspect.getsourcelines(method)[0][:5]
                print("方法源码前5行:")
                for i, line in enumerate(source_lines, 1):
                    print(f"  {i}: {line.rstrip()}")
            except Exception as e:
                print(f"无法获取源码: {e}")
        
        # 创建实例测试
        print("\n🚀 步骤5: 创建实例测试...")
        config = PolygonTrajectoryConfig()
        instance = HighPerformancePolygonTrajectoryQuery(config)
        print(f"实例创建成功: {type(instance)}")
        
        # 检查实例方法
        instance_has_method = hasattr(instance, 'process_complete_workflow')
        print(f"实例的 process_complete_workflow 方法存在: {instance_has_method}")
        
        if instance_has_method:
            instance_method = getattr(instance, 'process_complete_workflow')
            print(f"实例方法类型: {type(instance_method)}")
            print(f"实例方法可调用: {callable(instance_method)}")
        
        # 测试函数调用
        print("\n🧪 步骤6: 测试函数调用...")
        test_geojson = "test.geojson"
        
        # 创建临时测试文件
        test_data = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"id": "test"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[116.3, 39.9], [116.4, 39.9], [116.4, 40.0], [116.3, 40.0], [116.3, 39.9]]]
                }
            }]
        }
        
        import json
        with open(test_geojson, 'w', encoding='utf-8') as f:
            json.dump(test_data, f)
        
        print(f"创建测试文件: {test_geojson}")
        
        # 尝试直接调用实例方法
        if instance_has_method:
            print("尝试直接调用实例方法...")
            try:
                result = instance.process_complete_workflow(test_geojson)
                print(f"✅ 直接调用成功: {type(result)}")
            except Exception as e:
                print(f"❌ 直接调用失败: {e}")
        
        # 尝试通过封装函数调用
        print("尝试通过封装函数调用...")
        try:
            result = process_polygon_trajectory_query(test_geojson)
            print(f"✅ 封装函数调用成功: {type(result)}")
        except Exception as e:
            print(f"❌ 封装函数调用失败: {e}")
            import traceback
            traceback.print_exc()
        
        # 清理测试文件
        Path(test_geojson).unlink(missing_ok=True)
        
        print("\n" + "=" * 80)
        print("🎉 调试完成")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ 调试过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_class_structure() 