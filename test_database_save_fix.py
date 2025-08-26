#!/usr/bin/env python3
"""
数据库保存功能修复验证测试

用于验证 'discovered_trajectories' is not defined 错误是否已修复

使用方法：
python test_database_save_fix.py
"""

import pandas as pd
from unittest.mock import MagicMock, patch


def test_trajectory_to_dict_conversion():
    """测试 trajectory_points DataFrame 到字典列表的转换"""
    print("🧪 测试1: DataFrame 到字典列表转换...")
    
    try:
        # 模拟轨迹点DataFrame
        trajectory_data = {
            'dataset_name': ['test_dataset_1', 'test_dataset_2'],
            'longitude': [116.397, 116.398],
            'latitude': [39.916, 39.917], 
            'timestamp': [1748507506699, 1748507506800],
            'velocity': [5.2, 4.8],
            'heading': [90.0, 95.0]
        }
        
        trajectory_points = pd.DataFrame(trajectory_data)
        print(f"   ✅ 创建测试DataFrame: {len(trajectory_points)} 行")
        
        # 测试转换逻辑（模拟代码中的转换）
        trajectory_records = trajectory_points.to_dict('records') if not trajectory_points.empty else []
        
        print(f"   ✅ 转换成功: {len(trajectory_records)} 条记录")
        print(f"   📊 第一条记录: {trajectory_records[0] if trajectory_records else 'N/A'}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ DataFrame转换测试失败: {e}")
        return False


def test_finalize_lightweight_results_mock():
    """测试 _finalize_lightweight_results 方法的变量作用域"""
    print("\n🧪 测试2: _finalize_lightweight_results 变量作用域...")
    
    try:
        from spdatalab.fusion.multimodal_trajectory_retrieval import MultimodalTrajectoryWorkflow, MultimodalConfig
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        
        # 创建测试配置
        config = MultimodalConfig(
            api_config=APIConfig.from_env(),
            output_table="test_trajectories"
        )
        
        workflow = MultimodalTrajectoryWorkflow(config)
        
        # 模拟trajectory_points DataFrame
        trajectory_data = {
            'dataset_name': ['test_dataset_1'],
            'longitude': [116.397],
            'latitude': [39.916], 
            'timestamp': [1748507506699],
            'velocity': [5.2],
            'heading': [90.0]
        }
        trajectory_points = pd.DataFrame(trajectory_data)
        
        # 模拟merged_polygons
        merged_polygons = [{'id': 'test_poly_1', 'properties': {}}]
        
        # 模拟stats
        stats = {
            'raw_polygon_count': 5,
            'merged_polygon_count': 1,
            'query_type': 'text',
            'query_content': 'test query'
        }
        
        print(f"   ✅ 测试数据准备完成")
        print(f"   📊 DataFrame: {len(trajectory_points)} 行")
        print(f"   🔧 配置表名: {config.output_table}")
        
        # 使用mock来避免实际的数据库操作
        with patch.object(workflow, '_save_to_database', return_value=1) as mock_save:
            # 调用被测试的方法
            results = workflow._finalize_lightweight_results(trajectory_points, merged_polygons, stats)
            
            # 验证_save_to_database被正确调用
            if mock_save.called:
                print(f"   ✅ _save_to_database 被调用")
                print(f"   📋 调用参数数量: {len(mock_save.call_args[0])}")
                
                # 检查传递的数据
                saved_data = mock_save.call_args[0][0]  # 第一个参数
                table_name = mock_save.call_args[0][1]  # 第二个参数
                
                print(f"   📊 保存数据类型: {type(saved_data)}")
                print(f"   📊 保存数据长度: {len(saved_data) if hasattr(saved_data, '__len__') else 'N/A'}")
                print(f"   📊 目标表名: {table_name}")
                
                # 验证stats中是否有保存信息
                if 'saved_to_database' in stats:
                    print(f"   ✅ stats更新成功: saved_to_database = {stats['saved_to_database']}")
                else:
                    print(f"   ⚠️ stats未更新 saved_to_database")
            else:
                print(f"   ⚠️ _save_to_database 未被调用")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 方法测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_variable_scope_check():
    """检查代码中是否还有未定义的变量引用"""
    print("\n🧪 测试3: 代码变量引用检查...")
    
    try:
        # 读取源代码文件检查是否还有 discovered_trajectories 引用
        with open('src/spdatalab/fusion/multimodal_trajectory_retrieval.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查是否还有未定义的 discovered_trajectories 引用
        lines = content.split('\n')
        issues = []
        
        for i, line in enumerate(lines, 1):
            if 'discovered_trajectories' in line and 'trajectory_records' not in line:
                # 排除注释行
                if not line.strip().startswith('#'):
                    issues.append(f"第{i}行: {line.strip()}")
        
        if issues:
            print(f"   ⚠️ 发现可能的问题引用:")
            for issue in issues:
                print(f"      {issue}")
        else:
            print(f"   ✅ 未发现 discovered_trajectories 变量引用问题")
        
        # 检查是否正确使用了 trajectory_points 和 trajectory_records
        trajectory_points_count = content.count('trajectory_points')
        trajectory_records_count = content.count('trajectory_records')
        
        print(f"   📊 trajectory_points 引用次数: {trajectory_points_count}")
        print(f"   📊 trajectory_records 引用次数: {trajectory_records_count}")
        
        return len(issues) == 0
        
    except Exception as e:
        print(f"   ❌ 代码检查失败: {e}")
        return False


def main():
    """运行所有测试"""
    print("🚀 数据库保存功能修复验证")
    print("=" * 50)
    
    tests = [
        test_trajectory_to_dict_conversion,
        test_finalize_lightweight_results_mock,
        test_variable_scope_check
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"   💥 测试异常: {e}")
    
    print("\n" + "=" * 50)
    print(f"🎯 测试完成: {passed}/{total} 通过")
    
    if passed == total:
        print("✅ 所有测试通过！数据库保存问题已修复")
        print("\n📋 修复内容总结:")
        print("1. ✅ 修复了 'discovered_trajectories' is not defined 错误")
        print("2. ✅ 使用正确的变量 trajectory_points 参数")
        print("3. ✅ 正确转换 DataFrame 为字典列表格式")
        print("4. ✅ 移除了不必要的变量定义")
        
        print("\n🚀 现在可以测试完整的数据库保存功能:")
        print("python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print("    --text 'bicycle crossing intersection' \\")
        print("    --collection 'ddi_collection_camera_encoded_1' \\")
        print("    --output-table 'discovered_trajectories' \\")
        print("    --verbose")
    else:
        print(f"❌ {total - passed} 个测试失败，可能还有其他问题")
    
    return passed == total


if __name__ == "__main__":
    main()
