#!/usr/bin/env python3
"""
测试training_dataset.json格式输入功能

这个脚本用于测试新增的JSON格式输入功能是否正常工作。
"""

import json
import os
import tempfile
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

def create_test_training_dataset_json():
    """创建测试用的training_dataset.json文件"""
    test_data = {
        "meta": {
            "release_name": "TestDataset_20250915",
            "consumer_version": "v1.2.0",
            "bundle_versions": ["v1.2.0-20250915-test"],
            "created_at": "2025-09-15 15:00:00",
            "description": "测试用的端到端网络联合训练数据集",
            "version": "v1.2.0"
        },
        "dataset_index": [
            {
                "name": "test_scenario_1",
                "obs_path": "obs://test-bucket/test-data/scenario1.jsonl.shrink",
                "bundle_versions": ["v1.2.0-20250915-test"],
                "duplicate": 5
            },
            {
                "name": "test_scenario_2", 
                "obs_path": "obs://test-bucket/test-data/scenario2.jsonl.shrink",
                "bundle_versions": ["v1.2.0-20250915-test"],
                "duplicate": 3
            }
        ]
    }
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
        return f.name

def test_json_input():
    """测试JSON格式输入功能"""
    print("🚀 开始测试JSON格式输入功能...")
    
    try:
        from spdatalab.dataset.dataset_manager import DatasetManager
        
        # 创建测试JSON文件
        json_file = create_test_training_dataset_json()
        print(f"✅ 创建测试JSON文件: {json_file}")
        
        # 测试DatasetManager.build_dataset_from_training_json
        manager = DatasetManager(include_scene_info=False)  # 关闭场景信息获取以避免网络依赖
        
        print("📖 从JSON文件构建数据集...")
        dataset = manager.build_dataset_from_training_json(json_file)
        
        # 验证结果
        print(f"✅ 数据集构建成功!")
        print(f"   - 数据集名称: {dataset.name}")
        print(f"   - 数据集描述: {dataset.description}")
        print(f"   - 子数据集数量: {len(dataset.subdatasets)}")
        print(f"   - 版本: {dataset.metadata.get('version')}")
        print(f"   - 源格式: {dataset.metadata.get('source_format')}")
        
        # 验证子数据集
        for i, sub in enumerate(dataset.subdatasets):
            print(f"   - 子数据集 {i+1}: {sub.name} (倍增: {sub.duplication_factor})")
        
        # 测试保存功能
        output_file = tempfile.mktemp(suffix='.json')
        manager.save_dataset(dataset, output_file, format='json')
        print(f"✅ 数据集保存成功: {output_file}")
        
        # 清理临时文件
        os.unlink(json_file)
        os.unlink(output_file)
        
        print("🎉 JSON格式输入功能测试通过!")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_cli_command():
    """测试CLI命令"""
    print("\n🚀 测试CLI命令...")
    
    try:
        # 创建测试JSON文件
        json_file = create_test_training_dataset_json()
        output_file = tempfile.mktemp(suffix='.json')
        
        print(f"✅ 创建测试文件: {json_file}")
        
        # 构建CLI命令
        cmd = [
            sys.executable, "-m", "spdatalab.cli", "build-dataset",
            "--training-dataset-json", json_file,
            "--output", output_file
        ]
        
        print(f"📝 执行命令: {' '.join(cmd)}")
        
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_root)
        
        if result.returncode == 0:
            print("✅ CLI命令执行成功!")
            print("输出:")
            print(result.stdout)
            
            # 验证输出文件是否存在
            if os.path.exists(output_file):
                print(f"✅ 输出文件创建成功: {output_file}")
                with open(output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"   - 数据集名称: {data['name']}")
                    print(f"   - 子数据集数量: {len(data['subdatasets'])}")
            else:
                print("❌ 输出文件未找到")
                return False
        else:
            print("❌ CLI命令执行失败!")
            print("错误输出:")
            print(result.stderr)
            return False
        
        # 清理临时文件
        os.unlink(json_file)
        if os.path.exists(output_file):
            os.unlink(output_file)
        
        print("🎉 CLI命令测试通过!")
        return True
        
    except Exception as e:
        print(f"❌ CLI测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 Training Dataset JSON 格式输入功能测试")
    print("=" * 60)
    
    success = True
    
    # 测试Python API
    success &= test_json_input()
    
    # 测试CLI命令  
    success &= test_cli_command()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 所有测试通过! JSON格式输入功能实现成功!")
    else:
        print("❌ 部分测试失败，请检查实现")
    print("=" * 60)
    
    sys.exit(0 if success else 1)
