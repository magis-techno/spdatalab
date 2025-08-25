#!/usr/bin/env python3
"""
验证导入修复是否成功的测试脚本
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

def test_import_fix():
    """测试导入修复"""
    print("🔧 测试导入修复...")
    
    try:
        # 测试基础数据检索模块导入
        from spdatalab.dataset.multimodal_data_retriever import (
            APIConfig,
            MultimodalRetriever,
            TrajectoryToPolygonConverter,
            APIRetryStrategy
        )
        print("✅ multimodal_data_retriever 模块导入成功")
        
        # 测试创建对象（不实际调用API）
        api_config = APIConfig(
            project="test_project",
            api_key="test_key",
            username="test_user"
        )
        print("✅ APIConfig 创建成功")
        
        retriever = MultimodalRetriever(api_config)
        print("✅ MultimodalRetriever 创建成功")
        
        converter = TrajectoryToPolygonConverter()
        print("✅ TrajectoryToPolygonConverter 创建成功")
        
        retry_strategy = APIRetryStrategy()
        print("✅ APIRetryStrategy 创建成功")
        
        return True
        
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 其他错误: {e}")
        return False

def test_fusion_import():
    """测试融合模块导入"""
    print("\n🔧 测试融合模块导入...")
    
    try:
        from spdatalab.fusion.multimodal_trajectory_retrieval import (
            MultimodalConfig,
            MultimodalTrajectoryWorkflow,
            ResultAggregator,
            PolygonMerger
        )
        print("✅ multimodal_trajectory_retrieval 模块导入成功")
        
        return True
        
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 其他错误: {e}")
        return False

def main():
    """主测试函数"""
    print("🚀 开始验证导入修复")
    print("="*50)
    
    test1 = test_import_fix()
    test2 = test_fusion_import()
    
    print("\n" + "="*50)
    if test1 and test2:
        print("🎉 所有导入测试通过！修复成功")
        print("\n📋 可以尝试运行完整命令：")
        print("python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print('    --text "bicycle crossing intersection" \\')
        print('    --collection "ddi_collection_camera_encoded_1" \\')
        print('    --output-table "discovered_trajectories"')
        return True
    else:
        print("❌ 导入测试失败，需要进一步检查")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
