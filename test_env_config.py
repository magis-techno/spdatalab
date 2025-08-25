#!/usr/bin/env python3
"""
测试环境变量配置更新的脚本
验证新的APIConfig.from_env()方法和相关功能
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))


def test_api_config_from_env():
    """测试APIConfig.from_env()方法"""
    print("🔧 测试APIConfig.from_env()方法...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        
        # 测试必需变量缺失的情况
        print("  测试1: 缺少必需变量...")
        try:
            config = APIConfig.from_env()
            print("❌ 应该抛出RuntimeError，但没有")
            return False
        except RuntimeError as e:
            print(f"✅ 正确抛出RuntimeError: {e}")
        
        # 测试完整配置
        print("  测试2: 完整配置...")
        test_env = {
            'MULTIMODAL_PROJECT': 'test_project',
            'MULTIMODAL_API_KEY': 'test_key',
            'MULTIMODAL_USERNAME': 'test_user',
            'MULTIMODAL_API_URL': 'https://test-api.example.com/xmodalitys',
            'MULTIMODAL_TIMEOUT': '45',
            'MULTIMODAL_MAX_RETRIES': '5'
        }
        
        with patch.dict(os.environ, test_env):
            config = APIConfig.from_env()
            
            assert config.project == 'test_project'
            assert config.api_key == 'test_key'
            assert config.username == 'test_user'
            assert config.api_url == 'https://test-api.example.com/xmodalitys'
            assert config.timeout == 45
            assert config.max_retries == 5
            
            print("✅ 完整配置测试成功")
        
        # 测试部分配置（使用默认值）
        print("  测试3: 部分配置（使用默认值）...")
        minimal_env = {
            'MULTIMODAL_PROJECT': 'test_project',
            'MULTIMODAL_API_KEY': 'test_key',
            'MULTIMODAL_USERNAME': 'test_user'
        }
        
        with patch.dict(os.environ, minimal_env, clear=True):
            config = APIConfig.from_env()
            
            assert config.project == 'test_project'
            assert config.api_key == 'test_key'
            assert config.username == 'test_user'
            assert config.api_url == 'https://driveinsight-api.ias.huawei.com/xmodalitys'  # 默认值
            assert config.timeout == 30  # 默认值
            assert config.max_retries == 3  # 默认值
            
            print("✅ 部分配置测试成功（默认值正确）")
        
        return True
        
    except Exception as e:
        print(f"❌ APIConfig.from_env()测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cli_config_integration():
    """测试CLI配置集成"""
    print("\n🔧 测试CLI配置集成...")
    
    try:
        from spdatalab.fusion.multimodal_cli import get_api_config_from_env
        
        # 测试缺少配置的情况
        print("  测试1: 缺少配置时的错误处理...")
        with patch.dict(os.environ, {}, clear=True):
            try:
                get_api_config_from_env()
                print("❌ 应该调用sys.exit(1)，但没有")
                return False
            except SystemExit as e:
                if e.code == 1:
                    print("✅ 正确调用sys.exit(1)")
                else:
                    print(f"❌ 错误的退出码: {e.code}")
                    return False
        
        # 测试正确配置的情况
        print("  测试2: 正确配置...")
        test_env = {
            'MULTIMODAL_PROJECT': 'test_project',
            'MULTIMODAL_API_KEY': 'test_key',
            'MULTIMODAL_USERNAME': 'test_user'
        }
        
        with patch.dict(os.environ, test_env):
            config = get_api_config_from_env()
            assert config.project == 'test_project'
            assert config.api_key == 'test_key'
            assert config.username == 'test_user'
            print("✅ CLI配置集成测试成功")
        
        return True
        
    except Exception as e:
        print(f"❌ CLI配置集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_workflow_integration():
    """测试工作流集成"""
    print("\n🔧 测试工作流配置集成...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        from spdatalab.fusion.multimodal_trajectory_retrieval import (
            MultimodalConfig,
            MultimodalTrajectoryWorkflow
        )
        
        # 创建测试环境
        test_env = {
            'MULTIMODAL_PROJECT': 'test_project',
            'MULTIMODAL_API_KEY': 'test_key',
            'MULTIMODAL_USERNAME': 'test_user',
            'MULTIMODAL_API_URL': 'https://test-api.example.com/xmodalitys'
        }
        
        with patch.dict(os.environ, test_env):
            # 测试从环境变量创建配置
            api_config = APIConfig.from_env()
            multimodal_config = MultimodalConfig(api_config=api_config)
            
            # 测试工作流创建
            workflow = MultimodalTrajectoryWorkflow(multimodal_config)
            
            # 验证配置传递
            assert workflow.retriever.api_config.project == 'test_project'
            assert workflow.retriever.api_config.api_url == 'https://test-api.example.com/xmodalitys'
            
            print("✅ 工作流配置集成测试成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 工作流集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_environment_variable_precedence():
    """测试环境变量优先级"""
    print("\n🔧 测试环境变量优先级...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        
        # 测试环境变量覆盖默认值
        custom_env = {
            'MULTIMODAL_PROJECT': 'test_project',
            'MULTIMODAL_API_KEY': 'test_key',
            'MULTIMODAL_USERNAME': 'test_user',
            'MULTIMODAL_API_URL': 'https://custom-api.example.com/xmodalitys',
            'MULTIMODAL_TIMEOUT': '60',
            'MULTIMODAL_MAX_RETRIES': '10'
        }
        
        with patch.dict(os.environ, custom_env):
            config = APIConfig.from_env()
            
            # 验证自定义值覆盖了默认值
            assert config.api_url == 'https://custom-api.example.com/xmodalitys'
            assert config.timeout == 60
            assert config.max_retries == 10
            
            print("✅ 环境变量正确覆盖默认值")
        
        return True
        
    except Exception as e:
        print(f"❌ 环境变量优先级测试失败: {e}")
        return False


def main():
    """运行所有测试"""
    print("🚀 开始环境变量配置更新测试")
    print("="*60)
    
    tests = [
        test_api_config_from_env,
        test_cli_config_integration,
        test_workflow_integration,
        test_environment_variable_precedence
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ 测试 {test_func.__name__} 异常: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"🎯 测试完成: {passed} 个通过, {failed} 个失败")
    
    if failed == 0:
        print("🎉 所有环境变量配置测试通过！")
        print("\n📋 新功能可用：")
        print("1. ✅ APIConfig.from_env() 方法")
        print("2. ✅ 支持自定义API URL")
        print("3. ✅ 支持自定义超时和重试次数")
        print("4. ✅ CLI自动环境变量配置")
        print("5. ✅ 详细的配置错误提示")
        return True
    else:
        print("⚠️ 部分测试失败，需要修复")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
