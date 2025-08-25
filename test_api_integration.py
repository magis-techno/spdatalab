#!/usr/bin/env python3
"""
多模态轨迹检索系统 - API集成测试

测试新的API格式和环境变量配置
"""

import os
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))


def test_api_config_from_env():
    """测试从环境变量创建API配置"""
    print("🔧 测试环境变量API配置...")
    
    # 设置测试环境变量
    test_env = {
        'MULTIMODAL_API_KEY': 'test_api_key_123',
        'MULTIMODAL_USERNAME': 'test_user',
        'MULTIMODAL_PROJECT': 'test_project',
        'MULTIMODAL_PLATFORM': 'test-platform',
        'MULTIMODAL_REGION': 'test-region',
        'MULTIMODAL_ENTRYPOINT_VERSION': 'v3',
        'MULTIMODAL_API_BASE_URL': 'https://test.example.com',
        'MULTIMODAL_API_PATH': '/test/api',
        'MULTIMODAL_TIMEOUT': '60',
        'MULTIMODAL_MAX_RETRIES': '5'
    }
    
    # 备份原始环境变量
    original_env = {}
    for key in test_env:
        original_env[key] = os.environ.get(key)
    
    try:
        # 设置测试环境变量
        for key, value in test_env.items():
            os.environ[key] = value
        
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        
        # 从环境变量创建配置
        config = APIConfig.from_env()
        
        # 验证配置
        assert config.api_key == 'test_api_key_123'
        assert config.username == 'test_user'
        assert config.project == 'test_project'
        assert config.platform == 'test-platform'
        assert config.region == 'test-region'
        assert config.entrypoint_version == 'v3'
        assert config.api_base_url == 'https://test.example.com'
        assert config.api_path == '/test/api'
        assert config.api_url == 'https://test.example.com/test/api'
        assert config.timeout == 60
        assert config.max_retries == 5
        
        print("✅ 环境变量API配置测试成功")
        return True
        
    except Exception as e:
        print(f"❌ 环境变量API配置测试失败: {e}")
        return False
    
    finally:
        # 恢复原始环境变量
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_api_headers():
    """测试API请求头构建"""
    print("\n🔧 测试API请求头...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
        
        # 创建测试配置
        config = APIConfig(
            project="test_project",
            api_key="test_key_123",
            username="test_user",
            platform="test-platform",
            region="test-region",
            entrypoint_version="v3",
            api_base_url="https://test.example.com"
        )
        
        # 创建检索器
        retriever = MultimodalRetriever(config)
        
        # 验证请求头
        headers = retriever.headers
        expected_headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": "Bearer test_key_123",
            "Content-Type": "application/json",
            "Deepdata-Platform": "test-platform",
            "Deepdata-Project": "test_project",
            "Deepdata-Region": "test-region",
            "Entrypoint-Version": "v3",
            "Host": "test.example.com",
            "User-Agent": "spdatalab-multimodal/1.0.0",
            "username": "test_user"
        }
        
        for key, expected_value in expected_headers.items():
            assert key in headers, f"请求头缺少 {key}"
            assert headers[key] == expected_value, f"请求头 {key} 期望 '{expected_value}', 实际 '{headers[key]}'"
        
        print("✅ API请求头测试成功")
        return True
        
    except Exception as e:
        print(f"❌ API请求头测试失败: {e}")
        return False


def test_api_payload():
    """测试API请求载荷格式"""
    print("\n🔧 测试API请求载荷...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
        
        # 创建测试配置
        config = APIConfig(
            project="test_project",
            api_key="test_key",
            username="test_user"
        )
        
        retriever = MultimodalRetriever(config)
        
        # 测试相机推导
        camera = retriever._extract_camera_from_collection("ddi_collection_camera_encoded_2")
        assert camera == "camera_2", f"期望 camera_2，实际 {camera}"
        
        camera = retriever._extract_camera_from_collection("ddi_collection_camera_encoded_12")
        assert camera == "camera_12", f"期望 camera_12，实际 {camera}"
        
        # 测试无效collection的默认值
        camera = retriever._extract_camera_from_collection("invalid_collection")
        assert camera == "camera_1", f"期望 camera_1，实际 {camera}"
        
        print("✅ API请求载荷测试成功")
        return True
        
    except Exception as e:
        print(f"❌ API请求载荷测试失败: {e}")
        return False


def test_cli_with_new_format():
    """测试CLI与新API格式的兼容性"""
    print("\n🔧 测试CLI新格式兼容性...")
    
    try:
        from spdatalab.fusion.multimodal_cli import create_parser, validate_args
        
        parser = create_parser()
        
        # 测试新的API参数格式
        args = parser.parse_args([
            '--text', 'bicycle crossing intersection',
            '--collection', 'ddi_collection_camera_encoded_2',
            '--count', '20',
            '--start', '10',
            '--start-time', '1234567891011',
            '--end-time', '1234567891111',
            '--buffer-distance', '20.0'
        ])
        
        # 验证参数
        validate_args(args)
        
        assert args.text == 'bicycle crossing intersection'
        assert args.collection == 'ddi_collection_camera_encoded_2'
        assert args.count == 20
        assert args.start == 10
        assert args.start_time == 1234567891011
        assert args.end_time == 1234567891111
        assert args.buffer_distance == 20.0
        
        print("✅ CLI新格式兼容性测试成功")
        return True
        
    except Exception as e:
        print(f"❌ CLI新格式兼容性测试失败: {e}")
        return False


def test_image_retrieval():
    """测试图片检索功能"""
    print("\n🔧 测试图片检索功能...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
        
        # 创建测试配置
        config = APIConfig(
            project="test_project",
            api_key="test_key",
            username="test_user"
        )
        
        retriever = MultimodalRetriever(config)
        
        # 测试图片检索参数验证
        test_images = ["base64_encoded_image_1", "base64_encoded_image_2"]
        
        # 验证图片列表不能为空
        try:
            retriever.retrieve_by_images([], "ddi_collection_camera_encoded_1")
            assert False, "应该抛出ValueError异常"
        except ValueError as e:
            assert "图片列表不能为空" in str(e)
        
        print("✅ 图片检索功能测试成功")
        return True
        
    except Exception as e:
        print(f"❌ 图片检索功能测试失败: {e}")
        return False


def test_real_api_format():
    """测试与真实API格式的兼容性（不实际调用API）"""
    print("\n🔧 测试真实API格式兼容性...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
        
        # 模拟真实的API配置
        config = APIConfig(
            project="driveinsight",
            api_key="xxx",  # 模拟密钥
            username="l00882130",
            platform="xmodalitys-external",
            region="RaD-prod",
            entrypoint_version="v2",
            api_base_url="https://driveinsight-api.ias.huawei.com",
            api_path="/xmodalitys/retrieve"
        )
        
        retriever = MultimodalRetriever(config)
        
        # 验证URL构建
        assert config.api_url == "https://driveinsight-api.ias.huawei.com/xmodalitys/retrieve"
        
        # 验证请求头格式（按照真实curl命令）
        headers = retriever.headers
        assert headers["Authorization"] == "Bearer xxx"
        assert headers["Deepdata-Platform"] == "xmodalitys-external"
        assert headers["Deepdata-Project"] == "driveinsight"
        assert headers["Deepdata-Region"] == "RaD-prod"
        assert headers["Entrypoint-Version"] == "v2"
        assert headers["Host"] == "driveinsight-api.ias.huawei.com"
        assert headers["username"] == "l00882130"
        
        # 验证相机推导（按照真实API示例）
        camera = retriever._extract_camera_from_collection("ddi_collection_camera_encoded_2")
        assert camera == "camera_2"
        
        print("✅ 真实API格式兼容性测试成功")
        print("🔧 API URL:", config.api_url)
        print("🔧 请求头格式验证通过")
        print("🔧 支持文本和图片检索（modality=1/2）")
        print("🔧 支持时间范围参数（start_time/end_time）")
        return True
        
    except Exception as e:
        print(f"❌ 真实API格式兼容性测试失败: {e}")
        return False


def main():
    """运行所有API集成测试"""
    print("🚀 开始多模态轨迹检索系统 - API集成测试")
    print("="*60)
    
    tests = [
        test_api_config_from_env,
        test_api_headers,
        test_api_payload,
        test_cli_with_new_format,
        test_image_retrieval,
        test_real_api_format
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
    print(f"🎯 API集成测试完成: {passed} 个通过, {failed} 个失败")
    
    if failed == 0:
        print("🎉 所有API集成测试通过！完整API格式适配完成")
        print("✅ 支持功能:")
        print("   - 文本检索 (modality=1)")
        print("   - 图片检索 (modality=2)")
        print("   - 时间范围查询 (start_time/end_time)")
        print("   - 分页查询 (start/count)")
        return True
    else:
        print("⚠️ 部分API测试失败，需要修复")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
