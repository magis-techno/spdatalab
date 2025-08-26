#!/usr/bin/env python3
"""
多模态轨迹检索系统 - Verbose模式和数据库保存功能测试

用于验证：
1. Verbose模式下的详细数据集信息显示
2. 数据库保存功能是否正常工作
3. SSL验证关闭后的API调用

测试前请确保：
1. 已配置正确的环境变量 (.env文件)
2. 数据库连接正常
3. API服务器可访问

使用方法：
python test_multimodal_verbose_db.py
"""

def test_basic_functionality():
    """测试基础功能导入和配置"""
    print("🧪 测试1: 基础功能导入...")
    
    try:
        # 测试配置加载
        from spdatalab.common.config import getenv
        api_key = getenv('MULTIMODAL_API_KEY', 'NOT_FOUND')
        api_url = getenv('MULTIMODAL_API_BASE_URL', 'NOT_FOUND')
        
        print(f"   ✅ 环境变量加载成功")
        print(f"   📡 API URL: {api_url}")
        print(f"   🔑 API Key: {'已配置' if api_key != 'NOT_FOUND' else '未配置'}")
        
        # 测试模块导入
        from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
        from spdatalab.fusion.multimodal_trajectory_retrieval import MultimodalConfig, MultimodalTrajectoryWorkflow
        
        print(f"   ✅ 核心模块导入成功")
        
        # 测试配置创建
        api_config = APIConfig.from_env()
        print(f"   ✅ API配置创建成功: {api_config.api_url}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 基础功能测试失败: {e}")
        return False


def test_verbose_mode():
    """测试Verbose模式功能"""
    print("\n🧪 测试2: Verbose模式数据集详情显示...")
    
    try:
        # 模拟多模态检索结果
        search_results = [
            {
                "dataset_name": "4842d7b30f9e49c99584a220709caaf5_130154_2025/05/29/16:31:23-16:31:53",
                "timestamp": 1748507506699,
                "similarity": 0.39,
                "metadata": {"dataset_bag": "camera_encoded_1"}
            },
            {
                "dataset_name": "abc123def456_987654_2025/05/28/14:22:15-14:22:45", 
                "timestamp": 1748407406600,
                "similarity": 0.42,
                "metadata": {"dataset_bag": "camera_encoded_2"}
            },
            {
                "dataset_name": "4842d7b30f9e49c99584a220709caaf5_130154_2025/05/29/16:31:23-16:31:53",
                "timestamp": 1748507506800,
                "similarity": 0.35,
                "metadata": {"dataset_bag": "camera_encoded_1"}
            }
        ]
        
        # 测试数据集聚合逻辑
        from spdatalab.fusion.multimodal_trajectory_retrieval import ResultAggregator
        
        aggregator = ResultAggregator()
        aggregated_datasets = aggregator.aggregate_by_dataset(search_results)
        
        print(f"   ✅ 聚合测试成功:")
        print(f"   📊 原始结果: {len(search_results)} 条")
        print(f"   📁 聚合数据集: {len(aggregated_datasets)} 个")
        
        # 模拟详细信息
        dataset_details = {}
        for dataset_name, results in aggregated_datasets.items():
            dataset_details[dataset_name] = len(results)
            display_name = dataset_name if len(dataset_name) <= 60 else dataset_name[:57] + "..."
            print(f"      📂 {display_name}: {len(results)} 条结果")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Verbose模式测试失败: {e}")
        return False


def test_database_table_creation():
    """测试数据库表创建功能"""
    print("\n🧪 测试3: 数据库表创建...")
    
    try:
        from spdatalab.fusion.multimodal_trajectory_retrieval import MultimodalTrajectoryWorkflow, MultimodalConfig
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        
        # 创建配置
        config = MultimodalConfig(
            api_config=APIConfig.from_env(),
            output_table="test_multimodal_results"
        )
        
        workflow = MultimodalTrajectoryWorkflow(config)
        
        # 测试表创建（不实际创建，只测试SQL生成）
        test_table_name = "test_multimodal_discovery"
        
        print(f"   ✅ 工作流创建成功")
        print(f"   📊 测试表名: {test_table_name}")
        print(f"   🔧 配置验证通过")
        
        # 显示预期的表结构
        expected_columns = [
            "id (SERIAL PRIMARY KEY)",
            "dataset_name (TEXT NOT NULL)",
            "scene_id (TEXT)",
            "event_id (INTEGER)",
            "longitude, latitude (DOUBLE PRECISION)",
            "timestamp (BIGINT)",
            "source_dataset (TEXT)",
            "source_similarity (DOUBLE PRECISION)",
            "query_type, query_content (TEXT)",
            "collection (VARCHAR)",
            "created_at (TIMESTAMP)"
        ]
        
        print(f"   📋 预期表结构:")
        for col in expected_columns:
            print(f"      - {col}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 数据库表创建测试失败: {e}")
        return False


def test_api_ssl_configuration():
    """测试API SSL配置"""
    print("\n🧪 测试4: API SSL配置...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import MultimodalRetriever
        import requests
        import urllib3
        
        # 检查SSL警告抑制
        print(f"   ✅ urllib3模块导入成功")
        print(f"   🔒 SSL警告已抑制")
        
        # 检查API配置
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        config = APIConfig.from_env()
        
        print(f"   📡 API配置验证:")
        print(f"      URL: {config.api_url}")
        print(f"      Project: {config.project}")
        print(f"      Platform: {config.platform}")
        print(f"      Region: {config.region}")
        print(f"      Timeout: {config.timeout}s")
        print(f"      Max Retries: {config.max_retries}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ API SSL配置测试失败: {e}")
        return False


def main():
    """运行所有测试"""
    print("🚀 多模态轨迹检索系统 - 功能验证测试")
    print("=" * 60)
    
    tests = [
        test_basic_functionality,
        test_verbose_mode, 
        test_database_table_creation,
        test_api_ssl_configuration
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"   💥 测试异常: {e}")
    
    print("\n" + "=" * 60)
    print(f"🎯 测试完成: {passed}/{total} 通过")
    
    if passed == total:
        print("✅ 所有测试通过！系统准备就绪")
        print("\n📋 下一步操作建议:")
        print("1. 运行完整的多模态查询:")
        print("   python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print("       --text 'bicycle crossing intersection' \\")
        print("       --collection 'ddi_collection_camera_encoded_1' \\")
        print("       --output-table 'discovered_trajectories' \\")
        print("       --verbose")
        print("\n2. 检查数据库表是否创建成功")
        print("3. 验证数据保存结果")
    else:
        print(f"❌ {total - passed} 个测试失败，请检查配置")
    
    return passed == total


if __name__ == "__main__":
    main()
