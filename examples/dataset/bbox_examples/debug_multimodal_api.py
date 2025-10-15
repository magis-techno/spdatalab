#!/usr/bin/env python3
"""
多模态API调试脚本
================

用于诊断API调用问题，逐步测试各种参数组合

使用方法：
    python debug_multimodal_api.py
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever


def test_basic_api():
    """测试1: 最基础的API调用（无额外过滤）"""
    print("\n" + "="*60)
    print("测试1: 基础API调用（无过滤）")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    try:
        results = retriever.retrieve_by_text(
            text="白天",
            collection="ddi_collection_camera_encoded_1",
            count=10
        )
        print(f"✅ 成功: 返回 {len(results)} 条结果")
        if results:
            print(f"📋 第一条结果:")
            r = results[0]
            print(f"   dataset_name: {r.get('dataset_name', 'N/A')}")
            print(f"   timestamp: {r.get('timestamp', 'N/A')}")
            print(f"   similarity: {r.get('similarity', 'N/A')}")
        return True
    except Exception as e:
        print(f"❌ 失败: {e}")
        return False


def test_city_filter():
    """测试2: 只使用城市过滤"""
    print("\n" + "="*60)
    print("测试2: 只使用城市过滤")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    # 测试不同的城市代码格式
    city_formats = [
        ("A72", "string"),
        ("A72", "int32"),
    ]
    
    for city_code, format_type in city_formats:
        print(f"\n🔍 测试城市代码: {city_code} (format: {format_type})")
        
        filter_dict = {
            "conditions": [[{
                "field": "ddi_basic.city_code",
                "func": "$eq",
                "value": city_code,
                "format": format_type
            }]],
            "logic": ["$and"],
            "cursorKey": None
        }
        
        try:
            results = retriever.retrieve_by_text(
                text="白天",
                collection="ddi_collection_camera_encoded_1",
                count=10,
                filter_dict=filter_dict
            )
            print(f"   ✅ 成功: 返回 {len(results)} 条结果")
            if results:
                print(f"   📋 第一条: dataset={results[0].get('dataset_name', 'N/A')[:50]}...")
        except Exception as e:
            print(f"   ❌ 失败: {e}")


def test_dataset_filter_small():
    """测试3: 只使用少量dataset过滤"""
    print("\n" + "="*60)
    print("测试3: 使用少量dataset过滤（5个）")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    # 使用一些常见的dataset名称模式
    test_datasets = [
        "test_dataset_1",
        "test_dataset_2", 
        "test_dataset_3",
        "test_dataset_4",
        "test_dataset_5"
    ]
    
    try:
        results = retriever.retrieve_by_text(
            text="白天",
            collection="ddi_collection_camera_encoded_1",
            count=10,
            dataset_name=test_datasets
        )
        print(f"✅ 成功: 返回 {len(results)} 条结果")
        if results:
            print(f"📋 匹配的dataset:")
            for r in results[:3]:
                print(f"   - {r.get('dataset_name', 'N/A')}")
    except Exception as e:
        print(f"❌ 失败: {e}")


def test_combined_filters_small():
    """测试4: 组合过滤（城市 + 少量dataset）"""
    print("\n" + "="*60)
    print("测试4: 组合过滤（城市 + 5个dataset）")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    filter_dict = {
        "conditions": [[{
            "field": "ddi_basic.city_code",
            "func": "$eq",
            "value": "A72",
            "format": "string"
        }]],
        "logic": ["$and"],
        "cursorKey": None
    }
    
    test_datasets = [
        "test_dataset_1",
        "test_dataset_2",
    ]
    
    try:
        results = retriever.retrieve_by_text(
            text="白天",
            collection="ddi_collection_camera_encoded_1",
            count=10,
            dataset_name=test_datasets,
            filter_dict=filter_dict
        )
        print(f"✅ 成功: 返回 {len(results)} 条结果")
    except Exception as e:
        print(f"❌ 失败: {e}")


def test_different_queries():
    """测试5: 不同的查询文本"""
    print("\n" + "="*60)
    print("测试5: 测试不同查询文本")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    queries = ["白天", "bicycle", "car", "road", "a"]
    
    for query in queries:
        print(f"\n🔍 查询: '{query}'")
        try:
            results = retriever.retrieve_by_text(
                text=query,
                collection="ddi_collection_camera_encoded_1",
                count=5
            )
            print(f"   ✅ 返回 {len(results)} 条结果")
        except Exception as e:
            print(f"   ❌ 失败: {e}")


def test_dataset_limit():
    """测试6: 测试dataset数量限制"""
    print("\n" + "="*60)
    print("测试6: 测试dataset参数的数量限制")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    # 测试不同数量的dataset
    counts = [10, 50, 100, 200, 500]
    
    for count in counts:
        datasets = [f"dataset_{i}" for i in range(count)]
        print(f"\n🔍 测试 {count} 个dataset")
        
        try:
            results = retriever.retrieve_by_text(
                text="白天",
                collection="ddi_collection_camera_encoded_1",
                count=10,
                dataset_name=datasets
            )
            print(f"   ✅ 成功: 返回 {len(results)} 条结果")
        except Exception as e:
            print(f"   ❌ 失败: {str(e)[:100]}...")


def test_without_dataset_filter():
    """测试7: 只用城市过滤，不用dataset过滤"""
    print("\n" + "="*60)
    print("测试7: 【关键测试】只用城市过滤，不传dataset_name")
    print("="*60)
    
    config = APIConfig.from_env()
    retriever = MultimodalRetriever(config)
    
    filter_dict = {
        "conditions": [[{
            "field": "ddi_basic.city_code",
            "func": "$eq",
            "value": "A72",
            "format": "string"
        }]],
        "logic": ["$and"],
        "cursorKey": None
    }
    
    try:
        results = retriever.retrieve_by_text(
            text="白天",
            collection="ddi_collection_camera_encoded_1",
            count=20,
            filter_dict=filter_dict
            # 注意：不传 dataset_name 参数
        )
        print(f"✅ 成功: 返回 {len(results)} 条结果")
        if results:
            print(f"\n📋 前5条结果:")
            for i, r in enumerate(results[:5], 1):
                ds = r.get('dataset_name', 'N/A')
                sim = r.get('similarity', 0)
                print(f"   {i}. {ds[:60]}... (similarity: {sim:.3f})")
        return results
    except Exception as e:
        print(f"❌ 失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    """主函数：运行所有测试"""
    print("\n🔧 多模态API调试工具")
    print("="*60)
    
    try:
        # 按顺序执行测试
        test_basic_api()
        test_city_filter()
        test_dataset_filter_small()
        test_combined_filters_small()
        test_different_queries()
        test_dataset_limit()
        
        # 最关键的测试
        print("\n" + "🎯"*30)
        print("关键诊断：只用城市过滤看是否能返回结果")
        print("🎯"*30)
        results = test_without_dataset_filter()
        
        if results:
            print("\n" + "="*60)
            print("💡 诊断结论:")
            print("="*60)
            print("✅ 城市过滤正常工作")
            print("❌ 问题可能在于:")
            print("   1. dataset_name参数数量太多（1962个）")
            print("   2. dataset_name + filter 组合使用有冲突")
            print("   3. API对dataset_name参数有大小限制")
            print("\n💡 建议方案:")
            print("   1. 不传dataset_name，只使用城市过滤")
            print("   2. 或者限制dataset_name数量（如只传前100个）")
            print("   3. 或者分批次调用API")
        else:
            print("\n" + "="*60)
            print("💡 诊断结论:")
            print("="*60)
            print("❌ 城市过滤可能有问题")
            print("💡 建议检查:")
            print("   1. A72的城市代码格式是否正确")
            print("   2. ddi_basic.city_code字段是否存在")
            print("   3. 查询文本'白天'是否合适")
        
        print("\n✅ 调试完成")
        
    except Exception as e:
        print(f"\n❌ 调试过程出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

