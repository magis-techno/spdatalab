#!/usr/bin/env python3
"""
测试问题单数据集统一subdataset功能
验证修改后的问题单处理是否正确创建统一的subdataset
"""

import json
import tempfile
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.spdatalab.dataset.dataset_manager import DatasetManager

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_unified_defect_subdataset():
    """测试问题单数据集统一subdataset功能"""
    print("=" * 60)
    print("测试问题单数据集统一subdataset功能")
    print("=" * 60)
    
    # 创建测试数据
    test_defect_urls = [
        "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=defect_001",
        "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=defect_002|priority=high",
        "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=defect_003|priority=medium|category=lane_detection",
        "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=defect_004|severity=critical|region=beijing",
    ]
    
    # 创建临时测试文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        for url in test_defect_urls:
            f.write(url + '\n')
        temp_file = f.name
    
    try:
        # Mock数据库查询
        def mock_query_defect_data(data_name):
            # 模拟查询成功的场景
            scene_mapping = {
                'defect_001': 'scene_001',
                'defect_002': 'scene_002', 
                'defect_003': 'scene_003',
                'defect_004': 'scene_004'
            }
            return scene_mapping.get(data_name)
        
        # 创建DatasetManager并测试
        manager = DatasetManager(defect_mode=True)
        
        # 使用patch来模拟数据库查询
        with patch.object(manager, 'query_defect_data', side_effect=mock_query_defect_data):
            # 构建问题单数据集
            dataset = manager.build_dataset_from_defect_urls(
                temp_file,
                "test_unified_defects",
                "测试统一问题单数据集"
            )
            
            # 验证数据集结构
            print(f"\n验证数据集结构:")
            print(f"- 数据集名称: {dataset.name}")
            print(f"- 子数据集数量: {len(dataset.subdatasets)}")
            print(f"- 总场景数: {dataset.total_scenes}")
            print(f"- 唯一场景数: {dataset.total_unique_scenes}")
            
            # 验证子数据集结构
            assert len(dataset.subdatasets) == 1, f"期望1个子数据集，实际得到{len(dataset.subdatasets)}个"
            
            subdataset = dataset.subdatasets[0]
            print(f"\n验证子数据集结构:")
            print(f"- 子数据集名称: {subdataset.name}")
            print(f"- 场景数量: {subdataset.scene_count}")
            print(f"- 场景ID数量: {len(subdataset.scene_ids)}")
            print(f"- 预期场景ID: {sorted(subdataset.scene_ids)}")
            
            # 验证metadata
            metadata = subdataset.metadata
            print(f"\n验证metadata:")
            print(f"- 数据类型: {metadata.get('data_type')}")
            print(f"- 源文件: {metadata.get('source_file')}")
            print(f"- 总URL数: {metadata.get('total_urls')}")
            print(f"- 成功场景数: {metadata.get('successful_scenes')}")
            print(f"- 额外属性: {[k for k in metadata.keys() if k not in ['data_type', 'source_file', 'total_urls', 'successful_scenes']]}")
            
            # 验证期望的结果
            expected_scene_ids = ['scene_001', 'scene_002', 'scene_003', 'scene_004']
            assert subdataset.scene_count == 4, f"期望4个场景，实际得到{subdataset.scene_count}个"
            assert len(subdataset.scene_ids) == 4, f"期望4个场景ID，实际得到{len(subdataset.scene_ids)}个"
            assert sorted(subdataset.scene_ids) == sorted(expected_scene_ids), f"场景ID不匹配"
            
            # 验证统一的额外属性
            expected_attributes = ['priority', 'category', 'severity', 'region']
            for attr in expected_attributes:
                assert attr in metadata, f"期望属性'{attr}'不在metadata中"
            
            print(f"\n✅ 所有验证通过!")
            
            # 保存测试数据集以供检查
            output_file = "test_unified_defect_dataset.json"
            manager.save_dataset(dataset, output_file, format='json')
            print(f"\n测试数据集已保存到: {output_file}")
            
            # 显示保存的JSON结构
            with open(output_file, 'r', encoding='utf-8') as f:
                dataset_json = json.load(f)
                print(f"\n生成的JSON结构:")
                print(json.dumps(dataset_json, ensure_ascii=False, indent=2))
                
    finally:
        # 清理临时文件
        Path(temp_file).unlink()

def test_defect_line_parsing():
    """测试问题单行解析功能"""
    print("\n" + "=" * 60)
    print("测试问题单行解析功能")
    print("=" * 60)
    
    manager = DatasetManager(defect_mode=True)
    
    test_cases = [
        {
            'input': 'https://example.com/defect?dataName=test_001',
            'expected_attributes': {}
        },
        {
            'input': 'https://example.com/defect?dataName=test_002|priority=high',
            'expected_attributes': {'priority': 'high'}
        },
        {
            'input': 'https://example.com/defect?dataName=test_003|priority=medium|category=lane_detection',
            'expected_attributes': {'priority': 'medium', 'category': 'lane_detection'}
        },
        {
            'input': 'https://example.com/defect?dataName=test_004|severity=3|region=beijing|urgent',
            'expected_attributes': {'severity': '3', 'region': 'beijing', 'urgent': True}
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n测试用例 {i}: {test_case['input']}")
        
        result = manager.parse_defect_line(test_case['input'])
        
        if result:
            print(f"- 解析成功")
            print(f"- URL: {result['url']}")
            print(f"- 属性: {result['attributes']}")
            
            # 验证属性
            assert result['attributes'] == test_case['expected_attributes'], \
                f"属性不匹配: 期望{test_case['expected_attributes']}, 实际{result['attributes']}"
            
            print(f"- ✅ 验证通过")
        else:
            print(f"- ❌ 解析失败")

if __name__ == "__main__":
    try:
        test_defect_line_parsing()
        test_unified_defect_subdataset()
        print("\n🎉 所有测试通过！")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc() 