#!/usr/bin/env python3
"""
测试bbox模块与dataset_manager的集成
"""

import sys
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# 添加src路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spdatalab.dataset.dataset_manager import DatasetManager, Dataset, SubDataset
from spdatalab.dataset.bbox import (
    load_scene_ids_from_json,
    load_scene_ids_from_parquet,
    load_scene_ids_from_text,
    load_scene_ids
)

class TestBboxIntegration(unittest.TestCase):
    """测试bbox与dataset_manager的集成"""
    
    def setUp(self):
        """设置测试环境"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # 创建测试数据集
        self.test_dataset = Dataset(
            name="test_dataset",
            description="测试数据集",
            subdatasets=[
                SubDataset(
                    name="test_sub1",
                    obs_path="obs://test/path1",
                    duplication_factor=10,
                    scene_count=3,
                    scene_ids=["scene_001", "scene_002", "scene_003"]
                ),
                SubDataset(
                    name="test_sub2",
                    obs_path="obs://test/path2",
                    duplication_factor=5,
                    scene_count=2,
                    scene_ids=["scene_004", "scene_005"]
                )
            ]
        )
    
    def tearDown(self):
        """清理测试环境"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_load_scene_ids_from_json(self):
        """测试从JSON文件加载scene_id"""
        # 创建JSON文件
        json_file = self.temp_dir / "test_dataset.json"
        manager = DatasetManager()
        manager.save_dataset(self.test_dataset, str(json_file), format='json')
        
        # 加载scene_id
        scene_ids = load_scene_ids_from_json(str(json_file))
        
        # 验证结果
        expected_scene_ids = ["scene_001", "scene_002", "scene_003", "scene_004", "scene_005"]
        self.assertEqual(sorted(scene_ids), sorted(expected_scene_ids))
        self.assertEqual(len(scene_ids), 5)
    
    def test_load_scene_ids_from_parquet(self):
        """测试从Parquet文件加载scene_id"""
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not available")
        
        # 创建Parquet文件
        parquet_file = self.temp_dir / "test_dataset.parquet"
        manager = DatasetManager()
        manager.save_dataset(self.test_dataset, str(parquet_file), format='parquet')
        
        # 加载scene_id
        scene_ids = load_scene_ids_from_parquet(str(parquet_file))
        
        # 验证结果
        expected_scene_ids = ["scene_001", "scene_002", "scene_003", "scene_004", "scene_005"]
        self.assertEqual(sorted(scene_ids), sorted(expected_scene_ids))
        self.assertEqual(len(scene_ids), 5)
    
    def test_load_scene_ids_from_text(self):
        """测试从文本文件加载scene_id"""
        # 创建文本文件
        text_file = self.temp_dir / "scene_ids.txt"
        scene_ids_list = ["scene_001", "scene_002", "scene_003"]
        text_file.write_text("\n".join(scene_ids_list))
        
        # 加载scene_id
        scene_ids = load_scene_ids_from_text(str(text_file))
        
        # 验证结果
        self.assertEqual(scene_ids, scene_ids_list)
        self.assertEqual(len(scene_ids), 3)
    
    def test_load_scene_ids_auto_detection(self):
        """测试自动格式检测"""
        # 测试JSON格式自动检测
        json_file = self.temp_dir / "test.json"
        manager = DatasetManager()
        manager.save_dataset(self.test_dataset, str(json_file), format='json')
        
        scene_ids_json = load_scene_ids(str(json_file))
        expected_scene_ids = ["scene_001", "scene_002", "scene_003", "scene_004", "scene_005"]
        self.assertEqual(sorted(scene_ids_json), sorted(expected_scene_ids))
        
        # 测试文本格式自动检测
        text_file = self.temp_dir / "test.txt"
        scene_ids_list = ["scene_001", "scene_002"]
        text_file.write_text("\n".join(scene_ids_list))
        
        scene_ids_text = load_scene_ids(str(text_file))
        self.assertEqual(scene_ids_text, scene_ids_list)
    
    @patch('spdatalab.dataset.bbox.fetch_meta')
    @patch('spdatalab.dataset.bbox.fetch_bbox_with_geometry')
    @patch('spdatalab.dataset.bbox.batch_insert_to_postgis')
    def test_bbox_run_with_json(self, mock_insert, mock_fetch_bbox, mock_fetch_meta):
        """测试使用JSON文件运行bbox处理"""
        from spdatalab.dataset.bbox import run
        import pandas as pd
        
        # 准备mock数据
        mock_fetch_meta.return_value = pd.DataFrame({
            'scene_token': ['scene_001', 'scene_002'],
            'data_name': ['data1', 'data2'],
            'event_id': [1, 2],
            'city_id': [100, 200],
            'timestamp': ['2025-01-01', '2025-01-02']
        })
        
        mock_fetch_bbox.return_value = pd.DataFrame({
            'dataset_name': ['data1', 'data2'],
            'xmin': [0.0, 1.0],
            'ymin': [0.0, 1.0],
            'xmax': [1.0, 2.0],
            'ymax': [1.0, 2.0],
            'all_good': [True, True]
        })
        
        mock_insert.return_value = 2
        
        # 创建JSON文件
        json_file = self.temp_dir / "test_dataset.json"
        manager = DatasetManager()
        manager.save_dataset(self.test_dataset, str(json_file), format='json')
        
        # 运行bbox处理
        with patch('spdatalab.dataset.bbox.create_engine') as mock_engine:
            mock_engine.return_value = MagicMock()
            run(str(json_file), batch=2, insert_batch=1000)
        
        # 验证调用
        self.assertTrue(mock_fetch_meta.called)
        self.assertTrue(mock_fetch_bbox.called)
        self.assertTrue(mock_insert.called)
    
    def test_dataset_manager_bbox_workflow(self):
        """测试完整的dataset_manager + bbox工作流程"""
        # 创建测试索引文件
        index_file = self.temp_dir / "test_index.txt"
        index_content = [
            "obs://test/path1/file1.jsonl@duplicate10",
            "obs://test/path2/file2.jsonl@duplicate5"
        ]
        index_file.write_text("\n".join(index_content))
        
        # Mock scene_list_generator
        with patch('spdatalab.dataset.dataset_manager.SceneListGenerator') as mock_generator:
            mock_instance = MagicMock()
            mock_generator.return_value = mock_instance
            
            # 模拟场景数据
            mock_instance.iter_scenes_from_file.side_effect = [
                [{'scene_id': 'scene_001'}, {'scene_id': 'scene_002'}],  # 第一个文件
                [{'scene_id': 'scene_003'}]  # 第二个文件
            ]
            
            # 构建数据集
            manager = DatasetManager()
            dataset = manager.build_dataset_from_index(
                str(index_file),
                "test_workflow_dataset",
                "测试工作流程数据集"
            )
            
            # 验证数据集构建结果
            self.assertEqual(dataset.name, "test_workflow_dataset")
            self.assertEqual(len(dataset.subdatasets), 2)
            self.assertEqual(dataset.total_unique_scenes, 3)
            self.assertEqual(dataset.total_scenes, 2*10 + 1*5)  # 2*10 + 1*5 = 25
            
            # 保存为JSON格式
            json_file = self.temp_dir / "workflow_dataset.json"
            manager.save_dataset(dataset, str(json_file), format='json')
            
            # 验证可以加载scene_id
            scene_ids = load_scene_ids_from_json(str(json_file))
            expected_scene_ids = ['scene_001', 'scene_002', 'scene_003']
            self.assertEqual(sorted(scene_ids), sorted(expected_scene_ids))
    
    def test_error_handling(self):
        """测试错误处理"""
        # 测试不存在的文件
        with self.assertRaises(FileNotFoundError):
            load_scene_ids_from_json("nonexistent.json")
        
        # 测试空JSON文件
        empty_json_file = self.temp_dir / "empty.json"
        empty_json_file.write_text("{}")
        
        scene_ids = load_scene_ids_from_json(str(empty_json_file))
        self.assertEqual(scene_ids, [])
        
        # 测试无效JSON文件
        invalid_json_file = self.temp_dir / "invalid.json"
        invalid_json_file.write_text("invalid json content")
        
        with self.assertRaises(json.JSONDecodeError):
            load_scene_ids_from_json(str(invalid_json_file))

class TestBboxPerformance(unittest.TestCase):
    """测试bbox性能优化"""
    
    def setUp(self):
        """设置测试环境"""
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def tearDown(self):
        """清理测试环境"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_large_dataset_processing(self):
        """测试大数据集处理"""
        # 创建大数据集
        subdatasets = []
        for i in range(100):  # 100个子数据集
            scene_ids = [f"scene_{i:03d}_{j:03d}" for j in range(10)]  # 每个10个场景
            subdataset = SubDataset(
                name=f"sub_dataset_{i:03d}",
                obs_path=f"obs://test/path_{i}",
                duplication_factor=1,
                scene_count=10,
                scene_ids=scene_ids
            )
            subdatasets.append(subdataset)
        
        large_dataset = Dataset(
            name="large_test_dataset",
            description="大型测试数据集",
            subdatasets=subdatasets
        )
        
        # 保存为JSON
        json_file = self.temp_dir / "large_dataset.json"
        manager = DatasetManager()
        manager.save_dataset(large_dataset, str(json_file), format='json')
        
        # 测试加载性能
        import time
        start_time = time.time()
        scene_ids = load_scene_ids_from_json(str(json_file))
        load_time = time.time() - start_time
        
        # 验证结果
        self.assertEqual(len(scene_ids), 1000)  # 100 * 10
        print(f"加载1000个scene_id耗时: {load_time:.3f}秒")
        
        # 性能应该在合理范围内（<1秒）
        self.assertLess(load_time, 1.0)

def run_tests():
    """运行所有测试"""
    unittest.main(verbosity=2)

if __name__ == "__main__":
    run_tests() 