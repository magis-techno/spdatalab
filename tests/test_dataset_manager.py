"""数据集管理器的单元测试。"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from spdatalab.dataset.dataset_manager import DatasetManager, Dataset, SubDataset

# 测试数据
SAMPLE_INDEX_CONTENT = """obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink@duplicate20
obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48/train_god_god_E2E_0419_7_6_0_9_20250518104817_839_21601_duplicate_26431_guiyang_f_pkg2_2frames.jsonl.shrink@duplicate10
obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_vru_avoid_obstacle_data_sub_ddi_2773412e2e_2025_05_18_10_10_41/train_god_god_E2E_0419_7_6_0_9_20250518104658_6844_218135_duplicate_441684_guiyang_f_pkg2_2frames.jsonl.shrink@duplicate5"""

SAMPLE_SCENE_DATA = [
    {"scene_id": "scene_001", "other_data": "test1"},
    {"scene_id": "scene_002", "other_data": "test2"},
    {"scene_id": "scene_003", "other_data": "test3"}
]

@pytest.fixture
def dataset_manager():
    return DatasetManager()

@pytest.fixture
def sample_dataset():
    subdataset1 = SubDataset(
        name="GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59",
        obs_path="obs://path1/file1.shrink",
        duplication_factor=20,
        scene_count=2,
        scene_ids=["scene_001", "scene_002"]
    )
    
    subdataset2 = SubDataset(
        name="GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48",
        obs_path="obs://path2/file2.shrink",
        duplication_factor=10,
        scene_count=1,
        scene_ids=["scene_003"]
    )
    
    dataset = Dataset(
        name="test_dataset",
        description="Test dataset",
        subdatasets=[subdataset1, subdataset2],
        total_unique_scenes=3,
        total_scenes=50  # (2*20) + (1*10)
    )
    
    return dataset

class TestDatasetManager:
    """数据集管理器单元测试。"""
    
    def test_extract_subdataset_name(self, dataset_manager):
        """测试子数据集名称提取。"""
        test_cases = [
            (
                "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink",
                "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
            ),
            (
                "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48/train_god_god_E2E_0419_7_6_0_9_20250518104817_839_21601_duplicate_26431_guiyang_f_pkg2_2frames.jsonl.shrink",
                "GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48"
            )
        ]
        
        for obs_path, expected_name in test_cases:
            result = dataset_manager.extract_subdataset_name(obs_path)
            assert result == expected_name
    
    def test_parse_index_line(self, dataset_manager):
        """测试索引行解析。"""
        # 有效的行
        valid_cases = [
            ("obs://path/file.shrink@duplicate20", ("obs://path/file.shrink", 20)),
            ("local/file.shrink@duplicate5", ("local/file.shrink", 5)),
        ]
        
        for line, expected in valid_cases:
            result = dataset_manager.parse_index_line(line)
            assert result == expected
        
        # 无效的行
        invalid_cases = ["", "invalid_line", "file@dup", "file@duplicate"]
        
        for line in invalid_cases:
            result = dataset_manager.parse_index_line(line)
            assert result is None
    
    @patch('spdatalab.dataset.dataset_manager.SceneListGenerator')
    def test_extract_scene_ids_from_file(self, mock_generator_class, dataset_manager):
        """测试从文件提取scene_id。"""
        # 设置mock
        mock_generator = mock_generator_class.return_value
        mock_generator.iter_scenes_from_file.return_value = SAMPLE_SCENE_DATA
        
        result = dataset_manager.extract_scene_ids_from_file("test_file.shrink")
        
        expected_scene_ids = ["scene_001", "scene_002", "scene_003"]
        assert result == expected_scene_ids
        mock_generator.iter_scenes_from_file.assert_called_once_with("test_file.shrink")
    
    @patch('spdatalab.dataset.dataset_manager.open_file')
    @patch('spdatalab.dataset.dataset_manager.SceneListGenerator')
    def test_build_dataset_from_index(self, mock_generator_class, mock_open_file, dataset_manager):
        """测试从索引文件构建数据集。"""
        # 设置mock
        mock_open_file.return_value.__enter__.return_value = SAMPLE_INDEX_CONTENT.split('\n')
        
        mock_generator = mock_generator_class.return_value
        # 为每个文件返回不同的场景数据
        scene_data_map = {
            "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink": [
                {"scene_id": "scene_001"}, {"scene_id": "scene_002"}
            ],
            "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48/train_god_god_E2E_0419_7_6_0_9_20250518104817_839_21601_duplicate_26431_guiyang_f_pkg2_2frames.jsonl.shrink": [
                {"scene_id": "scene_003"}
            ],
            "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_vru_avoid_obstacle_data_sub_ddi_2773412e2e_2025_05_18_10_10_41/train_god_god_E2E_0419_7_6_0_9_20250518104658_6844_218135_duplicate_441684_guiyang_f_pkg2_2frames.jsonl.shrink": [
                {"scene_id": "scene_004"}, {"scene_id": "scene_005"}
            ]
        }
        
        def mock_iter_scenes(file_path):
            return scene_data_map.get(file_path, [])
        
        mock_generator.iter_scenes_from_file.side_effect = mock_iter_scenes
        
        # 构建数据集
        dataset = dataset_manager.build_dataset_from_index(
            "test_index.txt", 
            "test_dataset", 
            "Test dataset description"
        )
        
        # 验证结果
        assert dataset.name == "test_dataset"
        assert dataset.description == "Test dataset description"
        assert len(dataset.subdatasets) == 3
        assert dataset.total_unique_scenes == 5  # 2 + 1 + 2
        assert dataset.total_scenes == 65  # (2*20) + (1*10) + (2*5)
        
        # 验证子数据集
        subdataset_names = [sub.name for sub in dataset.subdatasets]
        expected_names = [
            "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59",
            "GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48",
            "GOD_E2E_golden_vru_avoid_obstacle_data_sub_ddi_2773412e2e_2025_05_18_10_10_41"
        ]
        assert subdataset_names == expected_names
    
    def test_save_and_load_dataset(self, dataset_manager, sample_dataset):
        """测试数据集保存和加载。"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            # 保存数据集
            dataset_manager.save_dataset(sample_dataset, temp_file)
            
            # 加载数据集
            loaded_dataset = dataset_manager.load_dataset(temp_file)
            
            # 验证加载的数据集
            assert loaded_dataset.name == sample_dataset.name
            assert loaded_dataset.description == sample_dataset.description
            assert len(loaded_dataset.subdatasets) == len(sample_dataset.subdatasets)
            assert loaded_dataset.total_unique_scenes == sample_dataset.total_unique_scenes
            assert loaded_dataset.total_scenes == sample_dataset.total_scenes
            
            # 验证子数据集
            for i, subdataset in enumerate(loaded_dataset.subdatasets):
                original = sample_dataset.subdatasets[i]
                assert subdataset.name == original.name
                assert subdataset.obs_path == original.obs_path
                assert subdataset.duplication_factor == original.duplication_factor
                assert subdataset.scene_count == original.scene_count
                assert subdataset.scene_ids == original.scene_ids
                
        finally:
            Path(temp_file).unlink()
    
    def test_get_subdataset_info(self, dataset_manager, sample_dataset):
        """测试获取子数据集信息。"""
        # 存在的子数据集
        subdataset = dataset_manager.get_subdataset_info(
            sample_dataset, 
            "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
        )
        assert subdataset is not None
        assert subdataset.name == "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
        assert subdataset.scene_count == 2
        
        # 不存在的子数据集
        subdataset = dataset_manager.get_subdataset_info(sample_dataset, "non_existent")
        assert subdataset is None
    
    def test_list_scene_ids(self, dataset_manager, sample_dataset):
        """测试列出场景ID。"""
        # 列出所有场景ID
        all_scene_ids = dataset_manager.list_scene_ids(sample_dataset)
        expected_all = ["scene_001", "scene_002", "scene_003"]
        assert sorted(all_scene_ids) == sorted(expected_all)
        
        # 列出特定子数据集的场景ID
        subdataset_scene_ids = dataset_manager.list_scene_ids(
            sample_dataset, 
            "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
        )
        expected_subdataset = ["scene_001", "scene_002"]
        assert subdataset_scene_ids == expected_subdataset
        
        # 不存在的子数据集
        non_existent_scene_ids = dataset_manager.list_scene_ids(sample_dataset, "non_existent")
        assert non_existent_scene_ids == []
    
    def test_generate_scene_list_with_duplication(self, dataset_manager, sample_dataset):
        """测试生成包含倍增的场景ID列表。"""
        # 生成所有子数据集的倍增场景ID
        duplicated_scene_ids = list(dataset_manager.generate_scene_list_with_duplication(sample_dataset))
        
        # 验证总数
        expected_total = 50  # (2*20) + (1*10)
        assert len(duplicated_scene_ids) == expected_total
        
        # 验证scene_001出现20次，scene_002出现20次，scene_003出现10次
        scene_counts = {}
        for scene_id in duplicated_scene_ids:
            scene_counts[scene_id] = scene_counts.get(scene_id, 0) + 1
        
        assert scene_counts["scene_001"] == 20
        assert scene_counts["scene_002"] == 20
        assert scene_counts["scene_003"] == 10
        
        # 生成特定子数据集的倍增场景ID
        subdataset_duplicated = list(dataset_manager.generate_scene_list_with_duplication(
            sample_dataset, 
            "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
        ))
        
        assert len(subdataset_duplicated) == 40  # 2*20
        assert subdataset_duplicated.count("scene_001") == 20
        assert subdataset_duplicated.count("scene_002") == 20

class TestDataClasses:
    """测试数据类。"""
    
    def test_subdataset_creation(self):
        """测试SubDataset创建。"""
        subdataset = SubDataset(
            name="test_subdataset",
            obs_path="obs://test/path",
            duplication_factor=5
        )
        
        assert subdataset.name == "test_subdataset"
        assert subdataset.obs_path == "obs://test/path"
        assert subdataset.duplication_factor == 5
        assert subdataset.scene_count == 0
        assert subdataset.scene_ids == []
        assert subdataset.metadata == {}
    
    def test_dataset_creation(self):
        """测试Dataset创建。"""
        dataset = Dataset(name="test_dataset")
        
        assert dataset.name == "test_dataset"
        assert dataset.description == ""
        assert dataset.subdatasets == []
        assert dataset.total_scenes == 0
        assert dataset.total_unique_scenes == 0
        assert dataset.metadata == {}
        assert dataset.created_at  # 应该有创建时间 