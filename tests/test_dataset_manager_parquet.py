"""数据集管理器Parquet格式功能的单元测试。"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch

from spdatalab.dataset.dataset_manager import DatasetManager, Dataset, SubDataset, PARQUET_AVAILABLE

# 跳过测试如果pandas/pyarrow不可用
pytestmark = pytest.mark.skipif(not PARQUET_AVAILABLE, reason="需要安装pandas和pyarrow")

@pytest.fixture
def dataset_manager():
    return DatasetManager()

@pytest.fixture
def sample_dataset():
    subdataset1 = SubDataset(
        name="lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59",
        obs_path="obs://path1/file1.shrink",
        duplication_factor=3,
        scene_count=2,
        scene_ids=["scene_001", "scene_002"]
    )
    
    subdataset2 = SubDataset(
        name="stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48",
        obs_path="obs://path2/file2.shrink",
        duplication_factor=2,
        scene_count=1,
        scene_ids=["scene_003"]
    )
    
    dataset = Dataset(
        name="test_dataset",
        description="Test dataset for parquet",
        subdatasets=[subdataset1, subdataset2],
        total_unique_scenes=3,
        total_scenes=8  # (2*3) + (1*2)
    )
    
    return dataset

class TestParquetFormat:
    """测试Parquet格式功能。"""
    
    def test_save_and_load_parquet(self, dataset_manager, sample_dataset):
        """测试Parquet格式的保存和加载。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = Path(temp_dir) / "test_dataset.parquet"
            
            # 保存为parquet格式
            dataset_manager.save_dataset(sample_dataset, str(parquet_file), format='parquet')
            
            # 检查文件是否创建
            assert parquet_file.exists()
            meta_file = parquet_file.with_suffix('.meta.json')
            assert meta_file.exists()
            
            # 加载数据集
            loaded_dataset = dataset_manager.load_dataset(str(parquet_file))
            
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
                assert set(subdataset.scene_ids) == set(original.scene_ids)
    
    def test_export_scene_ids_parquet(self, dataset_manager, sample_dataset):
        """测试导出场景ID为Parquet格式。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 测试不含倍增的导出
            output_file = Path(temp_dir) / "scene_ids.parquet"
            dataset_manager.export_scene_ids_parquet(sample_dataset, str(output_file), include_duplicates=False)
            
            assert output_file.exists()
            
            # 验证数据
            import pandas as pd
            df = pd.read_parquet(output_file)
            assert len(df) == 3  # 总共3个唯一scene_id
            assert set(df['scene_id'].tolist()) == {'scene_001', 'scene_002', 'scene_003'}
            
            # 测试含倍增的导出
            output_file_dup = Path(temp_dir) / "scene_ids_duplicated.parquet"
            dataset_manager.export_scene_ids_parquet(sample_dataset, str(output_file_dup), include_duplicates=True)
            
            df_dup = pd.read_parquet(output_file_dup)
            assert len(df_dup) == 8  # (2*3) + (1*2) = 8
            
            # 验证场景ID的重复次数
            scene_counts = df_dup['scene_id'].value_counts()
            assert scene_counts['scene_001'] == 3
            assert scene_counts['scene_002'] == 3
            assert scene_counts['scene_003'] == 2
    
    def test_query_scenes_parquet(self, dataset_manager, sample_dataset):
        """测试查询Parquet格式数据集。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = Path(temp_dir) / "test_dataset.parquet"
            
            # 保存数据集为parquet格式
            dataset_manager.save_dataset(sample_dataset, str(parquet_file), format='parquet')
            
            # 测试无过滤条件的查询
            df_all = dataset_manager.query_scenes_parquet(str(parquet_file))
            assert len(df_all) == 3  # 总共3个scene_id
            
            # 测试按子数据集过滤
            df_filtered = dataset_manager.query_scenes_parquet(
                str(parquet_file),
                subdataset_name="lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
            )
            assert len(df_filtered) == 2  # 该子数据集有2个scene_id
            assert set(df_filtered['scene_id'].tolist()) == {'scene_001', 'scene_002'}
            
            # 测试按倍增因子过滤
            df_dup_factor = dataset_manager.query_scenes_parquet(
                str(parquet_file),
                duplication_factor=3
            )
            assert len(df_dup_factor) == 2  # 倍增因子为3的子数据集有2个scene_id
            
            # 测试多个过滤条件
            df_multi = dataset_manager.query_scenes_parquet(
                str(parquet_file),
                subdataset_name="stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48",
                duplication_factor=2
            )
            assert len(df_multi) == 1  # 应该只有scene_003
            assert df_multi['scene_id'].iloc[0] == 'scene_003'
    
    def test_get_dataset_stats(self, dataset_manager, sample_dataset):
        """测试获取数据集统计信息。"""
        stats = dataset_manager.get_dataset_stats(sample_dataset)
        
        assert stats['dataset_name'] == 'test_dataset'
        assert stats['subdataset_count'] == 2
        assert stats['total_unique_scenes'] == 3
        assert stats['total_scenes_with_duplicates'] == 8
        assert len(stats['subdatasets']) == 2
        
        # 验证子数据集统计信息
        sub1_stats = stats['subdatasets'][0]
        assert sub1_stats['name'] == "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"
        assert sub1_stats['scene_count'] == 2
        assert sub1_stats['duplication_factor'] == 3
        assert sub1_stats['total_scenes_with_duplicates'] == 6
        
        sub2_stats = stats['subdatasets'][1]
        assert sub2_stats['name'] == "stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48"
        assert sub2_stats['scene_count'] == 1
        assert sub2_stats['duplication_factor'] == 2
        assert sub2_stats['total_scenes_with_duplicates'] == 2
    
    def test_format_auto_detection(self, dataset_manager, sample_dataset):
        """测试格式自动检测。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 保存为JSON和Parquet格式
            json_file = Path(temp_dir) / "dataset.json"
            parquet_file = Path(temp_dir) / "dataset.parquet"
            
            dataset_manager.save_dataset(sample_dataset, str(json_file), format='json')
            dataset_manager.save_dataset(sample_dataset, str(parquet_file), format='parquet')
            
            # 测试自动检测
            loaded_json = dataset_manager.load_dataset(str(json_file))
            loaded_parquet = dataset_manager.load_dataset(str(parquet_file))
            
            # 两种格式加载的数据应该基本一致
            assert loaded_json.name == loaded_parquet.name
            assert loaded_json.description == loaded_parquet.description
            assert len(loaded_json.subdatasets) == len(loaded_parquet.subdatasets)
    
    def test_parquet_large_dataset_simulation(self, dataset_manager):
        """模拟大型数据集的Parquet性能测试。"""
        # 创建一个包含较多场景ID的数据集
        large_scene_ids = [f"scene_{i:06d}" for i in range(1000)]  # 1000个场景ID
        
        subdataset = SubDataset(
            name="large_subdataset",
            obs_path="obs://large/dataset.shrink",
            duplication_factor=10,
            scene_count=len(large_scene_ids),
            scene_ids=large_scene_ids
        )
        
        dataset = Dataset(
            name="large_dataset",
            description="Large dataset for performance test",
            subdatasets=[subdataset],
            total_unique_scenes=1000,
            total_scenes=10000
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = Path(temp_dir) / "large_dataset.parquet"
            
            # 保存和加载大型数据集
            dataset_manager.save_dataset(dataset, str(parquet_file), format='parquet')
            loaded_dataset = dataset_manager.load_dataset(str(parquet_file))
            
            assert len(loaded_dataset.subdatasets[0].scene_ids) == 1000
            assert loaded_dataset.total_unique_scenes == 1000
            assert loaded_dataset.total_scenes == 10000
            
            # 测试查询性能
            df = dataset_manager.query_scenes_parquet(str(parquet_file))
            assert len(df) == 1000
    
    def test_parquet_error_handling(self, dataset_manager, sample_dataset):
        """测试Parquet格式的错误处理。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 测试不支持的格式
            with pytest.raises(ValueError, match="不支持的格式"):
                dataset_manager.save_dataset(sample_dataset, "test.txt", format='xml')
            
            # 测试加载不存在的文件
            with pytest.raises(Exception):
                dataset_manager.load_dataset(str(Path(temp_dir) / "nonexistent.parquet"))

class TestParquetIntegration:
    """Parquet格式集成测试。"""
    
    def test_json_to_parquet_conversion(self, dataset_manager, sample_dataset):
        """测试JSON到Parquet格式的转换。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            json_file = Path(temp_dir) / "dataset.json"
            parquet_file = Path(temp_dir) / "dataset.parquet"
            
            # 先保存为JSON
            dataset_manager.save_dataset(sample_dataset, str(json_file), format='json')
            
            # 从JSON加载并保存为Parquet
            loaded_dataset = dataset_manager.load_dataset(str(json_file))
            dataset_manager.save_dataset(loaded_dataset, str(parquet_file), format='parquet')
            
            # 从Parquet加载并验证
            final_dataset = dataset_manager.load_dataset(str(parquet_file))
            
            assert final_dataset.name == sample_dataset.name
            assert final_dataset.total_unique_scenes == sample_dataset.total_unique_scenes
            assert final_dataset.total_scenes == sample_dataset.total_scenes
    
    def test_mixed_format_workflow(self, dataset_manager, sample_dataset):
        """测试混合格式工作流程。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. 保存为Parquet格式
            parquet_file = Path(temp_dir) / "dataset.parquet"
            dataset_manager.save_dataset(sample_dataset, str(parquet_file), format='parquet')
            
            # 2. 导出场景ID
            scene_ids_file = Path(temp_dir) / "scene_ids.parquet"
            dataset_manager.export_scene_ids_parquet(sample_dataset, str(scene_ids_file))
            
            # 3. 查询和过滤
            df = dataset_manager.query_scenes_parquet(str(parquet_file))
            
            # 4. 获取统计信息
            stats = dataset_manager.get_dataset_stats(sample_dataset)
            
            # 验证整个工作流程
            assert len(df) == stats['total_unique_scenes']
            assert parquet_file.exists()
            assert scene_ids_file.exists() 