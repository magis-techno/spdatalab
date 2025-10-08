"""
集成轨迹分析测试

测试完整的两阶段轨迹分析流程：
1. GeoJSON输入处理
2. 道路分析
3. 车道分析
4. 结果集成
5. 报告生成
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

from spdatalab.fusion.integrated_trajectory_analysis import (
    IntegratedTrajectoryAnalyzer,
    analyze_trajectories_from_geojson,
    create_analysis_summary
)
from spdatalab.fusion.integrated_analysis_config import (
    IntegratedAnalysisConfig,
    create_default_config,
    create_fast_config
)
from spdatalab.fusion.geojson_utils import create_sample_geojson


@pytest.fixture
def sample_geojson_file():
    """创建示例GeoJSON文件"""
    sample_data = [
        {
            "scene_id": "test_scene_1",
            "data_name": "test_data_1",
            "coordinates": [[116.3, 39.9], [116.31, 39.91], [116.32, 39.92]]
        },
        {
            "scene_id": "test_scene_2",
            "data_name": "test_data_2",
            "coordinates": [[116.4, 39.8], [116.41, 39.81], [116.42, 39.82]]
        }
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
        temp_file = f.name
    
    create_sample_geojson(temp_file, sample_data)
    yield temp_file
    
    # 清理
    Path(temp_file).unlink(missing_ok=True)


@pytest.fixture
def mock_config():
    """创建模拟配置"""
    config = create_default_config()
    config.dry_run = True
    config.debug_mode = True
    config.output_config.export_path = "test_output"
    return config


@pytest.fixture
def mock_road_analysis_results():
    """模拟道路分析结果"""
    return [
        ("test_scene_1", "road_analysis_1", {
            "total_lanes": 10,
            "total_intersections": 2,
            "total_roads": 5,
            "data_name": "test_data_1",
            "properties": {"key": "value1"}
        }),
        ("test_scene_2", "road_analysis_2", {
            "total_lanes": 15,
            "total_intersections": 3,
            "total_roads": 7,
            "data_name": "test_data_2",
            "properties": {"key": "value2"}
        })
    ]


@pytest.fixture
def mock_lane_analysis_results():
    """模拟车道分析结果"""
    return [
        ("test_scene_1", "lane_analysis_1", {
            "quality_passed": 8,
            "quality_failed": 2,
            "total_reconstructed": 6,
            "data_name": "test_data_1",
            "properties": {"key": "value1"}
        }),
        ("test_scene_2", "lane_analysis_2", {
            "quality_passed": 12,
            "quality_failed": 3,
            "total_reconstructed": 9,
            "data_name": "test_data_2",
            "properties": {"key": "value2"}
        })
    ]


class TestIntegratedTrajectoryAnalyzer:
    """集成轨迹分析器测试类"""
    
    def test_analyzer_initialization(self, mock_config):
        """测试分析器初始化"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        assert analyzer.config == mock_config
        assert analyzer.analysis_id is None
        assert analyzer.analysis_start_time is None
        assert "trajectories" in analyzer.analysis_results
        assert "road_analysis_results" in analyzer.analysis_results
        assert "lane_analysis_results" in analyzer.analysis_results
        assert "errors" in analyzer.analysis_results
        assert "summary" in analyzer.analysis_results
    
    def test_analyzer_default_config(self):
        """测试默认配置初始化"""
        analyzer = IntegratedTrajectoryAnalyzer()
        
        assert analyzer.config is not None
        assert analyzer.config.analysis_name == "integrated_trajectory_analysis"
    
    def test_validate_input_file_success(self, sample_geojson_file, mock_config):
        """测试输入文件验证成功"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        # 应该不抛出异常
        analyzer._validate_input_file(sample_geojson_file)
    
    def test_validate_input_file_not_exist(self, mock_config):
        """测试输入文件不存在"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        with pytest.raises(FileNotFoundError):
            analyzer._validate_input_file("nonexistent.geojson")
    
    def test_validate_input_file_invalid_format(self, mock_config):
        """测试输入文件格式无效"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        # 创建无效的GeoJSON文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            f.write("invalid json")
            invalid_file = f.name
        
        try:
            with pytest.raises(ValueError):
                analyzer._validate_input_file(invalid_file)
        finally:
            Path(invalid_file).unlink(missing_ok=True)
    
    def test_load_trajectories_success(self, sample_geojson_file, mock_config):
        """测试轨迹加载成功"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        trajectories = analyzer._load_trajectories(sample_geojson_file)
        
        assert len(trajectories) == 2
        assert trajectories[0].scene_id == "test_scene_1"
        assert trajectories[1].scene_id == "test_scene_2"
        assert analyzer.analysis_results['trajectories'] == trajectories
    
    def test_load_trajectories_empty_file(self, mock_config):
        """测试空文件加载"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        # 创建空的GeoJSON文件
        empty_data = {"type": "FeatureCollection", "features": []}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            json.dump(empty_data, f)
            empty_file = f.name
        
        try:
            with pytest.raises(ValueError):
                analyzer._load_trajectories(empty_file)
        finally:
            Path(empty_file).unlink(missing_ok=True)
    
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_trajectories_from_geojson')
    def test_execute_road_analysis_success(self, mock_batch_road_analysis, 
                                          sample_geojson_file, mock_config, mock_road_analysis_results):
        """测试道路分析执行成功"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        analyzer.analysis_id = "test_analysis"
        
        # 模拟道路分析结果
        mock_batch_road_analysis.return_value = mock_road_analysis_results
        
        # 加载轨迹
        trajectories = analyzer._load_trajectories(sample_geojson_file)
        
        # 执行道路分析
        road_results = analyzer._execute_road_analysis(sample_geojson_file, trajectories)
        
        assert len(road_results) == 2
        assert road_results == mock_road_analysis_results
        assert analyzer.analysis_results['road_analysis_results'] == road_results
        
        # 验证调用参数
        mock_batch_road_analysis.assert_called_once_with(
            geojson_file=sample_geojson_file,
            batch_analysis_id="test_analysis_road",
            config=mock_config.road_analysis_config
        )
    
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_trajectories_from_geojson')
    def test_execute_road_analysis_empty_results(self, mock_batch_road_analysis, 
                                                sample_geojson_file, mock_config):
        """测试道路分析返回空结果"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        analyzer.analysis_id = "test_analysis"
        
        # 模拟空结果
        mock_batch_road_analysis.return_value = []
        
        # 加载轨迹
        trajectories = analyzer._load_trajectories(sample_geojson_file)
        
        # 执行道路分析应该抛出异常
        with pytest.raises(ValueError):
            analyzer._execute_road_analysis(sample_geojson_file, trajectories)
    
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_lanes_from_road_results')
    def test_execute_lane_analysis_success(self, mock_batch_lane_analysis, 
                                          mock_config, mock_road_analysis_results, mock_lane_analysis_results):
        """测试车道分析执行成功"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        analyzer.analysis_id = "test_analysis"
        
        # 模拟车道分析结果
        mock_batch_lane_analysis.return_value = mock_lane_analysis_results
        
        # 执行车道分析
        lane_results = analyzer._execute_lane_analysis(mock_road_analysis_results)
        
        assert len(lane_results) == 2
        assert lane_results == mock_lane_analysis_results
        assert analyzer.analysis_results['lane_analysis_results'] == lane_results
        
        # 验证调用参数
        mock_batch_lane_analysis.assert_called_once_with(
            road_analysis_results=mock_road_analysis_results,
            batch_analysis_id="test_analysis_lane",
            config=mock_config.lane_analysis_config.__dict__
        )
    
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_lanes_from_road_results')
    def test_execute_lane_analysis_empty_results(self, mock_batch_lane_analysis, 
                                                mock_config, mock_road_analysis_results):
        """测试车道分析返回空结果"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        analyzer.analysis_id = "test_analysis"
        
        # 模拟空结果
        mock_batch_lane_analysis.return_value = []
        
        # 执行车道分析不应该抛出异常，返回空列表
        lane_results = analyzer._execute_lane_analysis(mock_road_analysis_results)
        
        assert lane_results == []
    
    def test_calculate_road_analysis_stats(self, mock_config, mock_road_analysis_results):
        """测试道路分析统计计算"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        stats = analyzer._calculate_road_analysis_stats(mock_road_analysis_results)
        
        assert stats['total_lanes'] == 25  # 10 + 15
        assert stats['total_intersections'] == 5  # 2 + 3
        assert stats['total_roads'] == 12  # 5 + 7
        assert stats['avg_lanes_per_trajectory'] == 12.5  # 25 / 2
        assert stats['avg_intersections_per_trajectory'] == 2.5  # 5 / 2
        assert stats['avg_roads_per_trajectory'] == 6.0  # 12 / 2
    
    def test_calculate_lane_analysis_stats(self, mock_config, mock_lane_analysis_results):
        """测试车道分析统计计算"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        stats = analyzer._calculate_lane_analysis_stats(mock_lane_analysis_results)
        
        assert stats['total_quality_passed'] == 20  # 8 + 12
        assert stats['total_quality_failed'] == 5  # 2 + 3
        assert stats['total_reconstructed'] == 15  # 6 + 9
        assert stats['quality_pass_rate'] == 80.0  # 20 / 25 * 100
        assert stats['avg_quality_passed_per_trajectory'] == 10.0  # 20 / 2
        assert stats['avg_reconstructed_per_trajectory'] == 7.5  # 15 / 2
    
    def test_calculate_stats_empty_results(self, mock_config):
        """测试空结果统计计算"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        
        road_stats = analyzer._calculate_road_analysis_stats([])
        lane_stats = analyzer._calculate_lane_analysis_stats([])
        
        assert road_stats == {}
        assert lane_stats == {}
    
    def test_generate_integrated_results(self, mock_config, sample_geojson_file, 
                                       mock_road_analysis_results, mock_lane_analysis_results):
        """测试综合结果生成"""
        analyzer = IntegratedTrajectoryAnalyzer(mock_config)
        analyzer.analysis_id = "test_analysis"
        analyzer.analysis_start_time = datetime.now()
        
        # 加载轨迹
        trajectories = analyzer._load_trajectories(sample_geojson_file)
        
        # 生成综合结果
        results = analyzer._generate_integrated_results(
            trajectories, mock_road_analysis_results, mock_lane_analysis_results
        )
        
        assert results['analysis_id'] == "test_analysis"
        assert results['status'] == 'completed'
        assert 'start_time' in results
        assert 'end_time' in results
        assert 'duration' in results
        assert 'summary' in results
        
        # 验证统计信息
        summary = results['summary']
        assert summary['total_trajectories'] == 2
        assert summary['successful_road_analyses'] == 2
        assert summary['successful_lane_analyses'] == 2
        assert summary['road_success_rate'] == 100.0
        assert summary['lane_success_rate'] == 100.0
        
        # 验证轨迹信息
        assert len(results['trajectories']) == 2
        assert results['trajectories'][0]['scene_id'] == "test_scene_1"
        assert results['trajectories'][1]['scene_id'] == "test_scene_2"


class TestIntegratedAnalysisFlow:
    """集成分析流程测试类"""
    
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_trajectories_from_geojson')
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_lanes_from_road_results')
    def test_complete_analysis_flow(self, mock_lane_analysis, mock_road_analysis, 
                                   sample_geojson_file, mock_config, 
                                   mock_road_analysis_results, mock_lane_analysis_results):
        """测试完整分析流程"""
        # 设置模拟返回值
        mock_road_analysis.return_value = mock_road_analysis_results
        mock_lane_analysis.return_value = mock_lane_analysis_results
        
        # 执行分析
        results = analyze_trajectories_from_geojson(
            geojson_file=sample_geojson_file,
            config=mock_config,
            analysis_id="test_complete_analysis"
        )
        
        # 验证结果
        assert results['status'] == 'completed'
        assert results['analysis_id'] == "test_complete_analysis"
        assert len(results['trajectories']) == 2
        assert len(results['road_analysis_results']) == 2
        assert len(results['lane_analysis_results']) == 2
        
        # 验证统计信息
        summary = results['summary']
        assert summary['total_trajectories'] == 2
        assert summary['successful_road_analyses'] == 2
        assert summary['successful_lane_analyses'] == 2
        assert summary['road_success_rate'] == 100.0
        assert summary['lane_success_rate'] == 100.0
    
    @patch('spdatalab.fusion.integrated_trajectory_analysis.batch_analyze_trajectories_from_geojson')
    def test_analysis_with_road_failures(self, mock_road_analysis, sample_geojson_file, mock_config):
        """测试包含道路分析失败的情况"""
        # 模拟部分失败的道路分析结果
        mock_road_results = [
            ("test_scene_1", "road_analysis_1", {
                "total_lanes": 10,
                "total_intersections": 2,
                "total_roads": 5,
                "data_name": "test_data_1"
            }),
            ("test_scene_2", None, {
                "error": "道路分析失败",
                "data_name": "test_data_2"
            })
        ]
        mock_road_analysis.return_value = mock_road_results
        
        # 执行分析
        results = analyze_trajectories_from_geojson(
            geojson_file=sample_geojson_file,
            config=mock_config
        )
        
        # 验证结果
        assert results['status'] == 'completed'
        assert results['summary']['total_trajectories'] == 2
        assert results['summary']['successful_road_analyses'] == 1
        assert results['summary']['road_success_rate'] == 50.0
        assert results['summary']['total_errors'] >= 1
        
        # 验证错误记录
        errors = results['errors']
        road_errors = [e for e in errors if e['stage'] == 'road_analysis']
        assert len(road_errors) >= 1
        assert road_errors[0]['trajectory_id'] == "test_scene_2"
    
    def test_analysis_with_invalid_file(self, mock_config):
        """测试无效文件输入"""
        results = analyze_trajectories_from_geojson(
            geojson_file="nonexistent.geojson",
            config=mock_config
        )
        
        assert results['status'] == 'failed'
        assert 'error' in results
        assert len(results['analysis_results']['errors']) > 0


class TestUtilityFunctions:
    """工具函数测试类"""
    
    def test_create_analysis_summary_success(self):
        """测试成功分析摘要创建"""
        results = {
            'analysis_id': 'test_analysis',
            'status': 'completed',
            'duration': '0:01:30',
            'summary': {
                'total_trajectories': 5,
                'road_success_rate': 80.0,
                'lane_success_rate': 90.0,
                'total_errors': 2
            }
        }
        
        summary = create_analysis_summary(results)
        
        assert "test_analysis" in summary
        assert "总轨迹数: 5" in summary
        assert "道路分析成功率: 80.0%" in summary
        assert "车道分析成功率: 90.0%" in summary
        assert "错误数: 2" in summary
    
    def test_create_analysis_summary_failed(self):
        """测试失败分析摘要创建"""
        results = {
            'status': 'failed',
            'error': '输入文件无效'
        }
        
        summary = create_analysis_summary(results)
        
        assert "分析失败" in summary
        assert "输入文件无效" in summary
    
    def test_create_analysis_summary_no_errors(self):
        """测试无错误分析摘要创建"""
        results = {
            'analysis_id': 'test_analysis',
            'status': 'completed',
            'duration': '0:01:30',
            'summary': {
                'total_trajectories': 3,
                'road_success_rate': 100.0,
                'lane_success_rate': 100.0,
                'total_errors': 0
            }
        }
        
        summary = create_analysis_summary(results)
        
        assert "错误数" not in summary
        assert "总轨迹数: 3" in summary
        assert "100.0%" in summary


class TestConfigurationHandling:
    """配置处理测试类"""
    
    def test_different_config_presets(self, sample_geojson_file):
        """测试不同配置预设"""
        # 测试默认配置
        default_config = create_default_config()
        analyzer1 = IntegratedTrajectoryAnalyzer(default_config)
        assert analyzer1.config.road_analysis_config.forward_chain_limit == 500.0
        
        # 测试快速配置
        fast_config = create_fast_config()
        analyzer2 = IntegratedTrajectoryAnalyzer(fast_config)
        assert analyzer2.config.road_analysis_config.forward_chain_limit == 200.0
    
    def test_config_validation(self):
        """测试配置验证"""
        config = create_default_config()
        
        # 测试有效配置
        analyzer = IntegratedTrajectoryAnalyzer(config)
        assert analyzer.config is not None
        
        # 测试无效配置
        config.road_analysis_config.buffer_distance = -1.0
        with pytest.raises(ValueError):
            IntegratedTrajectoryAnalyzer(config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 