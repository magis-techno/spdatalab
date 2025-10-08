"""轨迹车道分析模块单元测试"""

import unittest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from shapely.geometry import LineString, Point
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer, DEFAULT_CONFIG

class TestTrajectoryLaneAnalyzer(unittest.TestCase):
    """轨迹车道分析器测试类"""
    
    def setUp(self):
        """测试前设置"""
        # 创建测试配置
        self.test_config = DEFAULT_CONFIG.copy()
        self.test_config.update({
            'buffer_radius': 10.0,
            'min_points_single_lane': 3,
            'distance_interval': 5.0,
            'time_interval': 2.0,
            'uniform_step': 10
        })
        
        # 模拟road_analysis_id
        self.test_road_analysis_id = "test_road_analysis_20241201_123456"
        
        # 创建模拟数据库引擎
        with patch('src.spdatalab.fusion.trajectory_lane_analysis.create_engine'):
            self.analyzer = TrajectoryLaneAnalyzer(self.test_config, road_analysis_id=self.test_road_analysis_id)
    
    def test_init(self):
        """测试初始化"""
        self.assertIsNotNone(self.analyzer)
        self.assertEqual(self.analyzer.config['buffer_radius'], 10.0)
        self.assertEqual(self.analyzer.config['min_points_single_lane'], 3)
        self.assertEqual(self.analyzer.road_analysis_id, self.test_road_analysis_id)
    
    def test_build_trajectory_polyline(self):
        """测试构建polyline轨迹"""
        # 创建测试数据
        test_data = {
            'longitude': [116.3, 116.31, 116.32, 116.33],
            'latitude': [39.9, 39.91, 39.92, 39.93],
            'timestamp': [1000, 2000, 3000, 4000],
            'twist_linear': [10.0, 15.0, 20.0, 12.0],
            'avp_flag': [0, 1, 1, 0],
            'workstage': [1, 1, 2, 2]
        }
        points_df = pd.DataFrame(test_data)
        
        # 测试构建
        result = self.analyzer.build_trajectory_polyline("test_scene", "test_data", points_df)
        
        # 验证结果
        self.assertIsNotNone(result)
        self.assertEqual(result['scene_id'], "test_scene")
        self.assertEqual(result['data_name'], "test_data")
        self.assertEqual(len(result['polyline']), 4)
        self.assertEqual(result['total_points'], 4)
        self.assertGreater(result['avg_speed'], 0)
        self.assertIsInstance(result['geometry'], LineString)
    
    def test_build_trajectory_polyline_empty(self):
        """测试空数据构建polyline"""
        empty_df = pd.DataFrame()
        result = self.analyzer.build_trajectory_polyline("test_scene", "test_data", empty_df)
        self.assertIsNone(result)
    
    def test_distance_based_sampling(self):
        """测试距离采样"""
        # 创建测试polyline数据
        polyline_data = {
            'polyline': [(116.3, 39.9), (116.31, 39.91), (116.32, 39.92), (116.33, 39.93)],
            'timestamps': [1000, 2000, 3000, 4000],
            'speeds': [10.0, 15.0, 20.0, 12.0],
            'avp_flags': [0, 1, 1, 0],
            'workstages': [1, 1, 2, 2]
        }
        
        # 测试采样
        result = self.analyzer._distance_based_sampling(polyline_data)
        
        # 验证结果
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertIn('coordinate', result[0])
        self.assertIn('timestamp', result[0])
        self.assertIn('original_index', result[0])
    
    def test_time_based_sampling(self):
        """测试时间采样"""
        # 创建测试polyline数据
        polyline_data = {
            'polyline': [(116.3, 39.9), (116.31, 39.91), (116.32, 39.92), (116.33, 39.93)],
            'timestamps': [1000, 3000, 5000, 7000],  # 2秒间隔
            'speeds': [10.0, 15.0, 20.0, 12.0],
            'avp_flags': [0, 1, 1, 0],
            'workstages': [1, 1, 2, 2]
        }
        
        # 测试采样
        result = self.analyzer._time_based_sampling(polyline_data)
        
        # 验证结果
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
    
    def test_uniform_sampling(self):
        """测试均匀采样"""
        # 创建测试polyline数据
        polyline_data = {
            'polyline': [(116.3 + i*0.01, 39.9 + i*0.01) for i in range(20)],
            'timestamps': [1000 + i*100 for i in range(20)],
            'speeds': [10.0 + i for i in range(20)],
            'avp_flags': [i % 2 for i in range(20)],
            'workstages': [1 + i % 3 for i in range(20)]
        }
        
        # 测试采样
        result = self.analyzer._uniform_sampling(polyline_data)
        
        # 验证结果
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertLessEqual(len(result), 3)  # 20个点，步长10，应该有2-3个点
    
    def test_calculate_window_center(self):
        """测试计算窗口中心点"""
        # 创建测试窗口点
        window_points = [
            {'coordinate': (116.3, 39.9), 'timestamp': 1000, 'speed': 10.0},
            {'coordinate': (116.31, 39.91), 'timestamp': 2000, 'speed': 15.0},
            {'coordinate': (116.32, 39.92), 'timestamp': 3000, 'speed': 20.0}
        ]
        
        # 测试计算
        result = self.analyzer._calculate_window_center(window_points)
        
        # 验证结果
        self.assertIsInstance(result, dict)
        self.assertIn('coordinate', result)
        self.assertIn('timestamp', result)
        self.assertIn('speed', result)
        self.assertEqual(result['window_size'], 3)
        self.assertAlmostEqual(result['coordinate'][0], 116.31, places=5)
        self.assertAlmostEqual(result['coordinate'][1], 39.91, places=5)
    
    @patch('src.spdatalab.fusion.trajectory_lane_analysis.wkt')
    def test_create_lane_buffer(self, mock_wkt):
        """测试创建车道缓冲区"""
        # 模拟数据库查询结果
        mock_result = Mock()
        mock_result.fetchone.return_value = ["POLYGON((116.3 39.9, 116.31 39.91, 116.32 39.92, 116.3 39.9))"]
        
        mock_conn = Mock()
        mock_conn.execute.return_value = mock_result
        
        # 模拟WKT解析
        mock_polygon = Mock()
        mock_wkt.loads.return_value = mock_polygon
        
        # 模拟数据库连接
        with patch.object(self.analyzer.engine, 'connect') as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            # 测试创建缓冲区
            result = self.analyzer.create_lane_buffer("123")
            
            # 验证结果
            self.assertIsNotNone(result)
            mock_wkt.loads.assert_called_once()
            
            # 验证SQL中包含了road_analysis_id
            call_args = mock_conn.execute.call_args
            sql_params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            self.assertEqual(sql_params['road_analysis_id'], self.test_road_analysis_id)
            self.assertEqual(sql_params['lane_id'], 123)
    
    def test_filter_trajectory_by_buffer(self):
        """测试缓冲区过滤轨迹点"""
        # 创建测试轨迹点
        trajectory_points = [
            {'coordinate': (116.3, 39.9)},
            {'coordinate': (116.31, 39.91)},
            {'coordinate': (116.32, 39.92)}
        ]
        
        # 创建模拟缓冲区
        mock_buffer = Mock()
        mock_buffer.contains.return_value = True
        mock_buffer.intersects.return_value = False
        
        # 测试过滤
        result = self.analyzer.filter_trajectory_by_buffer(trajectory_points, mock_buffer)
        
        # 验证结果
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)  # 所有点都在缓冲区内
    
    def test_check_trajectory_quality_multi_lane(self):
        """测试多车道轨迹质量检查"""
        # 创建多车道缓冲区结果
        buffer_results = [
            {'lane_id': 'lane1', 'points_count': 2},
            {'lane_id': 'lane2', 'points_count': 3}
        ]
        
        # 测试质量检查
        result = self.analyzer.check_trajectory_quality("test_data", buffer_results)
        
        # 验证结果
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['total_lanes'], 2)
        self.assertEqual(result['total_points'], 5)
        self.assertIn('多车道轨迹', result['reason'])
    
    def test_check_trajectory_quality_single_lane_pass(self):
        """测试单车道轨迹质量检查（通过）"""
        # 创建单车道缓冲区结果
        buffer_results = [
            {'lane_id': 'lane1', 'points_count': 5}  # 大于最小阈值3
        ]
        
        # 测试质量检查
        result = self.analyzer.check_trajectory_quality("test_data", buffer_results)
        
        # 验证结果
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['total_lanes'], 1)
        self.assertEqual(result['total_points'], 5)
        self.assertIn('单车道轨迹但点数充足', result['reason'])
    
    def test_check_trajectory_quality_single_lane_fail(self):
        """测试单车道轨迹质量检查（失败）"""
        # 创建单车道缓冲区结果
        buffer_results = [
            {'lane_id': 'lane1', 'points_count': 2}  # 小于最小阈值3
        ]
        
        # 测试质量检查
        result = self.analyzer.check_trajectory_quality("test_data", buffer_results)
        
        # 验证结果
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['total_lanes'], 1)
        self.assertEqual(result['total_points'], 2)
        self.assertIn('单车道轨迹点数不足', result['reason'])
    
    @patch('src.spdatalab.fusion.trajectory_lane_analysis.fetch_trajectory_points')
    def test_reconstruct_trajectory(self, mock_fetch):
        """测试轨迹重构"""
        # 模拟质量检查结果
        quality_result = {
            'status': 'passed',
            'lanes_covered': ['lane1', 'lane2'],
            'total_lanes': 2,
            'reason': 'test reason'
        }
        
        # 模拟轨迹点数据
        mock_points = pd.DataFrame({
            'longitude': [116.3, 116.31, 116.32],
            'latitude': [39.9, 39.91, 39.92],
            'timestamp': [1000, 2000, 3000],
            'twist_linear': [10.0, 15.0, 20.0],
            'avp_flag': [0, 1, 1]
        })
        mock_fetch.return_value = mock_points
        
        # 测试重构
        result = self.analyzer.reconstruct_trajectory("test_data", quality_result)
        
        # 验证结果
        self.assertIsNotNone(result)
        self.assertEqual(result['dataset_name'], "test_data")
        self.assertEqual(result['total_lanes'], 2)
        self.assertIsInstance(result['geometry'], LineString)
        self.assertEqual(result['total_points'], 3)
    
    def test_simplify_trajectory(self):
        """测试轨迹简化"""
        # 创建测试轨迹
        coordinates = [(116.3 + i*0.001, 39.9 + i*0.001) for i in range(10)]
        trajectory = LineString(coordinates)
        
        # 测试简化
        result = self.analyzer.simplify_trajectory(trajectory)
        
        # 验证结果
        self.assertIsInstance(result, LineString)
        self.assertLessEqual(len(result.coords), len(trajectory.coords))
    
    def test_save_results(self):
        """测试保存结果"""
        # 创建测试结果
        test_results = [
            {
                'scene_id': 'test_scene',
                'dataset_name': 'test_data',
                'geometry': LineString([(116.3, 39.9), (116.31, 39.91)]),
                'total_lanes': 2,
                'total_points': 100,
                'quality_status': 'passed',
                'quality_reason': 'test reason',
                'lanes_covered': ['lane1', 'lane2'],
                'avg_speed': 15.0,
                'max_speed': 25.0,
                'min_speed': 5.0,
                'avp_ratio': 0.5,
                'trajectory_length': 0.01
            }
        ]
        
        # 模拟保存方法
        with patch.object(self.analyzer, '_save_quality_results') as mock_save:
            mock_save.return_value = 1
            
            # 测试保存
            result = self.analyzer.save_results(test_results)
            
            # 验证结果
            self.assertTrue(result)
            mock_save.assert_called_once_with(test_results)

class TestIntegration(unittest.TestCase):
    """集成测试"""
    
    def setUp(self):
        """测试前设置"""
        self.test_config = DEFAULT_CONFIG.copy()
        self.test_config.update({
            'buffer_radius': 10.0,
            'min_points_single_lane': 3,
            'batch_size': 2
        })
        self.test_road_analysis_id = "test_road_analysis_integration"
    
    @patch('src.spdatalab.fusion.trajectory_lane_analysis.create_engine')
    @patch('src.spdatalab.fusion.trajectory_lane_analysis.load_scene_data_mappings')
    def test_main_workflow(self, mock_load, mock_engine):
        """测试主工作流程"""
        # 创建模拟数据
        mock_mappings = pd.DataFrame({
            'scene_id': ['scene1', 'scene2'],
            'data_name': ['data1', 'data2'],
            'subdataset_name': ['sub1', 'sub2']
        })
        mock_load.return_value = mock_mappings
        
        # 创建分析器
        analyzer = TrajectoryLaneAnalyzer(self.test_config, road_analysis_id=self.test_road_analysis_id)
        
        # 模拟各个处理步骤
        with patch.object(analyzer, 'create_database_tables', return_value=True), \
             patch.object(analyzer, 'build_trajectory_polyline', return_value={'test': 'data'}), \
             patch.object(analyzer, 'sample_trajectory', return_value=[]), \
             patch.object(analyzer, 'sliding_window_analysis', return_value=[]), \
             patch.object(analyzer, 'check_trajectory_quality', return_value={'status': 'failed'}), \
             patch('src.spdatalab.fusion.trajectory_lane_analysis.fetch_trajectory_points') as mock_fetch:
            
            # 模拟空轨迹点
            mock_fetch.return_value = pd.DataFrame()
            
            # 测试处理
            stats = analyzer.process_scene_mappings(mock_mappings)
            
            # 验证统计结果
            self.assertIsInstance(stats, dict)
            self.assertEqual(stats['total_scenes'], 2)
            self.assertIn('processed_scenes', stats)
            self.assertIn('empty_scenes', stats)

if __name__ == '__main__':
    unittest.main() 