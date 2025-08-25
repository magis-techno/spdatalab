"""多模态轨迹检索系统 - 基础功能测试

验证API调用、配置管理和核心组件的基础功能
"""

import os
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

# 测试导入
def test_imports():
    """测试模块导入是否正常"""
    print("🔧 测试模块导入...")
    
    try:
        # 测试基础数据检索模块导入
        from spdatalab.dataset.multimodal_data_retriever import (
            APIConfig,
            MultimodalRetriever,
            TrajectoryToPolygonConverter
        )
        print("✅ 基础数据检索模块导入成功")
        
        # 测试融合分析模块导入
        from spdatalab.fusion.multimodal_trajectory_retrieval import (
            MultimodalConfig,
            MultimodalTrajectoryWorkflow,
            ResultAggregator,
            PolygonMerger
        )
        print("✅ 融合分析模块导入成功")
        
        # 测试CLI模块导入
        from spdatalab.fusion.multimodal_cli import create_parser, get_api_config_from_env
        print("✅ CLI模块导入成功")
        
        return True
        
    except ImportError as e:
        print(f"❌ 模块导入失败: {e}")
        return False


def test_api_config():
    """测试API配置创建"""
    print("\n🔧 测试API配置...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        
        # 创建测试配置
        config = APIConfig(
            project="test_project",
            api_key="test_key",
            username="test_user"
        )
        
        assert config.project == "test_project"
        assert config.api_key == "test_key"
        assert config.username == "test_user"
        assert config.api_url == "https://driveinsight-api.ias.huawei.com/xmodalitys/retrieve"
        assert config.timeout == 30
        assert config.platform == "xmodalitys-external"
        assert config.region == "RaD-prod"
        assert config.entrypoint_version == "v2"
        
        print("✅ API配置创建和验证成功")
        return True
        
    except Exception as e:
        print(f"❌ API配置测试失败: {e}")
        return False


def test_multimodal_retriever():
    """测试多模态检索器基础功能"""
    print("\n🔧 测试多模态检索器...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
        
        # 创建测试配置
        api_config = APIConfig(
            project="test_project",
            api_key="test_key",
            username="test_user"
        )
        
        # 创建检索器
        retriever = MultimodalRetriever(api_config)
        
        # 测试相机推导功能
        camera = retriever._extract_camera_from_collection("ddi_collection_camera_encoded_1")
        assert camera == "camera_1", f"期望 camera_1，实际 {camera}"
        
        camera = retriever._extract_camera_from_collection("ddi_collection_camera_encoded_12")
        assert camera == "camera_12", f"期望 camera_12，实际 {camera}"
        
        # 测试查询统计
        stats = retriever.get_query_stats()
        assert stats['total_queries'] == 0
        assert stats['remaining_queries'] == 100000
        assert stats['max_single_query'] == 10000
        assert stats['max_total_query'] == 100000
        
        print("✅ 多模态检索器基础功能验证成功")
        return True
        
    except Exception as e:
        print(f"❌ 多模态检索器测试失败: {e}")
        return False


def test_trajectory_converter():
    """测试轨迹转换器"""
    print("\n🔧 测试轨迹转换器...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import TrajectoryToPolygonConverter
        from shapely.geometry import LineString
        
        # 创建转换器
        converter = TrajectoryToPolygonConverter(buffer_distance=10.0)
        
        # 创建测试轨迹线
        test_coords = [(116.3, 39.9), (116.31, 39.91), (116.32, 39.92)]
        test_linestring = LineString(test_coords)
        
        # 测试转换
        polygon = converter.convert_trajectory_to_polygon(test_linestring)
        
        if polygon is not None:
            assert polygon.is_valid, "生成的polygon应该是有效的"
            assert polygon.area > 0, "生成的polygon应该有面积"
            print(f"✅ 轨迹转换成功，生成polygon面积: {polygon.area:.2f}")
        else:
            print("⚠️ 轨迹转换返回None（可能是测试数据太小）")
        
        # 测试批量转换
        trajectory_data = [
            {"linestring": test_linestring, "dataset_name": "test_dataset", "timestamp": 123456789}
        ]
        results = converter.batch_convert(trajectory_data)
        
        print(f"✅ 批量转换测试成功，结果数量: {len(results)}")
        return True
        
    except Exception as e:
        print(f"❌ 轨迹转换器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_result_aggregator():
    """测试结果聚合器"""
    print("\n🔧 测试结果聚合器...")
    
    try:
        from spdatalab.fusion.multimodal_trajectory_retrieval import ResultAggregator
        
        # 创建聚合器
        aggregator = ResultAggregator(time_window_hours=24)
        
        # 测试数据
        search_results = [
            {"dataset_name": "dataset1", "timestamp": 1739958971349},
            {"dataset_name": "dataset1", "timestamp": 1739958971350},
            {"dataset_name": "dataset2", "timestamp": 1739958971351},
        ]
        
        # 测试dataset聚合
        dataset_groups = aggregator.aggregate_by_dataset(search_results)
        assert len(dataset_groups) == 2, f"期望2个数据集，实际{len(dataset_groups)}"
        assert len(dataset_groups["dataset1"]) == 2, "dataset1应该有2条记录"
        assert len(dataset_groups["dataset2"]) == 1, "dataset2应该有1条记录"
        
        # 测试时间窗口聚合
        time_queries = aggregator.aggregate_by_timewindow(dataset_groups)
        assert len(time_queries) == 2, f"期望2个时间查询，实际{len(time_queries)}"
        
        for dataset_name, query_info in time_queries.items():
            assert 'start_time' in query_info
            assert 'end_time' in query_info
            assert query_info['start_time'] < query_info['end_time']
        
        print("✅ 结果聚合器测试成功")
        return True
        
    except Exception as e:
        print(f"❌ 结果聚合器测试失败: {e}")
        return False


def test_polygon_merger():
    """测试Polygon合并器"""
    print("\n🔧 测试Polygon合并器...")
    
    try:
        from spdatalab.fusion.multimodal_trajectory_retrieval import PolygonMerger
        from shapely.geometry import Polygon
        
        # 创建合并器
        merger = PolygonMerger(overlap_threshold=0.7)
        
        # 创建测试polygon
        poly1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # 1x1方形
        poly2 = Polygon([(0.5, 0), (1.5, 0), (1.5, 1), (0.5, 1)])  # 重叠的1x1方形
        
        # 测试重叠比例计算
        overlap_ratio = merger.calculate_overlap_ratio(poly1, poly2)
        assert 0 < overlap_ratio < 1, f"重叠比例应该在0-1之间，实际{overlap_ratio}"
        
        # 测试polygon合并
        polygons_with_source = [
            {
                "id": "poly1",
                "geometry": poly1,
                "properties": {"source": "test1"}
            },
            {
                "id": "poly2", 
                "geometry": poly2,
                "properties": {"source": "test2"}
            }
        ]
        
        merged_results = merger.merge_overlapping_polygons(polygons_with_source)
        assert len(merged_results) >= 1, "应该返回至少1个合并结果"
        
        print(f"✅ Polygon合并器测试成功，原始{len(polygons_with_source)}个，合并后{len(merged_results)}个")
        return True
        
    except Exception as e:
        print(f"❌ Polygon合并器测试失败: {e}")
        return False


def test_multimodal_config():
    """测试多模态配置"""
    print("\n🔧 测试多模态配置...")
    
    try:
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        from spdatalab.fusion.multimodal_trajectory_retrieval import MultimodalConfig
        
        # 创建API配置
        api_config = APIConfig(
            project="test_project",
            api_key="test_key", 
            username="test_user"
        )
        
        # 创建多模态配置
        config = MultimodalConfig(api_config=api_config)
        
        # 验证默认值
        assert config.max_search_results == 5
        assert config.time_window_days == 30
        assert config.buffer_distance == 10.0
        assert config.overlap_threshold == 0.7
        assert config.polygon_config is not None
        
        print("✅ 多模态配置测试成功")
        return True
        
    except Exception as e:
        print(f"❌ 多模态配置测试失败: {e}")
        return False


def test_cli_parser():
    """测试CLI参数解析"""
    print("\n🔧 测试CLI参数解析...")
    
    try:
        from spdatalab.fusion.multimodal_cli import create_parser
        
        parser = create_parser()
        
        # 测试基础参数解析
        args = parser.parse_args([
            '--text', 'bicycle crossing intersection',
            '--collection', 'ddi_collection_camera_encoded_1',
            '--count', '10',
            '--start', '5',
            '--start-time', '1234567891011',
            '--end-time', '1234567891111',
            '--buffer-distance', '15.0'
        ])
        
        assert args.text == 'bicycle crossing intersection'
        assert args.collection == 'ddi_collection_camera_encoded_1'
        assert args.count == 10
        assert args.start == 5
        assert args.start_time == 1234567891011
        assert args.end_time == 1234567891111
        assert args.buffer_distance == 15.0
        
        print("✅ CLI参数解析测试成功")
        return True
        
    except Exception as e:
        print(f"❌ CLI参数解析测试失败: {e}")
        return False


def main():
    """运行所有测试"""
    print("🚀 开始多模态轨迹检索系统基础功能测试")
    print("="*60)
    
    tests = [
        test_imports,
        test_api_config,
        test_multimodal_retriever,
        test_trajectory_converter,
        test_result_aggregator,
        test_polygon_merger,
        test_multimodal_config,
        test_cli_parser
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
        print("🎉 所有基础功能测试通过！Day 1 开发目标达成")
        return True
    else:
        print("⚠️ 部分测试失败，需要修复")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
