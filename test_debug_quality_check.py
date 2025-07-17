"""调试质检轨迹查询模块

专门用于测试和定位问题的调试脚本
"""

import logging
import sys
from pathlib import Path

# 配置详细的日志输出
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_basic_functionality():
    """测试基本功能"""
    logger.info("🚀 开始调试测试")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import (
            QualityCheckConfig,
            QualityCheckRecord,
            ResultFieldProcessor,
            TrajectorySegmenter
        )
        
        logger.info("✅ 模块导入成功")
        
        # 测试配置
        config = QualityCheckConfig()
        logger.info(f"✅ 配置创建成功: min_points={config.min_points_per_segment}")
        
        # 测试结果字段处理
        processor = ResultFieldProcessor()
        result = processor.merge_and_clean_results(
            ['压线行驶'], 
            ['压斑马线']
        )
        logger.info(f"✅ 结果字段处理成功: {result}")
        
        # 测试轨迹分段器
        segmenter = TrajectorySegmenter(config)
        logger.info("✅ 轨迹分段器创建成功")
        
        # 创建测试记录
        test_record = QualityCheckRecord(
            task_name='test_task',
            annotator='test_annotator',
            autoscene_id='test_scene_id',
            result=['压线行驶'],
            description=[[0.0, 4.0], [10.0, 15.0]],
            other_scenario=['压斑马线']
        )
        logger.info(f"✅ 测试记录创建成功: {test_record.autoscene_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 基本功能测试失败: {str(e)}")
        import traceback
        logger.error(f"错误详情: {traceback.format_exc()}")
        return False

def test_trajectory_segmenter_with_mock_data():
    """使用模拟数据测试轨迹分段器"""
    logger.info("🔧 测试轨迹分段器（模拟数据）")
    
    try:
        import pandas as pd
        from spdatalab.dataset.quality_check_trajectory_query import (
            QualityCheckConfig,
            TrajectorySegmenter
        )
        
        # 创建模拟轨迹数据
        timestamps = [1000000000 + i for i in range(20)]  # 20个时间点
        longitudes = [116.3 + i * 0.001 for i in range(20)]  # 经度递增
        latitudes = [39.9 + i * 0.0005 for i in range(20)]   # 纬度递增
        
        mock_trajectory_df = pd.DataFrame({
            'timestamp': timestamps,
            'longitude': longitudes,
            'latitude': latitudes,
            'twist_linear': [10.0] * 20,
            'avp_flag': [1] * 20,
            'workstage': ['normal'] * 20
        })
        
        logger.info(f"✅ 创建模拟轨迹数据: {len(mock_trajectory_df)} 个点")
        logger.info(f"   时间范围: {mock_trajectory_df['timestamp'].min()} - {mock_trajectory_df['timestamp'].max()}")
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 测试完整轨迹创建
        logger.info("📋 测试完整轨迹创建...")
        geometry, duration = segmenter.create_complete_trajectory(mock_trajectory_df)
        
        if geometry.is_empty:
            logger.error("❌ 完整轨迹创建失败")
            return False
        else:
            logger.info(f"✅ 完整轨迹创建成功: 时长={duration}s, 几何类型={geometry.geom_type}")
        
        # 测试时间分段
        logger.info("📋 测试时间分段...")
        time_ranges = [[0.0, 5.0], [10.0, 15.0]]  # 两个时间区间
        geometry2, segment_count = segmenter.segment_trajectory_by_time_ranges(
            mock_trajectory_df, time_ranges
        )
        
        if geometry2.is_empty:
            logger.error("❌ 时间分段失败")
            return False
        else:
            logger.info(f"✅ 时间分段成功: {segment_count} 个分段, 几何类型={geometry2.geom_type}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 轨迹分段器测试失败: {str(e)}")
        import traceback
        logger.error(f"错误详情: {traceback.format_exc()}")
        return False

def test_problematic_cases():
    """测试可能有问题的情况"""
    logger.info("🔧 测试问题情况")
    
    try:
        import pandas as pd
        from spdatalab.dataset.quality_check_trajectory_query import (
            QualityCheckConfig,
            TrajectorySegmenter
        )
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 测试1: 空数据
        logger.info("📋 测试1: 空DataFrame")
        empty_df = pd.DataFrame()
        geometry, duration = segmenter.create_complete_trajectory(empty_df)
        logger.info(f"   结果: geometry.is_empty={geometry.is_empty}, duration={duration}")
        
        # 测试2: 点数不足
        logger.info("📋 测试2: 点数不足")
        insufficient_df = pd.DataFrame({
            'timestamp': [1000000000],
            'longitude': [116.3],
            'latitude': [39.9]
        })
        geometry, duration = segmenter.create_complete_trajectory(insufficient_df)
        logger.info(f"   结果: geometry.is_empty={geometry.is_empty}, duration={duration}")
        
        # 测试3: 包含空值的数据
        logger.info("📋 测试3: 包含空值")
        null_df = pd.DataFrame({
            'timestamp': [1000000000, 1000000001, 1000000002],
            'longitude': [116.3, None, 116.5],
            'latitude': [39.9, 39.91, None]
        })
        geometry, duration = segmenter.create_complete_trajectory(null_df)
        logger.info(f"   结果: geometry.is_empty={geometry.is_empty}, duration={duration}")
        
        # 测试4: 无效时间区间
        logger.info("📋 测试4: 无效时间区间")
        normal_df = pd.DataFrame({
            'timestamp': [1000000000 + i for i in range(10)],
            'longitude': [116.3 + i * 0.001 for i in range(10)],
            'latitude': [39.9 + i * 0.0005 for i in range(10)]
        })
        
        invalid_time_ranges = [
            [5.0, 2.0],    # 开始时间 > 结束时间
            [-10.0, -5.0], # 时间区间在轨迹开始之前
            [20.0, 25.0]   # 时间区间在轨迹结束之后
        ]
        
        geometry, segment_count = segmenter.segment_trajectory_by_time_ranges(
            normal_df, invalid_time_ranges
        )
        logger.info(f"   结果: geometry.is_empty={geometry.is_empty}, segment_count={segment_count}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 问题情况测试失败: {str(e)}")
        import traceback
        logger.error(f"错误详情: {traceback.format_exc()}")
        return False

def main():
    """主测试函数"""
    logger.info("🚀 开始调试质检轨迹查询模块")
    
    # 执行测试
    tests = [
        ("基本功能测试", test_basic_functionality),
        ("轨迹分段器测试", test_trajectory_segmenter_with_mock_data),
        ("问题情况测试", test_problematic_cases),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"🔧 {test_name}")
        logger.info(f"{'='*50}")
        
        result = test_func()
        results.append((test_name, result))
        
        if result:
            logger.info(f"✅ {test_name} 通过")
        else:
            logger.error(f"❌ {test_name} 失败")
    
    # 总结
    logger.info(f"\n{'='*50}")
    logger.info("📊 测试总结")
    logger.info(f"{'='*50}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总计: {passed}/{total} 个测试通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！")
        return 0
    else:
        logger.error("❌ 部分测试失败")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 