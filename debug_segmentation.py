"""调试轨迹分段功能

专门用于调试和验证轨迹分段逻辑的脚本
"""

import logging
import pandas as pd
from typing import List

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_description_parsing():
    """测试description字段解析"""
    logger.info("🔧 测试description字段解析")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import ExcelDataParser, QualityCheckConfig
        
        config = QualityCheckConfig()
        parser = ExcelDataParser(config)
        
        # 测试用例
        test_cases = [
            "[[0.0, 4.0], [13.0, 20.0], [4.0, 7.0]]",  # 标准格式
            "[[0, 4], [13, 20]]",                       # 整数格式
            "[]",                                        # 空列表
            "",                                          # 空字符串
            "[[0.0, 4.0]]",                             # 单个区间
            "[[4.0, 0.0]]",                             # 错误的时间顺序
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            logger.info(f"\n测试用例 {i}: {test_case}")
            result = parser._parse_description_field(test_case)
            logger.info(f"解析结果: {result}")
            
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        return False

def test_trajectory_segmentation():
    """测试轨迹分段逻辑"""
    logger.info("🔧 测试轨迹分段逻辑")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 创建模拟轨迹数据（20秒，每秒1个点）
        timestamps = [1000000000 + i for i in range(20)]
        longitudes = [116.3 + i * 0.001 for i in range(20)]
        latitudes = [39.9 + i * 0.0005 for i in range(20)]
        
        trajectory_df = pd.DataFrame({
            'timestamp': timestamps,
            'longitude': longitudes,
            'latitude': latitudes,
            'twist_linear': [10.0] * 20,
            'avp_flag': [1] * 20,
            'workstage': ['normal'] * 20
        })
        
        logger.info(f"创建模拟轨迹: {len(trajectory_df)} 个点，时长 {trajectory_df['timestamp'].max() - trajectory_df['timestamp'].min()} 秒")
        
        # 测试不同的时间区间
        test_ranges = [
            [[0.0, 4.0], [10.0, 15.0]],    # 标准区间
            [[0.0, 5.0]],                   # 单个区间
            [[15.0, 19.0]],                 # 靠近结尾的区间
            [[25.0, 30.0]],                 # 超出范围的区间
            [[5.0, 2.0]],                   # 错误的时间顺序
            [],                             # 空区间
        ]
        
        for i, time_ranges in enumerate(test_ranges, 1):
            logger.info(f"\n测试分段 {i}: {time_ranges}")
            geometry, segment_count = segmenter.segment_trajectory_by_time_ranges(trajectory_df, time_ranges)
            
            logger.info(f"分段结果: {segment_count} 个分段")
            logger.info(f"几何状态: {'空' if geometry.is_empty else '非空'}")
            logger.info(f"几何类型: {geometry.geom_type if not geometry.is_empty else 'None'}")
            
            if not geometry.is_empty and hasattr(geometry, 'geoms'):
                logger.info(f"子几何数量: {len(geometry.geoms)}")
        
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def test_complete_record_processing():
    """测试完整记录处理"""
    logger.info("🔧 测试完整记录处理")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import (
            QualityCheckRecord, 
            QualityCheckTrajectoryQuery,
            QualityCheckConfig
        )
        
        # 创建测试记录
        test_record = QualityCheckRecord(
            task_name='测试任务',
            annotator='测试标注员',  
            autoscene_id='test_scene_123',
            result=['压线行驶'],
            description=[[0.0, 4.0], [10.0, 15.0]],
            other_scenario=['压斑马线']
        )
        
        logger.info("创建的测试记录:")
        logger.info(f"  task_name: '{test_record.task_name}'")
        logger.info(f"  annotator: '{test_record.annotator}'")
        logger.info(f"  autoscene_id: '{test_record.autoscene_id}'")
        logger.info(f"  result: {test_record.result}")
        logger.info(f"  description: {test_record.description}")
        logger.info(f"  other_scenario: {test_record.other_scenario}")
        
        # 测试字段编码
        for field_name, field_value in [
            ('task_name', test_record.task_name),
            ('annotator', test_record.annotator),
            ('result', test_record.result),
            ('other_scenario', test_record.other_scenario)
        ]:
            try:
                if isinstance(field_value, str):
                    encoded = field_value.encode('utf-8').decode('utf-8')
                    logger.info(f"  {field_name} 编码检查: ✅ 正常")
                elif isinstance(field_value, list):
                    for item in field_value:
                        if isinstance(item, str):
                            encoded = item.encode('utf-8').decode('utf-8')
                    logger.info(f"  {field_name} 编码检查: ✅ 正常")
            except Exception as e:
                logger.warning(f"  {field_name} 编码检查: ❌ 异常 - {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("🚀 开始调试轨迹分段功能")
    
    tests = [
        ("description字段解析", test_description_parsing),
        ("轨迹分段逻辑", test_trajectory_segmentation), 
        ("完整记录处理", test_complete_record_processing),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n{'='*60}")
        logger.info(f"🔧 {test_name}")
        logger.info(f"{'='*60}")
        
        result = test_func()
        results.append((test_name, result))
        
        if result:
            logger.info(f"✅ {test_name} 测试通过")
        else:
            logger.error(f"❌ {test_name} 测试失败")
    
    # 总结
    logger.info(f"\n{'='*60}")
    logger.info("📊 测试总结")
    logger.info(f"{'='*60}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总计: {passed}/{total} 个测试通过")
    
    if passed == total:
        logger.info("🎉 所有分段功能测试通过！")
        return 0
    else:
        logger.error("❌ 部分分段功能测试失败")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main()) 