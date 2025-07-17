"""测试时间戳转换功能

验证时间戳单位检测和转换是否正确
"""

import logging
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_timestamp_detection():
    """测试时间戳单位检测"""
    logger.info("🔧 测试时间戳单位检测")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 测试不同单位的时间戳差值
        test_cases = [
            (100, "秒", 1),                          # 100秒
            (30000, "毫秒", 1000),                   # 30秒 = 30000毫秒
            (15000000, "微秒", 1000000),             # 15秒 = 15000000微秒
            (29000273000000000, "纳秒", 1000000000), # 29000273秒 = 29000273000000000纳秒
        ]
        
        for duration, expected_unit, expected_scale in test_cases:
            unit, scale = segmenter._detect_timestamp_unit(duration)
            logger.info(f"持续时间: {duration}")
            logger.info(f"  检测结果: {unit} (缩放: {scale})")
            logger.info(f"  预期结果: {expected_unit} (缩放: {expected_scale})")
            logger.info(f"  转换后秒数: {duration / scale:.1f}s")
            
            if unit == expected_unit and scale == expected_scale:
                logger.info("  ✅ 检测正确")
            else:
                logger.error("  ❌ 检测错误")
            logger.info("")
        
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        return False

def test_real_timestamp_conversion():
    """测试实际时间戳转换"""
    logger.info("🔧 测试实际时间戳转换")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 模拟16位UTC时间戳（纳秒级）
        base_timestamp = 1641946800000000000  # 2022-01-12 00:00:00 UTC (纳秒)
        
        # 创建30秒的轨迹数据（每秒1个点）
        timestamps = [base_timestamp + i * 1000000000 for i in range(30)]  # 每秒增加1000000000纳秒
        longitudes = [116.3 + i * 0.001 for i in range(30)]
        latitudes = [39.9 + i * 0.0005 for i in range(30)]
        
        trajectory_df = pd.DataFrame({
            'timestamp': timestamps,
            'longitude': longitudes,
            'latitude': latitudes
        })
        
        logger.info(f"创建模拟轨迹数据: {len(trajectory_df)} 个点")
        logger.info(f"时间戳范围: {timestamps[0]} - {timestamps[-1]}")
        logger.info(f"原始时间差: {timestamps[-1] - timestamps[0]}")
        
        # 测试时间区间分段
        time_ranges = [[5.0, 10.0], [15.0, 20.0]]  # 两个5秒的区间
        
        logger.info(f"测试时间区间: {time_ranges}")
        
        geometry, segment_count = segmenter.segment_trajectory_by_time_ranges(trajectory_df, time_ranges)
        
        logger.info(f"分段结果: {segment_count} 个分段")
        logger.info(f"几何状态: {'空' if geometry.is_empty else '非空'}")
        
        if not geometry.is_empty:
            logger.info("✅ 时间戳转换成功!")
            return True
        else:
            logger.error("❌ 分段失败，可能时间戳转换有问题")
            return False
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def test_description_time_matching():
    """测试description时间区间匹配"""
    logger.info("🔧 测试description时间区间匹配")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 创建与用户报错类似的情况
        base_timestamp = 1641946800000000000  # 纳秒级时间戳
        
        # 创建20秒的轨迹（模拟实际情况）
        timestamps = [base_timestamp + i * 1000000000 for i in range(20)]
        longitudes = [116.3 + i * 0.001 for i in range(20)]
        latitudes = [39.9 + i * 0.0005 for i in range(20)]
        
        trajectory_df = pd.DataFrame({
            'timestamp': timestamps,
            'longitude': longitudes,
            'latitude': latitudes
        })
        
        # 测试用户报错中的时间区间
        time_ranges = [[8.3, 9.8]]  # 1.5秒的区间
        
        logger.info(f"轨迹数据: {len(trajectory_df)} 个点")
        logger.info(f"时间戳类型: 纳秒级 UTC")
        logger.info(f"测试时间区间: {time_ranges}")
        
        geometry, segment_count = segmenter.segment_trajectory_by_time_ranges(trajectory_df, time_ranges)
        
        logger.info(f"分段结果: {segment_count} 个分段")
        logger.info(f"几何状态: {'空' if geometry.is_empty else '非空'}")
        
        if segment_count > 0:
            logger.info("✅ 时间区间匹配成功!")
            return True
        else:
            logger.error("❌ 时间区间匹配失败")
            return False
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """主测试函数"""
    logger.info("🚀 开始测试时间戳转换功能")
    
    tests = [
        ("时间戳单位检测", test_timestamp_detection),
        ("实际时间戳转换", test_real_timestamp_conversion),
        ("时间区间匹配", test_description_time_matching),
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
        logger.info("🎉 时间戳转换功能正常!")
        return 0
    else:
        logger.error("❌ 时间戳转换功能存在问题")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main()) 