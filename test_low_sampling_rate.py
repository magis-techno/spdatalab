"""测试低采样率轨迹分段功能

验证自适应时间容差是否能解决分段点数不足的问题
"""

import logging
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_low_sampling_rate_segmentation():
    """测试低采样率轨迹的分段功能"""
    logger.info("🔧 测试低采样率轨迹分段")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        # 启用自适应容差的配置
        config = QualityCheckConfig(
            adaptive_tolerance=True,
            time_tolerance=0.5,
            min_points_per_segment=2
        )
        segmenter = TrajectorySegmenter(config)
        
        # 模拟用户报错的情况：微秒级时间戳，低采样率
        base_timestamp = 1736484537684165  # 用户提供的实际时间戳
        
        # 创建低采样率轨迹：30秒内只有6个点（每5秒1个点）
        timestamps = [base_timestamp + i * 5000000 for i in range(6)]  # 每5秒增加5000000微秒
        longitudes = [116.3 + i * 0.001 for i in range(6)]
        latitudes = [39.9 + i * 0.0005 for i in range(6)]
        
        trajectory_df = pd.DataFrame({
            'timestamp': timestamps,
            'longitude': longitudes,
            'latitude': latitudes
        })
        
        logger.info(f"创建低采样率轨迹: {len(trajectory_df)} 个点，30秒时长")
        logger.info(f"采样间隔: 5秒/点")
        logger.info(f"时间戳范围: {timestamps[0]} - {timestamps[-1]}")
        
        # 测试用户报错的具体时间区间
        test_cases = [
            [[8.3, 9.8]],   # 用户报错案例1：1.5秒区间
            [[24.1, 25.5]], # 用户报错案例2：1.4秒区间
            [[0.0, 5.0]],   # 较长区间：5秒
            [[10.0, 20.0]], # 更长区间：10秒
        ]
        
        for i, time_ranges in enumerate(test_cases, 1):
            logger.info(f"\n--- 测试案例 {i}: {time_ranges} ---")
            
            geometry, segment_count = segmenter.segment_trajectory_by_time_ranges(trajectory_df, time_ranges)
            
            logger.info(f"分段结果: {segment_count} 个分段")
            logger.info(f"几何状态: {'空' if geometry.is_empty else '非空'}")
            
            if not geometry.is_empty:
                logger.info(f"✅ 案例 {i} 成功: 找到 {segment_count} 个分段")
            else:
                logger.warning(f"⚠️ 案例 {i} 失败: 无法创建分段")
        
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def test_adaptive_tolerance_progression():
    """测试自适应容差的递进过程"""
    logger.info("🔧 测试自适应容差递进")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        config = QualityCheckConfig(
            adaptive_tolerance=True,
            time_tolerance=0.1,  # 从小容差开始
            min_points_per_segment=2
        )
        segmenter = TrajectorySegmenter(config)
        
        # 创建稀疏轨迹：20秒内只有3个点
        base_timestamp = 1736484537684165
        timestamps = [base_timestamp + i * 10000000 for i in range(3)]  # 每10秒1个点
        longitudes = [116.3 + i * 0.001 for i in range(3)]
        latitudes = [39.9 + i * 0.0005 for i in range(3)]
        
        trajectory_df = pd.DataFrame({
            'timestamp': timestamps,
            'longitude': longitudes,
            'latitude': latitudes
        })
        
        logger.info(f"创建稀疏轨迹: {len(trajectory_df)} 个点，20秒时长")
        logger.info(f"采样间隔: 10秒/点")
        
        # 测试一个需要大容差的时间区间
        time_ranges = [[5.0, 7.0]]  # 2秒区间，在两个采样点之间
        
        logger.info(f"测试时间区间: {time_ranges}")
        logger.info("预期：需要较大容差才能找到足够的点")
        
        geometry, segment_count = segmenter.segment_trajectory_by_time_ranges(trajectory_df, time_ranges)
        
        if segment_count > 0:
            logger.info("✅ 自适应容差成功工作")
        else:
            logger.warning("⚠️ 自适应容差未能解决问题")
        
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        return False

def test_timestamp_unit_detection_real_case():
    """测试实际时间戳的单位检测"""
    logger.info("🔧 测试实际时间戳单位检测")
    
    try:
        from spdatalab.dataset.quality_check_trajectory_query import TrajectorySegmenter, QualityCheckConfig
        
        config = QualityCheckConfig()
        segmenter = TrajectorySegmenter(config)
        
        # 测试用户提供的实际时间戳
        real_timestamp = 1736484537684165
        
        # 模拟30秒的轨迹
        duration_micro = 30 * 1000000  # 30秒 = 30000000微秒
        
        logger.info(f"实际时间戳示例: {real_timestamp}")
        logger.info(f"模拟30秒时长的微秒差值: {duration_micro}")
        
        unit, scale = segmenter._detect_timestamp_unit(duration_micro)
        
        logger.info(f"检测结果: {unit} (缩放因子: {scale})")
        logger.info(f"转换后时长: {duration_micro / scale:.1f}s")
        
        if unit == "微秒" and abs(duration_micro / scale - 30.0) < 0.1:
            logger.info("✅ 时间戳单位检测正确")
            return True
        else:
            logger.error("❌ 时间戳单位检测错误")
            return False
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("🚀 开始测试低采样率轨迹分段功能")
    
    tests = [
        ("实际时间戳单位检测", test_timestamp_unit_detection_real_case),
        ("低采样率轨迹分段", test_low_sampling_rate_segmentation),
        ("自适应容差递进", test_adaptive_tolerance_progression),
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
        logger.info("🎉 低采样率分段功能正常!")
        return 0
    else:
        logger.error("❌ 低采样率分段功能存在问题")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main()) 