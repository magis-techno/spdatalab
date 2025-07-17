"""质检轨迹查询示例

演示如何使用quality_check_trajectory_query模块处理Excel质检结果：
1. 加载Excel文件并解析质检记录
2. 查询对应的轨迹数据并进行时间分段
3. 合并result和other_scenario字段
4. 保存到数据库和导出GeoJSON文件

使用场景：
- 基于质检结果Excel文件查找对应轨迹
- 根据时间区间对轨迹进行分段处理
- 统一MultiLineString格式输出
"""

import logging
import sys
from pathlib import Path

from spdatalab.dataset.quality_check_trajectory_query import (
    process_quality_check_excel,
    QualityCheckConfig,
    QualityCheckTrajectoryQuery,
    ExcelDataParser,
    ResultFieldProcessor
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def example_basic_usage():
    """基础用法示例：处理Excel文件并保存结果"""
    
    excel_file = "data/quality_check_sample.xlsx"  # 替换为实际文件路径
    output_table = "quality_check_trajectories_demo"
    output_geojson = "output/quality_trajectories_demo.geojson"
    
    logger.info("=" * 50)
    logger.info("🔥 质检轨迹查询基础用法示例")
    logger.info("=" * 50)
    
    try:
        # 使用默认配置处理Excel文件
        stats = process_quality_check_excel(
            excel_file=excel_file,
            output_table=output_table,
            output_geojson=output_geojson
        )
        
        # 输出处理结果
        if stats.get('success'):
            logger.info("✅ 处理成功完成！")
            logger.info(f"📊 处理统计: {stats['valid_trajectories']}/{stats['total_records']} 条记录成功")
            logger.info(f"⏱️ 总耗时: {stats['workflow_duration']:.2f}s")
        else:
            logger.error(f"❌ 处理失败: {stats.get('error', '未知错误')}")
            
    except Exception as e:
        logger.error(f"❌ 示例执行失败: {str(e)}")

def example_custom_config():
    """自定义配置示例：调整处理参数"""
    
    excel_file = "data/quality_check_large.xlsx"  # 替换为实际文件路径
    output_table = "quality_check_trajectories_custom"
    
    logger.info("=" * 50)
    logger.info("🔧 质检轨迹查询自定义配置示例")
    logger.info("=" * 50)
    
    try:
        # 自定义配置
        config = QualityCheckConfig(
            excel_batch_size=500,           # 较小的批量处理大小
            min_points_per_segment=3,       # 更严格的最小点数要求
            time_tolerance=0.2,             # 更宽松的时间容差
            simplify_geometry=True,         # 启用几何简化
            simplify_tolerance=0.00005,     # 几何简化容差
            batch_insert_size=500,          # 较小的插入批次
            cache_scene_mappings=True       # 启用场景映射缓存
        )
        
        logger.info("🔧 使用自定义配置:")
        logger.info(f"   • 最小分段点数: {config.min_points_per_segment}")
        logger.info(f"   • 时间容差: {config.time_tolerance}s")
        logger.info(f"   • 几何简化: {'启用' if config.simplify_geometry else '禁用'}")
        logger.info(f"   • 场景映射缓存: {'启用' if config.cache_scene_mappings else '禁用'}")
        
        # 执行处理
        stats = process_quality_check_excel(
            excel_file=excel_file,
            output_table=output_table,
            config=config
        )
        
        # 输出结果
        if stats.get('success'):
            logger.info("✅ 自定义配置处理成功！")
            logger.info(f"📊 有效轨迹: {stats['valid_trajectories']} 条")
            logger.info(f"📊 失败记录: {stats['failed_records']} 条")
        else:
            logger.error(f"❌ 自定义配置处理失败: {stats.get('error')}")
            
    except Exception as e:
        logger.error(f"❌ 自定义配置示例失败: {str(e)}")

def example_step_by_step():
    """分步处理示例：手动控制处理流程"""
    
    excel_file = "data/quality_check_sample.xlsx"  # 替换为实际文件路径
    
    logger.info("=" * 50)
    logger.info("🔍 质检轨迹查询分步处理示例")
    logger.info("=" * 50)
    
    try:
        # 创建配置和查询器
        config = QualityCheckConfig()
        query_processor = QualityCheckTrajectoryQuery(config)
        
        # 步骤1: 解析Excel文件
        logger.info("📖 步骤1: 解析Excel文件...")
        records = query_processor.excel_parser.load_excel_data(excel_file)
        logger.info(f"✅ 解析完成: {len(records)} 条记录")
        
        # 步骤2: 演示结果字段处理
        logger.info("🔧 步骤2: 演示结果字段处理...")
        if records:
            sample_record = records[0]
            merged_results = query_processor.result_processor.merge_and_clean_results(
                sample_record.result, sample_record.other_scenario
            )
            logger.info(f"   原始result: {sample_record.result}")
            logger.info(f"   原始other_scenario: {sample_record.other_scenario}")
            logger.info(f"   合并后结果: {merged_results}")
        
        # 步骤3: 批量查询场景映射
        logger.info("🔍 步骤3: 查询场景映射...")
        autoscene_ids = [record.autoscene_id for record in records[:5]]  # 只处理前5条
        scene_mappings = query_processor.scene_mapper.batch_query_scene_mappings(autoscene_ids)
        logger.info(f"✅ 场景映射查询完成: {len(scene_mappings)} 个映射")
        
        # 步骤4: 演示轨迹分段
        logger.info("🔧 步骤4: 演示轨迹分段...")
        for record in records[:2]:  # 只处理前2条记录
            scene_info = scene_mappings.get(record.autoscene_id)
            if scene_info and scene_info.get('dataset_name'):
                dataset_name = scene_info['dataset_name']
                logger.info(f"   处理轨迹: {record.autoscene_id} -> {dataset_name}")
                
                # 查询完整轨迹
                trajectory_df = query_processor.trajectory_segmenter.query_complete_trajectory(dataset_name)
                if not trajectory_df.empty:
                    logger.info(f"   查询到轨迹: {len(trajectory_df)} 个点")
                    
                    # 分段处理
                    if record.description:
                        logger.info(f"   时间区间: {record.description}")
                        geometry, segment_count = query_processor.trajectory_segmenter.segment_trajectory_by_time_ranges(
                            trajectory_df, record.description
                        )
                        logger.info(f"   分段结果: {segment_count} 个分段")
                    else:
                        logger.info("   无时间区间，使用完整轨迹")
                        geometry, _ = query_processor.trajectory_segmenter.create_complete_trajectory(trajectory_df)
                        
                    logger.info(f"   几何类型: {geometry.geom_type}")
                else:
                    logger.warning(f"   未查询到轨迹数据: {dataset_name}")
        
        logger.info("✅ 分步处理演示完成")
        
    except Exception as e:
        logger.error(f"❌ 分步处理示例失败: {str(e)}")

def example_result_field_processing():
    """结果字段处理演示"""
    
    logger.info("=" * 50)
    logger.info("🔧 结果字段处理演示")
    logger.info("=" * 50)
    
    processor = ResultFieldProcessor()
    
    # 测试用例
    test_cases = [
        {
            'result': "压线行驶",
            'other_scenario': "压斑马线",
            'expected': ['压斑马线', '压线行驶']
        },
        {
            'result': ['压线行驶', '压线行驶'],
            'other_scenario': ['压斑马线'],
            'expected': ['压斑马线', '压线行驶']
        },
        {
            'result': "['超速行驶', '急刹车']",
            'other_scenario': "",
            'expected': ['急刹车', '超速行驶']
        },
        {
            'result': [],
            'other_scenario': "违规变道",
            'expected': ['违规变道']
        }
    ]
    
    for i, case in enumerate(test_cases, 1):
        logger.info(f"🧪 测试用例 {i}:")
        logger.info(f"   输入result: {case['result']}")
        logger.info(f"   输入other_scenario: {case['other_scenario']}")
        
        result = processor.merge_and_clean_results(case['result'], case['other_scenario'])
        
        logger.info(f"   输出结果: {result}")
        logger.info(f"   期望结果: {case['expected']}")
        logger.info(f"   ✅ {'通过' if result == case['expected'] else '❌ 失败'}")
        logger.info("")

def main():
    """主函数：运行所有示例"""
    logger.info("🚀 开始运行质检轨迹查询示例")
    
    try:
        # 运行结果字段处理演示
        example_result_field_processing()
        
        # 注意：以下示例需要实际的Excel文件才能运行
        logger.info("⚠️  以下示例需要实际的Excel文件，请根据需要调整文件路径")
        
        # 如果有实际文件，可以取消注释以下代码
        # example_basic_usage()
        # example_custom_config()
        # example_step_by_step()
        
        logger.info("🎉 示例演示完成")
        
    except Exception as e:
        logger.error(f"❌ 示例运行失败: {str(e)}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main()) 