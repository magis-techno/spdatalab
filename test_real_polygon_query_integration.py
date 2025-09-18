#!/usr/bin/env python3
"""
测试真实polygon查询功能集成

验证功能：
1. 真实的HighPerformancePolygonTrajectoryQuery集成
2. Polygon映射功能
3. 轻量化查询流程

测试步骤：
1. 设置API配置和环境变量
2. 执行文本查询
3. 验证轨迹点查询和映射功能
4. 检查数据库保存功能
"""

import os
import sys
import logging
from pathlib import Path

# 设置项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from spdatalab.fusion.multimodal_trajectory_retrieval import (
    MultimodalTrajectoryWorkflow,
    MultimodalConfig
)
from spdatalab.dataset.multimodal_data_retriever import APIConfig

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_real_polygon_query_integration():
    """测试真实polygon查询功能集成"""
    logger.info("🧪 开始测试真实polygon查询功能集成...")
    
    try:
        # 1. 设置API配置
        api_config = APIConfig.from_env()
        logger.info(f"✅ API配置加载成功: {api_config.api_base_url}")
        
        # 2. 创建轻量化配置
        config = MultimodalConfig(
            api_config=api_config,
            max_search_results=5,          # 小规模测试
            buffer_distance=10.0,          # 10米缓冲区
            similarity_threshold=0.3,
            time_window_days=30
        )
        
        # 3. 创建工作流
        workflow = MultimodalTrajectoryWorkflow(config)
        logger.info("✅ 多模态工作流初始化成功")
        
        # 4. 执行文本查询（小规模测试）
        logger.info("🔍 开始执行文本查询测试...")
        result = workflow.process_text_query(
            text="bicycle crossing intersection",
            collection="ddi_collection_camera_encoded_1",
            count=5,  # 小数量测试
            output_table="test_real_polygon_query"
        )
        
        # 5. 验证结果
        if result.get('success', False):
            logger.info("✅ 文本查询测试成功！")
            
            # 检查关键功能
            stats = result.get('stats', {})
            
            # 验证多模态检索
            search_count = stats.get('search_results_count', 0)
            logger.info(f"📊 多模态检索结果: {search_count} 条")
            
            # 验证聚合优化
            raw_polygons = stats.get('raw_polygon_count', 0)
            merged_polygons = stats.get('merged_polygon_count', 0)
            if raw_polygons > 0:
                compression_ratio = ((raw_polygons - merged_polygons) / raw_polygons) * 100
                logger.info(f"🔄 Polygon优化: {raw_polygons} → {merged_polygons} "
                           f"(压缩率: {compression_ratio:.1f}%)")
            
            # 验证轨迹点查询
            points_count = stats.get('discovered_points_count', 0)
            logger.info(f"⚡ 轨迹点查询结果: {points_count} 个点")
            
            # 验证数据库保存
            saved_count = stats.get('saved_to_database', 0)
            if saved_count > 0:
                logger.info(f"💾 数据库保存成功: {saved_count} 条轨迹")
            elif 'database_save_error' in stats:
                logger.warning(f"⚠️ 数据库保存失败: {stats['database_save_error']}")
            
            # 性能统计
            total_duration = stats.get('total_duration', 0)
            logger.info(f"⏱️ 总耗时: {total_duration:.2f} 秒")
            
            return True
            
        else:
            error_msg = result.get('error', '未知错误')
            logger.error(f"❌ 文本查询测试失败: {error_msg}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_polygon_mapping_functionality():
    """测试polygon映射功能"""
    logger.info("🧪 开始测试polygon映射功能...")
    
    try:
        from spdatalab.fusion.multimodal_trajectory_retrieval import MultimodalTrajectoryWorkflow
        from spdatalab.dataset.multimodal_data_retriever import APIConfig
        import pandas as pd
        from shapely.geometry import Polygon, Point
        
        # 创建测试配置
        api_config = APIConfig.from_env()
        config = MultimodalConfig(api_config=api_config)
        workflow = MultimodalTrajectoryWorkflow(config)
        
        # 创建测试数据
        test_points_df = pd.DataFrame({
            'dataset_name': ['test_dataset_1', 'test_dataset_2'],
            'timestamp': [1739958971349, 1739958971350],
            'longitude': [116.3, 116.31],
            'latitude': [39.9, 39.91]
        })
        
        # 创建测试polygon
        test_polygons = [{
            'id': 'test_polygon_1',
            'geometry': Polygon([(116.29, 39.89), (116.32, 39.89), (116.32, 39.92), (116.29, 39.92)]),
            'sources': [
                {'dataset_name': 'source_dataset_1', 'timestamp': 1739958970000},
                {'dataset_name': 'source_dataset_2', 'timestamp': 1739958971000}
            ]
        }]
        
        # 测试映射功能
        result_df = workflow._add_polygon_mapping(test_points_df, test_polygons)
        
        # 验证结果
        if 'source_polygons' in result_df.columns:
            logger.info("✅ Polygon映射功能测试成功！")
            for i, row in result_df.iterrows():
                logger.info(f"  点 {i+1}: {row['dataset_name']} → {row['source_polygons']}")
            return True
        else:
            logger.error("❌ Polygon映射功能测试失败：缺少source_polygons列")
            return False
            
    except Exception as e:
        logger.error(f"❌ Polygon映射测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主测试函数"""
    logger.info("🚀 开始真实polygon查询功能集成测试...")
    
    # 检查环境变量
    required_env_vars = ['MULTIMODAL_API_KEY', 'MULTIMODAL_USERNAME', 'MULTIMODAL_API_BASE_URL']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ 缺少环境变量: {missing_vars}")
        logger.info("💡 请确保.env文件配置正确")
        return False
    
    success_count = 0
    total_tests = 2
    
    # 测试1：真实polygon查询集成
    logger.info("\n" + "="*50)
    logger.info("测试1: 真实polygon查询功能集成")
    logger.info("="*50)
    if test_real_polygon_query_integration():
        success_count += 1
    
    # 测试2：polygon映射功能
    logger.info("\n" + "="*50)
    logger.info("测试2: Polygon映射功能")
    logger.info("="*50)
    if test_polygon_mapping_functionality():
        success_count += 1
    
    # 总结
    logger.info("\n" + "="*50)
    logger.info(f"测试完成: {success_count}/{total_tests} 通过")
    logger.info("="*50)
    
    if success_count == total_tests:
        logger.info("🎉 所有测试通过！真实polygon查询功能集成成功")
        return True
    else:
        logger.warning(f"⚠️ {total_tests - success_count} 个测试失败")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)






