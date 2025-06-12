#!/usr/bin/env python3
"""Mock环境初始化脚本"""

import os
import sys
import time
import logging
import argparse
from pathlib import Path

# 添加项目路径到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from data_generators.config import get_config
from data_generators.trajectory_generator import TrajectoryGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_databases(timeout_seconds: int = 120):
    """等待数据库服务启动"""
    from sqlalchemy import create_engine, text
    
    db_config, _ = get_config()
    databases = {
        'trajectory': db_config.trajectory_dsn,
        'business': db_config.business_dsn,
        'map': db_config.map_dsn
    }
    
    logger.info("等待数据库服务启动...")
    start_time = time.time()
    
    for db_name, dsn in databases.items():
        logger.info(f"检查 {db_name} 数据库连接...")
        
        while time.time() - start_time < timeout_seconds:
            try:
                engine = create_engine(dsn)
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1"))
                    if result.fetchone():
                        logger.info(f"✅ {db_name} 数据库连接成功")
                        break
            except Exception as e:
                logger.debug(f"连接 {db_name} 失败: {e}")
                time.sleep(2)
        else:
            logger.error(f"❌ {db_name} 数据库连接超时")
            return False
    
    logger.info("🎉 所有数据库服务已就绪")
    return True

def generate_test_data(scale: str = 'small'):
    """生成测试数据"""
    logger.info(f"开始生成 {scale} 规模的测试数据...")
    
    try:
        # 生成轨迹数据
        logger.info("生成轨迹数据...")
        generator = TrajectoryGenerator()
        trajectory_count = generator.generate_trajectory_data(scale)
        logger.info(f"✅ 轨迹数据生成完成: {trajectory_count} 个点")
        
        # 生成业务数据（后续添加）
        logger.info("生成业务数据...")
        # business_count = generate_business_data(scale)
        
        # 生成地图数据（后续添加）
        logger.info("生成地图数据...")
        # map_count = generate_map_data(scale)
        
        logger.info("🎉 测试数据生成完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试数据生成失败: {e}")
        return False

def validate_mock_environment():
    """验证Mock环境"""
    logger.info("验证Mock环境...")
    
    try:
        # 检查数据库连接
        if not wait_for_databases(timeout_seconds=30):
            return False
        
        # 检查数据完整性
        logger.info("检查数据完整性...")
        generator = TrajectoryGenerator()
        stats = generator.get_trajectory_stats()
        
        if stats['total_points'] == 0:
            logger.warning("⚠️  轨迹数据为空，建议运行 generate_test_data")
        else:
            logger.info(f"✅ 轨迹数据: {stats['total_points']} 个点, {stats['total_scenes']} 个场景")
        
        logger.info("🎉 Mock环境验证完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ Mock环境验证失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Mock环境管理工具')
    parser.add_argument('action', choices=['wait', 'generate', 'validate', 'setup'], 
                       help='执行的操作')
    parser.add_argument('--scale', choices=['small', 'medium', 'large'], default='small',
                       help='数据规模 (默认: small)')
    parser.add_argument('--timeout', type=int, default=120,
                       help='数据库等待超时时间 (默认: 120秒)')
    
    args = parser.parse_args()
    
    if args.action == 'wait':
        success = wait_for_databases(args.timeout)
    elif args.action == 'generate':
        success = generate_test_data(args.scale)
    elif args.action == 'validate':
        success = validate_mock_environment()
    elif args.action == 'setup':
        # 完整设置流程
        logger.info("🚀 开始Mock环境完整设置...")
        success = (
            wait_for_databases(args.timeout) and
            generate_test_data(args.scale) and
            validate_mock_environment()
        )
        
        if success:
            logger.info("🎉 Mock环境设置成功！")
        else:
            logger.error("❌ Mock环境设置失败")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 