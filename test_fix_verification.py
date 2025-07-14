#!/usr/bin/env python3
"""
验证修复效果的测试脚本
"""

import sys
import os
import subprocess
import logging
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_integrated_analysis():
    """测试集成轨迹分析"""
    logger = setup_logging()
    
    logger.info("开始测试修复效果...")
    
    # 检查sample_trajectories.geojson是否存在
    geojson_file = "sample_trajectories.geojson"
    if not os.path.exists(geojson_file):
        logger.error(f"测试文件不存在: {geojson_file}")
        return False
    
    try:
        # 运行集成分析
        cmd = [
            sys.executable, "-m", "spdatalab.fusion.integrated_trajectory_analysis",
            "--input", geojson_file,
            "--verbose"
        ]
        
        logger.info(f"执行命令: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        # 检查执行结果
        if result.returncode == 0:
            logger.info("✅ 集成分析执行成功")
            
            # 检查日志中是否还有ERROR
            if "ERROR" in result.stderr:
                logger.warning("⚠️ 仍有错误信息，需要进一步检查")
                print("错误信息:")
                print(result.stderr)
            else:
                logger.info("✅ 没有发现错误信息")
            
            # 检查是否有几何相关的错误
            if "find_srid" in result.stderr or "GEOMETRY_COLUMNS" in result.stderr:
                logger.error("❌ 几何列创建问题仍然存在")
                return False
            else:
                logger.info("✅ 几何列创建问题已修复")
            
            return True
        else:
            logger.error(f"❌ 集成分析执行失败，返回码: {result.returncode}")
            print("标准输出:")
            print(result.stdout)
            print("错误输出:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ 执行超时")
        return False
    except Exception as e:
        logger.error(f"❌ 执行异常: {e}")
        return False

def check_database_tables():
    """检查数据库中的表是否按预期创建"""
    logger = setup_logging()
    
    try:
        from sqlalchemy import create_engine, text
        
        # 连接数据库
        engine = create_engine("postgresql+psycopg://postgres:postgres@local_pg:5432/postgres")
        
        # 查询所有表
        query = text("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename LIKE '%integrated_%'
            ORDER BY tablename;
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            tables = [row[0] for row in result.fetchall()]
        
        logger.info(f"发现 {len(tables)} 个集成分析相关的表:")
        for table in tables:
            logger.info(f"  - {table}")
            
        # 检查表名格式
        expected_patterns = [
            "_road_analysis",
            "_road_lanes", 
            "_road_intersections",
            "_road_roads",
            "_lane_segments",
            "_lane_buffer",
            "_lane_quality"
        ]
        
        found_patterns = []
        for pattern in expected_patterns:
            for table in tables:
                if pattern in table:
                    found_patterns.append(pattern)
                    break
        
        if len(found_patterns) == len(expected_patterns):
            logger.info("✅ 所有预期的表类型都已创建")
            return True
        else:
            logger.warning(f"⚠️ 只找到 {len(found_patterns)}/{len(expected_patterns)} 种类型的表")
            return False
            
    except Exception as e:
        logger.error(f"❌ 检查数据库表失败: {e}")
        return False

def main():
    """主函数"""
    logger = setup_logging()
    
    logger.info("=" * 50)
    logger.info("开始验证修复效果")
    logger.info("=" * 50)
    
    # 测试1: 运行集成分析
    logger.info("\n1. 测试集成分析执行...")
    analysis_success = test_integrated_analysis()
    
    # 测试2: 检查数据库表
    logger.info("\n2. 检查数据库表创建...")
    tables_success = check_database_tables()
    
    # 总结
    logger.info("\n" + "=" * 50)
    logger.info("修复验证总结")
    logger.info("=" * 50)
    
    if analysis_success and tables_success:
        logger.info("🎉 所有测试通过，修复成功！")
        return 0
    else:
        logger.error("❌ 部分测试失败，需要进一步修复")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 