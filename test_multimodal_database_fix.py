#!/usr/bin/env python3
"""
多模态轨迹检索数据库修复验证测试脚本

此脚本用于全面测试数据库配置修复后的多模态轨迹检索功能：
1. 验证数据库连接是否正常
2. 测试轨迹表创建功能
3. 执行完整的多模态轨迹检索流程
4. 验证结果保存到数据库

使用方法:
python test_multimodal_database_fix.py [--quick] [--cleanup]

参数:
--quick: 快速测试模式，跳过耗时的检索过程
--cleanup: 测试后清理创建的测试表
"""

import argparse
import sys
import logging
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import psycopg
from sqlalchemy import create_engine, text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultimodalDatabaseTester:
    """多模态数据库功能测试器"""
    
    def __init__(self, quick_mode: bool = False, cleanup: bool = False):
        self.quick_mode = quick_mode
        self.cleanup = cleanup
        self.test_table = f"test_discovered_trajectories_{int(time.time())}"
        
        # 测试配置
        self.db_configs = [
            "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
            "postgresql+psycopg://postgres:postgres@127.0.0.1:5432/postgres"
        ]
        
        self.working_engine = None
        
        self.test_results = {
            'database_connection': False,
            'table_creation': False,
            'multimodal_execution': False,
            'data_saved': False,
            'execution_time': 0,
            'error_messages': [],
            'recommendations': []
        }
    
    def run_comprehensive_test(self) -> Dict[str, Any]:
        """运行综合测试"""
        logger.info("🧪 开始多模态轨迹检索数据库修复验证")
        logger.info(f"   测试模式: {'快速' if self.quick_mode else '完整'}")
        logger.info(f"   测试表名: {self.test_table}")
        
        start_time = time.time()
        
        try:
            # 步骤1: 测试数据库连接
            if not self.test_database_connection():
                return self.test_results
            
            # 步骤2: 测试表创建功能
            if not self.test_table_creation():
                return self.test_results
            
            # 步骤3: 测试多模态轨迹检索
            if not self.test_multimodal_execution():
                return self.test_results
            
            # 步骤4: 验证数据保存
            if not self.test_data_verification():
                return self.test_results
            
            self.test_results['execution_time'] = time.time() - start_time
            logger.info(f"✅ 所有测试通过！总用时: {self.test_results['execution_time']:.2f}秒")
            
        except Exception as e:
            logger.error(f"❌ 测试过程中发生错误: {e}")
            self.test_results['error_messages'].append(str(e))
        
        finally:
            # 清理测试数据
            if self.cleanup:
                self.cleanup_test_data()
        
        # 生成测试报告
        self.generate_test_report()
        
        return self.test_results
    
    def test_database_connection(self) -> bool:
        """测试数据库连接"""
        logger.info("🔌 测试数据库连接...")
        
        for dsn in self.db_configs:
            try:
                logger.info(f"   尝试连接: {dsn}")
                engine = create_engine(dsn, future=True)
                
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT version()"))
                    version = result.scalar()
                
                self.working_engine = engine
                self.test_results['database_connection'] = True
                logger.info(f"   ✅ 数据库连接成功")
                logger.info(f"   PostgreSQL版本: {version[:100]}...")
                return True
                
            except Exception as e:
                logger.warning(f"   ❌ 连接失败: {e}")
                continue
        
        logger.error("❌ 所有数据库连接均失败")
        self.test_results['error_messages'].append("数据库连接失败")
        return False
    
    def test_table_creation(self) -> bool:
        """测试轨迹表创建功能"""
        logger.info("🏗️ 测试轨迹表创建功能...")
        
        if not self.working_engine:
            logger.error("没有可用的数据库连接")
            return False
        
        try:
            # 模拟轨迹表创建SQL（来自polygon_trajectory_query.py）
            create_sql = text(f"""
                CREATE TABLE IF NOT EXISTS {self.test_table} (
                    id serial PRIMARY KEY,
                    scene_id text NOT NULL,
                    dataset_name text NOT NULL,
                    total_points integer DEFAULT 0,
                    total_duration numeric(10,2),
                    start_time bigint,
                    end_time bigint,
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 添加几何列的SQL
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{self.test_table}', 'geometry', 4326, 'LINESTRING', 2);
            """)
            
            # 执行创建
            with self.working_engine.connect() as conn:
                conn.execute(create_sql)
                conn.commit()
                logger.info("   ✅ 基础表创建成功")
                
                # 尝试添加几何列（可能失败，但不影响主要功能）
                try:
                    conn.execute(add_geom_sql)
                    conn.commit()
                    logger.info("   ✅ 几何列添加成功")
                except Exception as e:
                    logger.warning(f"   ⚠️ 几何列添加失败（不影响主要功能）: {e}")
            
            self.test_results['table_creation'] = True
            logger.info(f"   ✅ 测试表创建成功: {self.test_table}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 表创建失败: {e}")
            self.test_results['error_messages'].append(f"表创建失败: {e}")
            return False
    
    def test_multimodal_execution(self) -> bool:
        """测试多模态轨迹检索执行"""
        logger.info("🚀 测试多模态轨迹检索执行...")
        
        # 构建测试命令
        cmd_args = [
            sys.executable, "-m", "spdatalab.fusion.multimodal_trajectory_retrieval",
            "--text", "bicycle crossing intersection",
            "--collection", "ddi_collection_camera_encoded_1",
            "--output-table", self.test_table,
            "--verbose"
        ]
        
        if self.quick_mode:
            # 快速模式：添加限制参数（如果支持的话）
            logger.info("   使用快速测试模式")
        
        try:
            logger.info("   执行命令: " + " ".join(cmd_args))
            
            # 执行命令
            result = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                timeout=600 if not self.quick_mode else 180  # 快速模式3分钟，完整模式10分钟
            )
            
            # 分析执行结果
            if result.returncode == 0:
                logger.info("   ✅ 多模态轨迹检索执行成功")
                self.test_results['multimodal_execution'] = True
                
                # 分析输出信息
                if "数据库保存成功" in result.stdout:
                    logger.info("   ✅ 发现数据库保存成功的日志")
                elif "❌ 数据库保存失败" in result.stdout:
                    logger.warning("   ⚠️ 检测到数据库保存失败")
                    self.test_results['error_messages'].append("多模态执行中数据库保存失败")
                
                # 保存输出日志
                self.save_execution_log(result.stdout, result.stderr)
                return True
            else:
                logger.error(f"   ❌ 多模态轨迹检索执行失败，返回码: {result.returncode}")
                if result.stderr:
                    logger.error(f"   错误信息: {result.stderr}")
                    self.test_results['error_messages'].append(f"执行失败: {result.stderr}")
                
                self.save_execution_log(result.stdout, result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("   ❌ 多模态轨迹检索执行超时")
            self.test_results['error_messages'].append("执行超时")
            return False
        except Exception as e:
            logger.error(f"   ❌ 多模态轨迹检索执行异常: {e}")
            self.test_results['error_messages'].append(f"执行异常: {e}")
            return False
    
    def test_data_verification(self) -> bool:
        """验证数据是否正确保存到数据库"""
        logger.info("🔍 验证数据库数据保存...")
        
        if not self.working_engine:
            logger.error("没有可用的数据库连接")
            return False
        
        try:
            # 检查表是否存在并有数据
            check_sql = text(f"""
                SELECT COUNT(*) as record_count
                FROM {self.test_table}
            """)
            
            with self.working_engine.connect() as conn:
                result = conn.execute(check_sql)
                count = result.scalar()
            
            if count > 0:
                logger.info(f"   ✅ 数据验证成功: 发现 {count} 条轨迹记录")
                self.test_results['data_saved'] = True
                
                # 获取一些样例数据
                sample_sql = text(f"""
                    SELECT scene_id, dataset_name, total_points, created_at
                    FROM {self.test_table}
                    LIMIT 3
                """)
                
                with self.working_engine.connect() as conn:
                    sample_result = conn.execute(sample_sql)
                    samples = sample_result.fetchall()
                
                logger.info("   样例记录:")
                for sample in samples:
                    logger.info(f"     {dict(sample)}")
                
                return True
            else:
                logger.warning("   ⚠️ 表已创建但无数据记录")
                self.test_results['error_messages'].append("轨迹表无数据")
                return False
                
        except Exception as e:
            logger.error(f"   ❌ 数据验证失败: {e}")
            self.test_results['error_messages'].append(f"数据验证失败: {e}")
            return False
    
    def save_execution_log(self, stdout: str, stderr: str):
        """保存执行日志"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存标准输出
        if stdout:
            log_file = f"multimodal_test_output_{timestamp}.log"
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(stdout)
            logger.info(f"   📝 执行日志已保存: {log_file}")
        
        # 保存错误输出
        if stderr:
            error_file = f"multimodal_test_error_{timestamp}.log"
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(stderr)
            logger.warning(f"   📝 错误日志已保存: {error_file}")
    
    def cleanup_test_data(self):
        """清理测试数据"""
        logger.info("🧹 清理测试数据...")
        
        if not self.working_engine:
            return
        
        try:
            drop_sql = text(f"DROP TABLE IF EXISTS {self.test_table}")
            
            with self.working_engine.connect() as conn:
                conn.execute(drop_sql)
                conn.commit()
            
            logger.info(f"   ✅ 测试表已清理: {self.test_table}")
            
        except Exception as e:
            logger.warning(f"   ⚠️ 清理失败: {e}")
    
    def generate_test_report(self):
        """生成测试报告"""
        logger.info("\n" + "="*60)
        logger.info("📋 多模态轨迹检索数据库修复验证报告")
        logger.info("="*60)
        
        # 测试结果概述
        logger.info(f"\n🧪 测试结果概述:")
        logger.info(f"   数据库连接: {'✅' if self.test_results['database_connection'] else '❌'}")
        logger.info(f"   表创建功能: {'✅' if self.test_results['table_creation'] else '❌'}")
        logger.info(f"   多模态执行: {'✅' if self.test_results['multimodal_execution'] else '❌'}")
        logger.info(f"   数据保存验证: {'✅' if self.test_results['data_saved'] else '❌'}")
        
        if self.test_results['execution_time'] > 0:
            logger.info(f"   总执行时间: {self.test_results['execution_time']:.2f}秒")
        
        # 错误信息
        if self.test_results['error_messages']:
            logger.warning(f"\n❌ 错误信息:")
            for error in self.test_results['error_messages']:
                logger.warning(f"   {error}")
        
        # 最终结论
        all_passed = all([
            self.test_results['database_connection'],
            self.test_results['table_creation'],
            self.test_results['multimodal_execution'],
            self.test_results['data_saved']
        ])
        
        logger.info(f"\n🎯 最终结论:")
        if all_passed:
            logger.info("   ✅ 数据库配置修复成功！多模态轨迹检索功能正常")
            logger.info("   ✅ 可以正常使用原始命令进行轨迹检索")
        else:
            logger.warning("   ⚠️ 部分功能存在问题，需要进一步排查")
            logger.info("   💡 建议:")
            logger.info("     1. 检查PostgreSQL服务是否正常运行")
            logger.info("     2. 验证数据库连接配置是否正确")
            logger.info("     3. 确认相关Python依赖包是否安装完整")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="多模态轨迹检索数据库修复验证测试")
    parser.add_argument(
        '--quick', 
        action='store_true',
        help='快速测试模式'
    )
    parser.add_argument(
        '--cleanup', 
        action='store_true',
        help='测试后清理数据'
    )
    
    args = parser.parse_args()
    
    print("🧪 多模态轨迹检索数据库修复验证测试")
    print("="*60)
    
    # 运行测试
    tester = MultimodalDatabaseTester(
        quick_mode=args.quick,
        cleanup=args.cleanup
    )
    
    results = tester.run_comprehensive_test()
    
    # 提供后续建议
    print("\n" + "="*60)
    print("🎯 后续操作建议:")
    print("="*60)
    
    if results.get('data_saved', False):
        print("✅ 测试通过！现在可以正常使用多模态轨迹检索功能：")
        print("")
        print("python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print("  --text 'bicycle crossing intersection' \\")
        print("  --collection 'ddi_collection_camera_encoded_1' \\")
        print("  --output-table 'discovered_trajectories' \\")
        print("  --verbose")
    else:
        print("❌ 测试未完全通过，建议:")
        print("1. 检查PostgreSQL服务状态")
        print("2. 重新运行数据库连接诊断:")
        print("   python database_connection_diagnostic.py")
        print("3. 如需要，重新运行配置修复:")
        print("   python fix_database_config.py")

if __name__ == "__main__":
    main()
