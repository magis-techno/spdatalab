#!/usr/bin/env python3
"""
数据库连接诊断和修复脚本

此脚本用于诊断多模态轨迹检索中的数据库连接问题：
- 检查DNS解析 (local_pg 主机名)
- 测试数据库连接
- 提供修复方案
- 验证修复效果

错误背景：
用户运行多模态轨迹检索命令时，在创建轨迹表阶段遇到：
(psycopg.OperationalError) [Errno -3] Temporary failure in name resolution

原因分析：
代码中使用 local_pg:5432 作为PostgreSQL主机，但DNS无法解析此主机名
"""

import socket
import subprocess
import sys
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any
import psycopg
from sqlalchemy import create_engine, text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConnectionDiagnostic:
    """数据库连接诊断器"""
    
    def __init__(self):
        self.original_dsn = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        self.fallback_dsn = "postgresql+psycopg://postgres:postgres@localhost:5432/postgres"
        self.docker_dsn = "postgresql+psycopg://postgres:postgres@127.0.0.1:5432/postgres"
        
        self.test_results = {
            'dns_resolution': False,
            'original_connection': False,
            'localhost_connection': False,
            'docker_connection': False,
            'docker_container_running': False,
            'recommended_solution': None
        }
    
    def run_full_diagnostic(self) -> Dict[str, Any]:
        """运行完整的诊断流程"""
        logger.info("🔍 开始数据库连接诊断...")
        
        # 1. DNS解析测试
        self.test_dns_resolution()
        
        # 2. Docker容器检查
        self.check_docker_containers()
        
        # 3. 数据库连接测试
        self.test_database_connections()
        
        # 4. 分析和推荐
        self.analyze_and_recommend()
        
        # 5. 生成报告
        self.generate_diagnostic_report()
        
        return self.test_results
    
    def test_dns_resolution(self):
        """测试DNS解析"""
        logger.info("🌐 测试DNS解析: local_pg")
        
        try:
            socket.gethostbyname('local_pg')
            self.test_results['dns_resolution'] = True
            logger.info("✅ DNS解析成功: local_pg")
        except socket.gaierror as e:
            self.test_results['dns_resolution'] = False
            logger.warning(f"❌ DNS解析失败: local_pg - {e}")
    
    def check_docker_containers(self):
        """检查Docker容器状态"""
        logger.info("🐳 检查Docker容器状态...")
        
        try:
            # 检查是否有PostgreSQL相关的容器
            result = subprocess.run(
                ['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                output = result.stdout
                logger.info("Docker容器列表:")
                logger.info(output)
                
                # 查找PostgreSQL相关容器
                if 'local_pg' in output.lower() or 'postgres' in output.lower():
                    self.test_results['docker_container_running'] = True
                    logger.info("✅ 发现PostgreSQL相关容器")
                else:
                    logger.warning("⚠️ 未发现PostgreSQL相关容器")
            else:
                logger.warning("❌ 无法访问Docker")
                
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.warning(f"❌ Docker检查失败: {e}")
    
    def test_database_connections(self):
        """测试各种数据库连接方案"""
        logger.info("🔌 测试数据库连接...")
        
        # 测试原始连接
        self.test_connection("原始连接(local_pg)", self.original_dsn, 'original_connection')
        
        # 测试localhost连接
        self.test_connection("localhost连接", self.fallback_dsn, 'localhost_connection')
        
        # 测试127.0.0.1连接
        self.test_connection("127.0.0.1连接", self.docker_dsn, 'docker_connection')
    
    def test_connection(self, name: str, dsn: str, result_key: str):
        """测试单个数据库连接"""
        try:
            logger.info(f"🔄 测试{name}: {dsn}")
            
            engine = create_engine(dsn, future=True)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                
                self.test_results[result_key] = True
                logger.info(f"✅ {name}成功")
                logger.info(f"   PostgreSQL版本: {version[:50]}...")
                
        except Exception as e:
            self.test_results[result_key] = False
            logger.warning(f"❌ {name}失败: {e}")
    
    def analyze_and_recommend(self):
        """分析测试结果并提供推荐方案"""
        logger.info("🔍 分析测试结果...")
        
        if self.test_results['original_connection']:
            self.test_results['recommended_solution'] = 'original'
            logger.info("✅ 原始配置工作正常，无需修改")
            
        elif self.test_results['localhost_connection']:
            self.test_results['recommended_solution'] = 'localhost'
            logger.info("💡 推荐使用localhost连接")
            
        elif self.test_results['docker_connection']:
            self.test_results['recommended_solution'] = 'docker_ip'
            logger.info("💡 推荐使用127.0.0.1连接")
            
        else:
            self.test_results['recommended_solution'] = 'setup_required'
            logger.warning("⚠️ 所有连接均失败，需要设置PostgreSQL环境")
    
    def generate_diagnostic_report(self):
        """生成诊断报告"""
        logger.info("\n" + "="*60)
        logger.info("📋 数据库连接诊断报告")
        logger.info("="*60)
        
        # 测试结果概述
        logger.info("\n🔍 测试结果概述:")
        logger.info(f"   DNS解析(local_pg): {'✅' if self.test_results['dns_resolution'] else '❌'}")
        logger.info(f"   原始连接(local_pg): {'✅' if self.test_results['original_connection'] else '❌'}")
        logger.info(f"   localhost连接: {'✅' if self.test_results['localhost_connection'] else '❌'}")
        logger.info(f"   127.0.0.1连接: {'✅' if self.test_results['docker_connection'] else '❌'}")
        logger.info(f"   Docker容器运行: {'✅' if self.test_results['docker_container_running'] else '❌'}")
        
        # 推荐方案
        logger.info(f"\n💡 推荐方案: {self.test_results['recommended_solution']}")
        
        # 详细建议
        self.provide_detailed_recommendations()
    
    def provide_detailed_recommendations(self):
        """提供详细的修复建议"""
        solution = self.test_results['recommended_solution']
        
        logger.info("\n🛠️ 修复建议:")
        
        if solution == 'original':
            logger.info("   ✅ 当前配置正常工作，无需修改")
            
        elif solution == 'localhost':
            logger.info("   1. 修改数据库配置文件中的连接字符串")
            logger.info("   2. 将 'local_pg' 替换为 'localhost'")
            logger.info("   3. 运行修复脚本: python fix_database_config.py")
            
        elif solution == 'docker_ip':
            logger.info("   1. 修改数据库配置文件中的连接字符串")
            logger.info("   2. 将 'local_pg' 替换为 '127.0.0.1'")
            logger.info("   3. 运行修复脚本: python fix_database_config.py")
            
        elif solution == 'setup_required':
            logger.info("   ⚠️ 需要设置PostgreSQL环境:")
            logger.info("   1. 安装PostgreSQL服务器")
            logger.info("   2. 或者启动PostgreSQL Docker容器:")
            logger.info("      docker run -d --name local_pg -p 5432:5432 \\")
            logger.info("      -e POSTGRES_PASSWORD=postgres postgres:latest")
            logger.info("   3. 等待数据库启动后重新测试")

def create_database_fix_script():
    """创建数据库配置修复脚本"""
    fix_script = '''#!/usr/bin/env python3
"""
数据库配置修复脚本
根据诊断结果自动修复数据库连接配置
"""

import re
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def fix_database_config():
    """修复数据库配置"""
    
    # 目标文件
    target_file = Path("src/spdatalab/dataset/polygon_trajectory_query.py")
    
    if not target_file.exists():
        logger.error(f"目标文件不存在: {target_file}")
        return False
    
    try:
        # 读取文件内容
        with open(target_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 备份原文件
        backup_file = target_file.with_suffix('.py.backup')
        with open(backup_file, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"已创建备份: {backup_file}")
        
        # 替换数据库连接字符串
        old_pattern = r'LOCAL_DSN = "postgresql\+psycopg://postgres:postgres@local_pg:5432/postgres"'
        new_dsn = 'LOCAL_DSN = "postgresql+psycopg://postgres:postgres@localhost:5432/postgres"'
        
        if re.search(old_pattern, content):
            new_content = re.sub(old_pattern, new_dsn, content)
            
            # 写入修复后的内容
            with open(target_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.info(f"✅ 数据库配置已修复: {target_file}")
            logger.info(f"   原配置: @local_pg:5432")
            logger.info(f"   新配置: @localhost:5432")
            return True
        else:
            logger.warning("未找到需要修复的配置项")
            return False
            
    except Exception as e:
        logger.error(f"修复配置失败: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fix_database_config()
'''
    
    with open('fix_database_config.py', 'w', encoding='utf-8') as f:
        f.write(fix_script)
    
    logger.info("✅ 已创建数据库配置修复脚本: fix_database_config.py")

def main():
    """主函数"""
    print("🏥 spdatalab 数据库连接诊断工具")
    print("="*50)
    
    # 运行诊断
    diagnostic = DatabaseConnectionDiagnostic()
    results = diagnostic.run_full_diagnostic()
    
    # 创建修复脚本
    create_database_fix_script()
    
    # 最终建议
    print("\n" + "="*60)
    print("🎯 下一步操作建议:")
    print("="*60)
    
    if results['recommended_solution'] in ['localhost', 'docker_ip']:
        print("1. 运行修复脚本:")
        print("   python fix_database_config.py")
        print("")
        print("2. 重新测试多模态轨迹检索:")
        print("   python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print("     --text 'bicycle crossing intersection' \\")
        print("     --collection 'ddi_collection_camera_encoded_1' \\")
        print("     --output-table 'discovered_trajectories' \\")
        print("     --verbose")
        
    elif results['recommended_solution'] == 'setup_required':
        print("1. 设置PostgreSQL环境 (选择其一):")
        print("   选项A - Docker方式:")
        print("   docker run -d --name local_pg -p 5432:5432 \\")
        print("     -e POSTGRES_PASSWORD=postgres postgres:latest")
        print("")
        print("   选项B - 本地安装PostgreSQL")
        print("")
        print("2. 等待数据库启动后，重新运行诊断:")
        print("   python database_connection_diagnostic.py")
    
    else:
        print("✅ 数据库连接正常，可以直接使用")

if __name__ == "__main__":
    main()
