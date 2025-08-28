#!/usr/bin/env python3
"""
数据库配置修复脚本

此脚本用于修复多模态轨迹检索中的数据库连接配置问题：
- 将 local_pg 主机名替换为 localhost 或 127.0.0.1
- 自动备份原始文件
- 验证修复效果

使用方法:
1. 先运行诊断: python database_connection_diagnostic.py
2. 根据诊断结果运行修复: python fix_database_config.py [--host localhost|127.0.0.1]
3. 重新测试多模态轨迹检索功能
"""

import argparse
import re
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConfigFixer:
    """数据库配置修复器"""
    
    def __init__(self, target_host: str = "localhost"):
        self.target_host = target_host
        self.target_files = [
            "src/spdatalab/dataset/polygon_trajectory_query.py",
            "src/spdatalab/fusion/multimodal_trajectory_retrieval.py"
        ]
        
        # 匹配模式
        self.patterns = [
            {
                'name': 'LOCAL_DSN配置',
                'pattern': r'LOCAL_DSN\s*=\s*["\']postgresql\+psycopg://([^@]+)@local_pg:(\d+)/([^"\']+)["\']',
                'replacement': lambda m: f'LOCAL_DSN = "postgresql+psycopg://{m.group(1)}@{self.target_host}:{m.group(2)}/{m.group(3)}"'
            },
            {
                'name': '其他local_pg引用',
                'pattern': r'local_pg:(\d+)',
                'replacement': lambda m: f'{self.target_host}:{m.group(1)}'
            }
        ]
        
        self.fix_results = {
            'files_processed': 0,
            'changes_made': 0,
            'backup_files': [],
            'errors': []
        }
    
    def run_fix(self) -> bool:
        """运行修复流程"""
        logger.info(f"🔧 开始数据库配置修复 (目标主机: {self.target_host})")
        
        success = True
        
        for file_path in self.target_files:
            try:
                if self.fix_file(file_path):
                    self.fix_results['files_processed'] += 1
                else:
                    success = False
            except Exception as e:
                logger.error(f"修复文件失败: {file_path} - {e}")
                self.fix_results['errors'].append(f"{file_path}: {e}")
                success = False
        
        self.generate_fix_report()
        return success
    
    def fix_file(self, file_path: str) -> bool:
        """修复单个文件"""
        target_file = Path(file_path)
        
        if not target_file.exists():
            logger.warning(f"文件不存在，跳过: {file_path}")
            return True  # 不算作错误
        
        logger.info(f"🔄 处理文件: {file_path}")
        
        try:
            # 读取原始内容
            with open(target_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # 应用修复模式
            modified_content = original_content
            changes_in_file = 0
            
            for pattern_info in self.patterns:
                pattern = pattern_info['pattern']
                replacement_func = pattern_info['replacement']
                
                # 查找匹配项
                matches = list(re.finditer(pattern, modified_content))
                
                if matches:
                    logger.info(f"   发现 {len(matches)} 个匹配项: {pattern_info['name']}")
                    
                    # 应用替换
                    modified_content = re.sub(pattern, replacement_func, modified_content)
                    changes_in_file += len(matches)
            
            # 如果有修改，创建备份并写入新内容
            if changes_in_file > 0:
                # 创建备份
                backup_path = self.create_backup(target_file)
                self.fix_results['backup_files'].append(str(backup_path))
                
                # 写入修复后的内容
                with open(target_file, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
                
                self.fix_results['changes_made'] += changes_in_file
                logger.info(f"   ✅ 文件修复完成: {changes_in_file} 处修改")
                logger.info(f"   📁 备份创建: {backup_path}")
            else:
                logger.info(f"   ℹ️ 文件无需修改")
            
            return True
            
        except Exception as e:
            logger.error(f"处理文件失败: {file_path} - {e}")
            return False
    
    def create_backup(self, original_file: Path) -> Path:
        """创建备份文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = original_file.with_suffix(f'.backup_{timestamp}.py')
        
        shutil.copy2(original_file, backup_path)
        return backup_path
    
    def generate_fix_report(self):
        """生成修复报告"""
        logger.info("\n" + "="*60)
        logger.info("📋 数据库配置修复报告")
        logger.info("="*60)
        
        logger.info(f"\n🔧 修复概述:")
        logger.info(f"   目标主机: {self.target_host}")
        logger.info(f"   处理文件数: {self.fix_results['files_processed']}")
        logger.info(f"   修改数量: {self.fix_results['changes_made']}")
        logger.info(f"   错误数量: {len(self.fix_results['errors'])}")
        
        if self.fix_results['backup_files']:
            logger.info(f"\n📁 备份文件:")
            for backup in self.fix_results['backup_files']:
                logger.info(f"   {backup}")
        
        if self.fix_results['errors']:
            logger.warning(f"\n❌ 错误列表:")
            for error in self.fix_results['errors']:
                logger.warning(f"   {error}")
        
        if self.fix_results['changes_made'] > 0:
            logger.info(f"\n✅ 修复成功！可以重新测试多模态轨迹检索功能")
        else:
            logger.info(f"\nℹ️ 未发现需要修复的配置")

def create_test_script():
    """创建测试脚本"""
    test_script = '''#!/usr/bin/env python3
"""
多模态轨迹检索数据库修复测试脚本

此脚本用于验证数据库配置修复后的功能是否正常
"""

import sys
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

def test_multimodal_trajectory_retrieval():
    """测试多模态轨迹检索功能"""
    logger.info("🧪 测试多模态轨迹检索功能...")
    
    # 测试命令
    test_cmd = [
        sys.executable, "-m", "spdatalab.fusion.multimodal_trajectory_retrieval",
        "--text", "bicycle crossing intersection",
        "--collection", "ddi_collection_camera_encoded_1", 
        "--output-table", "discovered_trajectories_test",
        "--verbose"
    ]
    
    try:
        logger.info("运行命令: " + " ".join(test_cmd))
        
        result = subprocess.run(
            test_cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        logger.info(f"返回代码: {result.returncode}")
        
        if result.stdout:
            logger.info("标准输出:")
            logger.info(result.stdout)
        
        if result.stderr:
            logger.warning("错误输出:")
            logger.warning(result.stderr)
        
        if result.returncode == 0:
            logger.info("✅ 多模态轨迹检索测试成功")
            return True
        else:
            logger.error("❌ 多模态轨迹检索测试失败")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ 测试超时")
        return False
    except Exception as e:
        logger.error(f"❌ 测试执行失败: {e}")
        return False

def main():
    """主函数"""
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 多模态轨迹检索数据库修复测试")
    print("="*50)
    
    # 检查修复后的文件是否存在
    target_file = Path("src/spdatalab/dataset/polygon_trajectory_query.py")
    if not target_file.exists():
        logger.error(f"目标文件不存在: {target_file}")
        return False
    
    # 运行测试
    success = test_multimodal_trajectory_retrieval()
    
    if success:
        print("\\n🎉 测试通过！数据库配置修复成功")
    else:
        print("\\n❌ 测试失败，可能需要进一步排查")
    
    return success

if __name__ == "__main__":
    main()
'''
    
    with open('test_database_fix.py', 'w', encoding='utf-8') as f:
        f.write(test_script)
    
    logger.info("✅ 已创建测试脚本: test_database_fix.py")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="修复spdatalab数据库连接配置")
    parser.add_argument(
        '--host', 
        choices=['localhost', '127.0.0.1'], 
        default='localhost',
        help='目标数据库主机 (默认: localhost)'
    )
    parser.add_argument(
        '--test', 
        action='store_true',
        help='修复后自动运行测试'
    )
    
    args = parser.parse_args()
    
    print("🔧 spdatalab 数据库配置修复工具")
    print("="*50)
    
    # 运行修复
    fixer = DatabaseConfigFixer(target_host=args.host)
    success = fixer.run_fix()
    
    if success:
        # 创建测试脚本
        create_test_script()
        
        print("\n" + "="*60)
        print("🎯 下一步操作建议:")
        print("="*60)
        print("1. 运行测试验证修复效果:")
        print("   python test_database_fix.py")
        print("")
        print("2. 或手动测试多模态轨迹检索:")
        print("   python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print("     --text 'bicycle crossing intersection' \\")
        print("     --collection 'ddi_collection_camera_encoded_1' \\")
        print("     --output-table 'discovered_trajectories' \\")
        print("     --verbose")
        
        # 自动运行测试
        if args.test:
            print("\n🧪 自动运行测试...")
            subprocess.run([sys.executable, 'test_database_fix.py'])
    
    else:
        print("\n❌ 修复失败，请检查错误信息")

if __name__ == "__main__":
    main()
