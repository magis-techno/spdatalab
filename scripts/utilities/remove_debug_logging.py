#!/usr/bin/env python3
"""
移除临时调试代码的脚本

使用方法：
    python scripts/utilities/remove_debug_logging.py
    
或在项目根目录：
    python -m scripts.utilities.remove_debug_logging
"""

import sys
from pathlib import Path
import shutil
import re

# 包含临时调试代码的文件列表
DEBUG_FILES = [
    "src/spdatalab/common/io_obs.py",
    "src/spdatalab/common/file_utils.py",
    "src/spdatalab/dataset/scene_list_generator.py",
    "src/spdatalab/dataset/dataset_manager.py",
]

# 调试代码的标记
DEBUG_START_MARKER = "# ============ 临时调试代码 START ============"
DEBUG_END_MARKER = "# ============ 临时调试代码 END ============"


def remove_debug_code_from_file(file_path: Path) -> tuple[bool, int]:
    """从文件中移除调试代码
    
    Args:
        file_path: 文件路径
        
    Returns:
        (是否成功, 移除的行数) 元组
    """
    if not file_path.exists():
        print(f"  ✗ 文件不存在: {file_path}")
        return False, 0
    
    try:
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 创建备份
        backup_path = file_path.with_suffix(file_path.suffix + '.backup')
        shutil.copy2(file_path, backup_path)
        print(f"  📝 已创建备份: {backup_path.name}")
        
        # 移除调试代码
        new_lines = []
        in_debug_block = False
        removed_lines = 0
        current_block_start = -1
        
        for i, line in enumerate(lines, 1):
            if DEBUG_START_MARKER in line:
                in_debug_block = True
                current_block_start = i
                removed_lines += 1
                continue
            elif DEBUG_END_MARKER in line:
                in_debug_block = False
                removed_lines += 1
                continue
            
            if not in_debug_block:
                new_lines.append(line)
            else:
                removed_lines += 1
        
        # 检查是否有未关闭的调试块
        if in_debug_block:
            print(f"  ⚠️  警告：发现未关闭的调试块（从第 {current_block_start} 行开始）")
            return False, 0
        
        # 写回文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        
        print(f"  ✓ 已移除 {removed_lines} 行调试代码")
        return True, removed_lines
        
    except Exception as e:
        print(f"  ✗ 处理失败: {e}")
        return False, 0


def main():
    """主函数"""
    print("=" * 70)
    print("移除临时调试代码")
    print("=" * 70)
    print()
    
    # 获取项目根目录
    project_root = Path(__file__).resolve().parents[2]
    
    print("将从以下文件中移除调试代码：")
    for file_path in DEBUG_FILES:
        print(f"  - {file_path}")
    print()
    
    # 确认
    response = input("确认移除？(y/n) ").strip().lower()
    if response != 'y':
        print("已取消操作")
        return
    
    print()
    print("开始处理...")
    print()
    
    # 处理每个文件
    total_removed = 0
    success_count = 0
    
    for file_path_str in DEBUG_FILES:
        file_path = project_root / file_path_str
        print(f"处理: {file_path_str}")
        
        success, removed = remove_debug_code_from_file(file_path)
        if success:
            success_count += 1
            total_removed += removed
        print()
    
    # 输出总结
    print("=" * 70)
    print("处理完成！")
    print("=" * 70)
    print()
    print(f"成功处理: {success_count}/{len(DEBUG_FILES)} 个文件")
    print(f"共移除: {total_removed} 行调试代码")
    print()
    print("备份文件已保存为 .backup 后缀")
    print("如果需要恢复，可以使用:")
    print("  cp <file>.backup <file>")
    print()
    print("验证代码无误后，可以删除备份文件:")
    print("  find src -name '*.backup' -delete")
    print("=" * 70)


if __name__ == '__main__':
    main()

