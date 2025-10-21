#!/bin/bash

# 脚本：移除临时调试代码
# 使用方法：bash scripts/utilities/remove_debug_logging.sh

echo "=================================================="
echo "移除临时调试代码"
echo "=================================================="
echo ""

# 包含临时调试代码的文件列表
files=(
    "src/spdatalab/common/io_obs.py"
    "src/spdatalab/common/file_utils.py"
    "src/spdatalab/dataset/scene_list_generator.py"
    "src/spdatalab/dataset/dataset_manager.py"
)

echo "将从以下文件中移除调试代码："
for file in "${files[@]}"; do
    echo "  - $file"
done
echo ""

read -p "确认移除？(y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消操作"
    exit 1
fi

echo ""
echo "开始处理..."
echo ""

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "处理: $file"
        
        # 创建备份
        cp "$file" "$file.backup"
        
        # 移除所有在 "临时调试代码 START" 和 "临时调试代码 END" 之间的代码
        # 使用 sed 删除这些标记之间的行（包括标记行本身）
        sed -i '/# ============ 临时调试代码 START ============/,/# ============ 临时调试代码 END ============/d' "$file"
        
        echo "  ✓ 已处理并备份到 $file.backup"
    else
        echo "  ✗ 文件不存在: $file"
    fi
done

echo ""
echo "=================================================="
echo "处理完成！"
echo "=================================================="
echo ""
echo "备份文件已保存为 .backup 后缀"
echo "如果需要恢复，可以使用: cp <file>.backup <file>"
echo ""

