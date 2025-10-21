#!/bin/bash

# 脚本：查找包含 .env 文件的 git stash
# 使用方法：bash find_env_in_stash.sh

echo "=================================================="
echo "查找包含 .env 文件的 Git Stash"
echo "=================================================="
echo ""

# 检查是否在 git 仓库中
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "错误：当前目录不是 Git 仓库"
    exit 1
fi

# 获取 stash 数量
stash_count=$(git stash list | wc -l)
echo "总共有 $stash_count 个 stash"
echo ""

found=0

# 遍历所有 stash
for i in $(seq 0 $((stash_count - 1))); do
    # 检查该 stash 中是否包含 .env 文件
    if git stash show "stash@{$i}" --name-only 2>/dev/null | grep -q "^\.env$"; then
        echo "✓ 找到了！stash@{$i} 包含 .env 文件"
        echo "  创建时间："
        git stash list | grep "stash@{$i}"
        echo ""
        echo "  包含的文件："
        git stash show "stash@{$i}" --name-only
        echo ""
        echo "  .env 文件的内容预览（前 10 行）："
        echo "  ----------------------------------------"
        git show "stash@{$i}:.env" 2>/dev/null | head -n 10 | sed 's/^/  /'
        echo "  ----------------------------------------"
        echo ""
        found=$((found + 1))
    fi
done

echo "=================================================="
if [ $found -eq 0 ]; then
    echo "❌ 没有找到包含 .env 文件的 stash"
else
    echo "✓ 共找到 $found 个包含 .env 文件的 stash"
    echo ""
    echo "如果要恢复某个 stash 中的 .env 文件，可以使用："
    echo "  git show stash@{N}:.env > .env"
    echo ""
    echo "如果要应用整个 stash，可以使用："
    echo "  git stash apply stash@{N}"
fi
echo "=================================================="

