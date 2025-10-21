#!/bin/bash

# 脚本：从 Git 历史提交中查找 .env 文件
# 使用方法：bash find_env_in_history.sh

echo "=================================================="
echo "从 Git 历史提交中查找 .env 文件"
echo "=================================================="
echo ""

# 检查是否在 git 仓库中
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "错误：当前目录不是 Git 仓库"
    exit 1
fi

echo "正在搜索包含 .env 文件的提交..."
echo ""

# 查找所有包含 .env 文件的提交
commits=$(git log --all --full-history --pretty=format:"%H|%ai|%an|%s" -- .env 2>/dev/null)

if [ -z "$commits" ]; then
    echo "❌ 在 Git 历史中没有找到 .env 文件的提交记录"
    echo ""
    echo "可能的原因："
    echo "1. .env 文件从未被提交过（通常在 .gitignore 中）"
    echo "2. .env 文件已被完全删除且历史被清理"
    echo ""
    echo "建议："
    echo "- 检查 env.example 文件作为模板"
    echo "- 联系其他团队成员获取 .env 配置"
    echo "- 检查本地是否有 .env 的备份文件"
    exit 1
fi

echo "✓ 找到了包含 .env 文件的提交！"
echo ""
echo "=================================================="

# 显示找到的提交
count=0
while IFS='|' read -r hash date author subject; do
    count=$((count + 1))
    echo "[$count] 提交: $hash"
    echo "    时间: $date"
    echo "    作者: $author"
    echo "    说明: $subject"
    echo ""
done <<< "$commits"

echo "=================================================="

# 获取最新的提交（第一行）
latest_commit=$(echo "$commits" | head -n 1 | cut -d'|' -f1)
latest_date=$(echo "$commits" | head -n 1 | cut -d'|' -f2)

echo ""
echo "最新包含 .env 的提交："
echo "  提交 Hash: $latest_commit"
echo "  提交时间: $latest_date"
echo ""

# 检查该提交中的 .env 文件内容
echo "=================================================="
echo ".env 文件内容（来自最新提交 $latest_commit）："
echo "=================================================="
git show "$latest_commit:.env" 2>/dev/null

echo ""
echo "=================================================="
echo "如何恢复这个 .env 文件："
echo "=================================================="
echo ""
echo "方法 1：直接恢复到当前目录"
echo "  git show $latest_commit:.env > .env"
echo ""
echo "方法 2：恢复到备份文件"
echo "  git show $latest_commit:.env > .env.recovered"
echo ""
echo "方法 3：从特定提交恢复"
echo "  git checkout $latest_commit -- .env"
echo "  （注意：这会修改工作区，需要再提交或 stash）"
echo ""

# 检查是否有 env.example
if [ -f "env.example" ]; then
    echo "=================================================="
    echo "提示：项目中有 env.example 文件"
    echo "=================================================="
    echo "你也可以参考 env.example 来创建新的 .env 文件："
    echo "  cp env.example .env"
    echo "  # 然后编辑 .env 填入实际的配置值"
    echo ""
fi

