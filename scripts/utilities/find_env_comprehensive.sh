#!/bin/bash

# 综合脚本：从多个位置查找 .env 文件或配置
# 使用方法：bash find_env_comprehensive.sh

echo "=================================================="
echo "综合查找 .env 配置文件"
echo "=================================================="
echo ""

# 1. 检查当前工作区
echo "[1] 检查当前工作区..."
if [ -f ".env" ]; then
    echo "✓ 当前目录存在 .env 文件"
    echo "  文件大小: $(ls -lh .env | awk '{print $5}')"
    echo "  修改时间: $(ls -l .env | awk '{print $6, $7, $8}')"
    echo ""
else
    echo "✗ 当前目录不存在 .env 文件"
    echo ""
fi

# 2. 检查是否有备份文件
echo "[2] 检查备份文件..."
backup_files=$(ls -1 .env* 2>/dev/null | grep -v "env.example")
if [ -n "$backup_files" ]; then
    echo "✓ 找到相关备份文件："
    echo "$backup_files" | while read file; do
        echo "  - $file ($(ls -lh "$file" | awk '{print $5}'))"
    done
    echo ""
else
    echo "✗ 没有找到 .env 备份文件"
    echo ""
fi

# 3. 检查 env.example
echo "[3] 检查 env.example 模板..."
if [ -f "env.example" ]; then
    echo "✓ 找到 env.example 模板文件"
    echo "  可以使用: cp env.example .env"
    echo ""
else
    echo "✗ 没有找到 env.example"
    echo ""
fi

# 4. 检查 git stash
echo "[4] 检查 Git Stash..."
stash_count=$(git stash list 2>/dev/null | wc -l)
if [ $stash_count -gt 0 ]; then
    echo "  共有 $stash_count 个 stash"
    found_in_stash=0
    for i in $(seq 0 $((stash_count - 1))); do
        if git stash show "stash@{$i}" --name-only 2>/dev/null | grep -q "^\.env$"; then
            echo "  ✓ stash@{$i} 包含 .env"
            found_in_stash=$((found_in_stash + 1))
        fi
    done
    if [ $found_in_stash -eq 0 ]; then
        echo "  ✗ 所有 stash 中都不包含 .env"
    fi
else
    echo "  没有 stash"
fi
echo ""

# 5. 检查 git 历史
echo "[5] 检查 Git 历史提交..."
commits=$(git log --all --full-history --pretty=format:"%H|%ai|%s" -- .env 2>/dev/null | head -n 5)
if [ -n "$commits" ]; then
    echo "✓ 在 Git 历史中找到 .env 文件"
    echo ""
    echo "最近的几个包含 .env 的提交："
    count=0
    while IFS='|' read -r hash date subject; do
        count=$((count + 1))
        short_hash=$(echo $hash | cut -c1-8)
        echo "  [$count] $short_hash - $date"
        echo "      $subject"
    done <<< "$commits"
    echo ""
    
    # 显示最新提交中的 .env 内容
    latest_commit=$(echo "$commits" | head -n 1 | cut -d'|' -f1)
    echo "  最新版本 .env 文件内容："
    echo "  ----------------------------------------"
    git show "$latest_commit:.env" 2>/dev/null | head -n 20 | sed 's/^/  /'
    echo "  ----------------------------------------"
    echo ""
else
    echo "✗ Git 历史中没有找到 .env 文件"
    echo ""
fi

# 6. 检查其他分支
echo "[6] 检查其他分支..."
branches_with_env=$(git ls-tree -r --name-only $(git rev-list --all) 2>/dev/null | grep "^\.env$" | head -n 1)
if [ -n "$branches_with_env" ]; then
    echo "✓ 在其他分支或提交中找到 .env"
    echo ""
else
    echo "✗ 其他分支中也没有 .env"
    echo ""
fi

# 7. 检查未跟踪的文件
echo "[7] 检查 Git 状态..."
if git status --short 2>/dev/null | grep -q "\.env"; then
    echo "✓ .env 文件在 git status 中出现"
    git status --short | grep "\.env"
    echo ""
else
    echo "✗ Git 状态中没有 .env"
    echo ""
fi

# 总结和建议
echo "=================================================="
echo "总结和建议"
echo "=================================================="
echo ""

if [ -n "$commits" ]; then
    latest_commit=$(echo "$commits" | head -n 1 | cut -d'|' -f1)
    echo "✓ 推荐操作：从 Git 历史恢复"
    echo "  git show $latest_commit:.env > .env"
    echo ""
elif [ -f "env.example" ]; then
    echo "✓ 推荐操作：使用 env.example 作为模板"
    echo "  cp env.example .env"
    echo "  # 然后编辑 .env 填入实际配置"
    echo ""
else
    echo "❌ 无法找到任何 .env 相关信息"
    echo ""
    echo "建议："
    echo "1. 检查项目文档中的配置说明"
    echo "2. 查看 README.md 中的环境变量说明"
    echo "3. 联系团队成员获取配置模板"
    echo "4. 检查是否有配置管理系统（如 vault）"
    echo ""
fi

echo "=================================================="

