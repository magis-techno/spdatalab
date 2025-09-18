#!/usr/bin/env python3
"""
测试代码修改是否生效
验证当前使用的是新的SQL逻辑还是旧的逻辑
"""

def test_sql_changes():
    """检查SQL文件的当前内容"""
    from pathlib import Path
    
    sql_file = Path("examples/dataset/bbox_examples/sql/overlap_analysis.sql")
    
    if sql_file.exists():
        content = sql_file.read_text(encoding='utf-8')
        
        print("🔍 检查SQL文件内容...")
        print("=" * 50)
        
        # 检查关键标识
        if "individual_overlaps AS" in content:
            print("✅ 发现新逻辑: individual_overlaps")
        else:
            print("❌ 未发现新逻辑: individual_overlaps")
            
        if "GROUP BY ST_SnapToGrid" in content:
            print("❌ 仍然包含旧逻辑: GROUP BY ST_SnapToGrid")
        else:
            print("✅ 已移除旧逻辑: GROUP BY ST_SnapToGrid")
            
        if "ORDER BY total_overlap_area DESC" in content:
            print("✅ 发现新排序: ORDER BY total_overlap_area DESC")
        else:
            print("❌ 未发现新排序逻辑")
            
        if "ORDER BY overlap_count DESC" in content:
            print("❌ 仍然包含旧排序: ORDER BY overlap_count DESC")
        else:
            print("✅ 已移除旧排序: ORDER BY overlap_count DESC")
            
        # 检查关键片段
        print(f"\n📄 SQL文件关键片段:")
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if "individual_overlaps AS" in line or "GROUP BY ST_SnapToGrid" in line or "ORDER BY" in line:
                start = max(0, i-2)
                end = min(len(lines), i+3)
                print(f"Lines {start+1}-{end}:")
                for j in range(start, end):
                    prefix = ">>> " if j == i else "    "
                    print(f"{prefix}{j+1:3d}: {lines[j]}")
                print()
    else:
        print("❌ SQL文件不存在")

def test_python_changes():
    """检查Python文件的修改"""
    from pathlib import Path
    
    py_files = [
        "examples/dataset/bbox_examples/bbox_overlap_analysis.py",
        "examples/dataset/bbox_examples/run_overlap_analysis.py"
    ]
    
    for py_file_path in py_files:
        py_file = Path(py_file_path)
        if py_file.exists():
            content = py_file.read_text(encoding='utf-8')
            
            print(f"\n🔍 检查Python文件: {py_file.name}")
            print("-" * 40)
            
            if "individual_overlaps AS" in content:
                print("✅ 发现新逻辑: individual_overlaps")
            else:
                print("❌ 未发现新逻辑: individual_overlaps")
                
            if "ORDER BY total_overlap_area DESC" in content:
                print("✅ 发现新排序: ORDER BY total_overlap_area DESC")
            else:
                print("❌ 未发现新排序逻辑")
        else:
            print(f"❌ 文件不存在: {py_file_path}")

if __name__ == "__main__":
    print("🧪 测试代码修改是否生效")
    print("=" * 60)
    
    test_sql_changes()
    test_python_changes()
    
    print(f"\n💡 如果发现问题，可能的原因:")
    print(f"   1. 使用了内置SQL而不是文件中的SQL")
    print(f"   2. 缓存问题，需要重启程序")
    print(f"   3. 使用了错误的脚本路径")
    print(f"   4. 数据库中的分析结果是之前的，需要重新运行")
