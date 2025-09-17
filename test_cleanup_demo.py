#!/usr/bin/env python3
"""
测试清理功能演示
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer
    
    print("🧪 测试清理功能演示")
    print("=" * 50)
    
    # 创建分析器实例
    analyzer = BBoxOverlapAnalyzer()
    print("✅ 分析器创建成功")
    
    print("\n📋 1. 列出分析结果")
    print("-" * 30)
    df = analyzer.list_analysis_results()
    
    print(f"\n🧹 2. 清理功能测试（试运行）")
    print("-" * 30)
    
    # 测试按模式清理
    if not df.empty:
        # 使用一个不存在的模式进行安全测试
        result = analyzer.cleanup_analysis_results(
            pattern="test_demo_%",
            dry_run=True
        )
        print(f"按模式清理测试完成")
    
    print(f"\n🎨 3. 视图清理测试（试运行）")
    print("-" * 30)
    analyzer.cleanup_qgis_views(confirm=False)
    
    print(f"\n✅ 所有清理功能测试完成")
    print(f"💡 所有测试都是安全的试运行模式")
    
except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
