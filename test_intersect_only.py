#!/usr/bin/env python3
"""
测试简化重叠检测（只要相交就算重叠）
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer
    
    print("🧪 测试简化重叠检测")
    print("=" * 50)
    
    # 创建分析器实例
    analyzer = BBoxOverlapAnalyzer()
    print("✅ 分析器创建成功")
    
    # 测试A263城市的简化重叠检测
    city_id = "A263"
    
    print(f"\n📊 测试城市: {city_id}")
    print(f"🎯 使用简化模式: 只要相交就算重叠")
    
    # 执行简化分析
    analysis_id = analyzer.run_overlap_analysis(
        analysis_id=f"test_intersect_only_{city_id}",
        city_filter=city_id,
        min_overlap_area=0.0,  # 这个参数会被忽略
        top_n=10,
        intersect_only=True  # 关键参数
    )
    
    print(f"\n📋 分析完成，ID: {analysis_id}")
    
    # 查看结果摘要
    summary = analyzer.get_analysis_summary(analysis_id)
    if not summary.empty:
        print(f"\n📊 结果摘要:")
        print(summary.to_string(index=False))
    else:
        print(f"\n📭 未找到重叠结果")
    
except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
