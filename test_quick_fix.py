#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, '.')

from src.spdatalab.fusion.spatial_join_production import export_analysis_to_qgis

try:
    print("🔧 测试PostGIS几何数据保存修复...")
    analysis_id = export_analysis_to_qgis(
        analysis_type='intersection_type',
        city_filter='A253',
        include_geometry=True
    )
    print(f'✅ 成功导出分析: {analysis_id}')
    print("✅ PostGIS几何数据保存修复成功！")
except Exception as e:
    print(f'❌ 导出失败: {e}')
    print("❌ 需要进一步调试") 