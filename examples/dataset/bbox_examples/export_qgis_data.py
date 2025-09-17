#!/usr/bin/env python3
"""
QGIS数据导出工具
==============

专门解决QGIS无法浏览视图的问题，提供多种导出方案。

功能特性：
- 将视图物化为实际表
- 导出GeoJSON文件
- 生成QGIS样式文件
- 自动化QGIS集成

使用示例：
    # 导出指定分析的所有QGIS格式
    python export_qgis_data.py --analysis-id bbox_overlap_20231217_143025 --all
    
    # 只物化表
    python export_qgis_data.py --materialize-table
    
    # 只导出GeoJSON
    python export_qgis_data.py --export-geojson output.geojson
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='QGIS数据导出工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 导出所有格式（推荐）
  python export_qgis_data.py --analysis-id bbox_overlap_20231217_143025 --all
  
  # 物化视图为表
  python export_qgis_data.py --materialize-table
  
  # 导出GeoJSON
  python export_qgis_data.py --export-geojson hotspots.geojson
  
  # 生成样式文件
  python export_qgis_data.py --generate-style
  
  # 针对特定分析
  python export_qgis_data.py --analysis-id your_analysis_id --materialize-table
        """
    )
    
    # 基础参数
    parser.add_argument('--analysis-id', help='分析ID（可选，不指定则处理所有结果）')
    
    # 导出选项
    export_group = parser.add_argument_group('导出选项')
    export_group.add_argument('--all', action='store_true', 
                             help='导出所有格式（表+GeoJSON+样式）')
    export_group.add_argument('--materialize-table', action='store_true', 
                             help='将视图物化为实际表')
    export_group.add_argument('--export-geojson', 
                             help='导出GeoJSON文件（指定文件名）')
    export_group.add_argument('--generate-style', action='store_true', 
                             help='生成QGIS样式文件')
    
    # 控制参数
    control_group = parser.add_argument_group('控制选项')
    control_group.add_argument('--force-refresh', action='store_true', 
                              help='强制刷新已存在的表')
    control_group.add_argument('--output-dir', 
                              help='输出目录（默认当前目录）')
    
    args = parser.parse_args()
    
    print("🎨 QGIS数据导出工具")
    print("=" * 50)
    
    # 初始化分析器
    try:
        analyzer = BBoxOverlapAnalyzer()
        print("✅ 分析器初始化成功")
    except Exception as e:
        print(f"❌ 分析器初始化失败: {e}")
        return 1
    
    # 设置输出目录
    output_dir = Path(args.output_dir) if args.output_dir else Path.cwd()
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
        print(f"📁 创建输出目录: {output_dir}")
    
    print(f"📁 输出目录: {output_dir}")
    if args.analysis_id:
        print(f"🎯 分析ID: {args.analysis_id}")
    else:
        print(f"🌐 处理所有分析结果")
    
    success_count = 0
    
    try:
        # 如果指定了--all，启用所有导出选项
        if args.all:
            args.materialize_table = True
            args.export_geojson = f"bbox_overlap_{args.analysis_id if args.analysis_id else 'all'}.geojson"
            args.generate_style = True
        
        # 1. 物化表
        if args.materialize_table:
            print(f"\n📋 步骤1: 物化视图为表")
            print("-" * 30)
            success = analyzer.materialize_qgis_view(
                analysis_id=args.analysis_id,
                force_refresh=args.force_refresh
            )
            if success:
                success_count += 1
                print("✅ 物化表创建成功")
                print("💡 QGIS连接表名: qgis_bbox_overlap_hotspots_table")
            else:
                print("❌ 物化表创建失败")
        
        # 2. 导出GeoJSON
        if args.export_geojson:
            print(f"\n📁 步骤2: 导出GeoJSON")
            print("-" * 30)
            
            # 确保文件路径在输出目录中
            geojson_path = output_dir / args.export_geojson
            
            exported_file = analyzer.export_to_geojson(
                analysis_id=args.analysis_id,
                output_file=str(geojson_path)
            )
            if exported_file:
                success_count += 1
                print("✅ GeoJSON导出成功")
                print(f"📁 文件位置: {exported_file}")
                print("💡 可直接拖拽到QGIS中")
            else:
                print("❌ GeoJSON导出失败")
        
        # 3. 生成样式文件
        if args.generate_style:
            print(f"\n🎨 步骤3: 生成样式文件")
            print("-" * 30)
            
            style_path = output_dir / "bbox_overlap_hotspots.qml"
            style_file = analyzer.generate_qgis_style_file(str(style_path))
            if style_file:
                success_count += 1
                print("✅ 样式文件生成成功")
                print(f"🎨 文件位置: {style_file}")
                print("💡 在QGIS中加载样式: 图层属性 -> 样式 -> 加载样式")
            else:
                print("❌ 样式文件生成失败")
        
        # 4. 提供使用指南
        if success_count > 0:
            print(f"\n📋 QGIS使用指南")
            print("=" * 30)
            
            print(f"🎯 推荐使用方案:")
            
            if args.materialize_table:
                print(f"\n方案1: 数据库表连接")
                print(f"   1. 在QGIS中添加PostGIS连接")
                print(f"   2. 连接表: qgis_bbox_overlap_hotspots_table")
                print(f"   3. 主键字段: qgis_fid")
                print(f"   4. 几何字段: geometry")
            
            if args.export_geojson:
                print(f"\n方案2: GeoJSON文件（最简单）")
                print(f"   1. 直接拖拽文件到QGIS")
                print(f"   2. 或者: 图层 -> 添加图层 -> 添加矢量图层")
                print(f"   3. 文件: {geojson_path}")
            
            if args.generate_style:
                print(f"\n样式应用:")
                print(f"   1. 右键图层 -> 属性")
                print(f"   2. 样式 -> 样式 -> 加载样式")
                print(f"   3. 选择文件: {style_path}")
            
            print(f"\n🎨 样式字段说明:")
            print(f"   - density_level: 密度级别（用于颜色分类）")
            print(f"   - overlap_count: 重叠数量（用于标签）")
            print(f"   - hotspot_rank: 热点排名")
            
        else:
            print(f"\n⚠️ 没有执行任何导出操作")
            print(f"💡 使用 --help 查看可用选项")
        
        print(f"\n✅ 导出完成！成功执行 {success_count} 个操作")
        return 0
        
    except Exception as e:
        print(f"\n❌ 导出失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
