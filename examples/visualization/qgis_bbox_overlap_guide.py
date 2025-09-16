#!/usr/bin/env python3
"""
BBox叠置分析QGIS可视化指南
==========================

本指南展示如何将bbox叠置分析结果在QGIS中进行专业可视化。

主要功能：
- 自动运行叠置分析
- 创建QGIS兼容的视图
- 提供详细的可视化指导
- 生成示例样式配置

适用场景：
- 空间重叠热点分析
- 数据密度可视化
- 多数据集空间关系分析
- 质量检查和异常检测

工作流程：
1. 执行叠置分析
2. 准备QGIS数据源
3. 配置图层样式
4. 设置标签和过滤
5. 创建专题地图

使用方法：
    python examples/visualization/qgis_bbox_overlap_guide.py
    
    # 指定城市和参数
    python examples/visualization/qgis_bbox_overlap_guide.py --city beijing --demo-mode
"""

import sys
import os
from pathlib import Path
import argparse
from datetime import datetime
import json
import logging
from typing import Dict, List, Any, Optional

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QGISBBoxOverlapGuide:
    """QGIS BBox叠置分析可视化指南"""
    
    def __init__(self):
        """初始化指南"""
        self.analyzer = BBoxOverlapAnalyzer()
        self.connection_info = {
            'host': 'local_pg',
            'port': 5432,
            'database': 'postgres',
            'username': 'postgres',
            'password': 'postgres'
        }
    
    def run_demo_analysis(
        self, 
        city_filter: Optional[str] = None,
        analysis_id: Optional[str] = None
    ) -> str:
        """运行示例分析"""
        print("🚀 运行示例叠置分析...")
        
        if not analysis_id:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            analysis_id = f"demo_overlap_{timestamp}"
        
        try:
            # 确保环境准备就绪
            if not self.analyzer.ensure_unified_view():
                raise Exception("统一视图准备失败")
            
            if not self.analyzer.create_analysis_table():
                raise Exception("分析表创建失败")
            
            # 执行分析
            result_analysis_id = self.analyzer.run_overlap_analysis(
                analysis_id=analysis_id,
                city_filter=city_filter,
                min_overlap_area=0.0001,  # 较小的阈值，确保能找到结果
                top_n=15
            )
            
            # 创建QGIS视图
            if not self.analyzer.create_qgis_view(result_analysis_id):
                raise Exception("QGIS视图创建失败")
            
            print(f"✅ 示例分析完成：{result_analysis_id}")
            return result_analysis_id
            
        except Exception as e:
            print(f"❌ 示例分析失败：{str(e)}")
            raise
    
    def get_qgis_connection_guide(self) -> Dict[str, Any]:
        """获取QGIS连接指南"""
        return {
            'connection_steps': [
                "1. 打开QGIS Desktop",
                "2. 在浏览器面板中，右键点击 'PostgreSQL'",
                "3. 选择 '新建连接...'",
                "4. 输入连接参数（见下方）",
                "5. 点击 '测试连接' 验证",
                "6. 保存连接配置"
            ],
            'connection_params': self.connection_info,
            'important_notes': [
                "确保PostgreSQL服务正在运行",
                "确保防火墙允许5432端口连接",
                "如果连接失败，检查Docker容器状态"
            ]
        }
    
    def get_layer_loading_guide(self, analysis_id: str) -> Dict[str, Any]:
        """获取图层加载指南"""
        return {
            'recommended_layers': [
                {
                    'name': 'BBox底图',
                    'table': 'clips_bbox_unified_qgis',
                    'geometry_column': 'geometry',
                    'primary_key': 'qgis_id',
                    'purpose': '显示所有bbox作为底图背景',
                    'style_suggestions': {
                        'fill_color': 'lightblue',
                        'stroke_color': 'blue',
                        'opacity': 0.3,
                        'stroke_width': 0.5
                    }
                },
                {
                    'name': '重叠热点',
                    'table': 'qgis_bbox_overlap_hotspots',
                    'geometry_column': 'geometry',
                    'primary_key': 'qgis_id',
                    'purpose': '显示重叠热点区域',
                    'filter': f"analysis_id = '{analysis_id}'",
                    'style_suggestions': {
                        'style_type': 'categorized',
                        'classification_field': 'density_level',
                        'color_ramp': 'Reds'
                    }
                },
                {
                    'name': '热点详情',
                    'table': 'qgis_bbox_overlap_details',
                    'geometry_column': 'geometry',
                    'primary_key': 'qgis_id',
                    'purpose': '显示详细的热点信息',
                    'filter': f"analysis_id = '{analysis_id}'",
                    'style_suggestions': {
                        'style_type': 'graduated',
                        'classification_field': 'overlap_count',
                        'symbol_type': 'circle',
                        'size_range': [5, 25]
                    }
                }
            ],
            'loading_steps': [
                "1. 在QGIS中展开PostgreSQL连接",
                "2. 找到相应的表",
                "3. 双击表名或拖拽到地图画布",
                "4. 在弹出的对话框中：",
                "   - 选择几何列",
                "   - 选择主键列",
                "   - 设置过滤条件（如果需要）",
                "5. 点击 '添加' 完成加载"
            ]
        }
    
    def get_styling_guide(self) -> Dict[str, Any]:
        """获取样式设置指南"""
        return {
            'style_configurations': {
                'density_level_style': {
                    'description': '按密度等级分类显示',
                    'steps': [
                        "1. 右键图层 → 属性 → 符号系统",
                        "2. 选择 '分类'",
                        "3. 列选择 'density_level'",
                        "4. 点击 '分类' 按钮",
                        "5. 设置颜色方案：",
                        "   - Very High Density: 深红色",
                        "   - High Density: 红色",
                        "   - Medium Density: 橙色",
                        "   - Low Density: 黄色",
                        "   - Single Overlap: 浅黄色"
                    ],
                    'color_codes': {
                        'Very High Density': '#8B0000',
                        'High Density': '#DC143C',
                        'Medium Density': '#FF8C00',
                        'Low Density': '#FFD700',
                        'Single Overlap': '#FFFFE0'
                    }
                },
                'overlap_count_style': {
                    'description': '按重叠数量分级显示',
                    'steps': [
                        "1. 右键图层 → 属性 → 符号系统",
                        "2. 选择 '分级'",
                        "3. 列选择 'overlap_count'",
                        "4. 选择分级方法（建议：自然间断）",
                        "5. 设置类别数：5",
                        "6. 选择颜色渐变：红色系",
                        "7. 调整符号大小或透明度"
                    ]
                },
                'hotspot_score_style': {
                    'description': '按综合评分显示',
                    'steps': [
                        "1. 使用 'hotspot_score' 字段",
                        "2. 设置为分级符号",
                        "3. 使用圆形符号",
                        "4. 大小范围：5-30像素",
                        "5. 颜色从浅到深"
                    ]
                }
            },
            'advanced_styling': {
                'expression_based_styling': [
                    {
                        'name': '动态透明度',
                        'expression': 'overlap_count / 20.0 * 100',
                        'description': '根据重叠数量调整透明度'
                    },
                    {
                        'name': '大小表达式',
                        'expression': 'sqrt(overlap_count) * 3',
                        'description': '根据重叠数量平方根调整符号大小'
                    },
                    {
                        'name': '颜色混合',
                        'expression': 'color_mix_rgb(255,255,0, 255,0,0, overlap_count/20.0)',
                        'description': '从黄色到红色的动态颜色渐变'
                    }
                ]
            }
        }
    
    def get_labeling_guide(self) -> Dict[str, Any]:
        """获取标签设置指南"""
        return {
            'labeling_options': [
                {
                    'name': '重叠数量标签',
                    'field': 'overlap_count',
                    'format': '直接显示数值',
                    'positioning': '中心',
                    'font_size': 10,
                    'background': True
                },
                {
                    'name': '排名标签',
                    'field': 'rank_label',
                    'format': '例：Rank 1: 15 overlaps',
                    'positioning': '上方偏移',
                    'font_size': 8,
                    'background': False
                },
                {
                    'name': '面积标签',
                    'field': 'area_label',
                    'format': '例：0.0023 sq.deg',
                    'positioning': '下方偏移',
                    'font_size': 8,
                    'color': 'gray'
                }
            ],
            'expression_labels': [
                {
                    'name': '组合标签',
                    'expression': "'Rank ' || hotspot_rank || '\n' || overlap_count || ' overlaps'",
                    'description': '多行显示排名和重叠数'
                },
                {
                    'name': '条件标签',
                    'expression': "CASE WHEN overlap_count >= 10 THEN 'HIGH: ' || overlap_count ELSE overlap_count END",
                    'description': '高密度区域特殊标注'
                }
            ],
            'setup_steps': [
                "1. 右键图层 → 属性 → 标注",
                "2. 选择 '单一标注'",
                "3. 值来源选择字段或表达式",
                "4. 设置字体、大小、颜色",
                "5. 配置位置和偏移",
                "6. 添加背景或缓冲（可选）"
            ]
        }
    
    def get_analysis_workflow(self) -> Dict[str, Any]:
        """获取分析工作流程"""
        return {
            'workflow_steps': [
                {
                    'step': 1,
                    'title': '数据探索',
                    'description': '加载bbox底图，了解数据分布',
                    'actions': [
                        '加载 clips_bbox_unified_qgis 图层',
                        '查看属性表，了解数据结构',
                        '使用缩放工具浏览不同区域',
                        '按城市或数据集过滤数据'
                    ]
                },
                {
                    'step': 2,
                    'title': '热点识别',
                    'description': '加载重叠热点图层，识别高密度区域',
                    'actions': [
                        '加载 qgis_bbox_overlap_hotspots 图层',
                        '按密度等级设置颜色',
                        '识别最高密度的区域',
                        '记录关键热点的位置'
                    ]
                },
                {
                    'step': 3,
                    'title': '详细分析',
                    'description': '深入分析特定热点的详细信息',
                    'actions': [
                        '加载 qgis_bbox_overlap_details 图层',
                        '使用选择工具选择感兴趣的热点',
                        '查看属性表中的详细信息',
                        '分析涉及的数据集和场景'
                    ]
                },
                {
                    'step': 4,
                    'title': '空间关系分析',
                    'description': '分析热点的空间分布模式',
                    'actions': [
                        '使用缓冲区工具分析邻近关系',
                        '创建密度图显示分布模式',
                        '计算热点间的距离',
                        '分析与地理特征的关系'
                    ]
                },
                {
                    'step': 5,
                    'title': '结果展示',
                    'description': '创建专业的地图输出',
                    'actions': [
                        '使用打印布局器创建地图',
                        '添加图例、比例尺、北箭头',
                        '添加标题和说明文字',
                        '导出高质量的地图图像'
                    ]
                }
            ],
            'best_practices': [
                '始终检查数据的坐标系统',
                '使用适当的颜色方案提高可读性',
                '为不同的分析目的创建不同的项目文件',
                '定期保存项目避免数据丢失',
                '使用图层组织功能保持项目整洁'
            ]
        }
    
    def generate_style_files(self, output_dir: Path) -> Dict[str, str]:
        """生成QGIS样式文件"""
        style_files = {}
        
        # 密度等级样式XML
        density_style_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<qgis version="3.0">
  <renderer-v2 type="categorizedSymbol" attr="density_level">
    <categories>
      <category render="true" value="Very High Density" symbol="0" label="Very High Density"/>
      <category render="true" value="High Density" symbol="1" label="High Density"/>
      <category render="true" value="Medium Density" symbol="2" label="Medium Density"/>
      <category render="true" value="Low Density" symbol="3" label="Low Density"/>
      <category render="true" value="Single Overlap" symbol="4" label="Single Overlap"/>
    </categories>
    <symbols>
      <symbol type="fill" name="0"><layer><prop k="color" v="139,0,0,255"/></layer></symbol>
      <symbol type="fill" name="1"><layer><prop k="color" v="220,20,60,255"/></layer></symbol>
      <symbol type="fill" name="2"><layer><prop k="color" v="255,140,0,255"/></layer></symbol>
      <symbol type="fill" name="3"><layer><prop k="color" v="255,215,0,255"/></layer></symbol>
      <symbol type="fill" name="4"><layer><prop k="color" v="255,255,224,255"/></layer></symbol>
    </symbols>
  </renderer-v2>
</qgis>'''
        
        density_style_file = output_dir / "bbox_overlap_density_style.qml"
        with open(density_style_file, 'w', encoding='utf-8') as f:
            f.write(density_style_xml)
        style_files['density_style'] = str(density_style_file)
        
        return style_files
    
    def print_comprehensive_guide(self, analysis_id: str):
        """打印完整的使用指南"""
        print("\n" + "="*80)
        print("🎯 BBox叠置分析QGIS可视化完整指南")
        print("="*80)
        
        # 1. 连接指南
        print("\n📋 1. QGIS数据库连接设置")
        print("-" * 40)
        conn_guide = self.get_qgis_connection_guide()
        
        print("连接步骤:")
        for step in conn_guide['connection_steps']:
            print(f"   {step}")
        
        print(f"\n连接参数:")
        for key, value in conn_guide['connection_params'].items():
            print(f"   {key}: {value}")
        
        print(f"\n⚠️  重要提示:")
        for note in conn_guide['important_notes']:
            print(f"   • {note}")
        
        # 2. 图层加载指南
        print(f"\n📊 2. 图层加载指南")
        print("-" * 40)
        layer_guide = self.get_layer_loading_guide(analysis_id)
        
        print("推荐图层加载顺序:")
        for i, layer in enumerate(layer_guide['recommended_layers'], 1):
            print(f"\n   {i}. {layer['name']}")
            print(f"      表名: {layer['table']}")
            print(f"      主键: {layer['primary_key']}")
            print(f"      几何列: {layer['geometry_column']}")
            print(f"      用途: {layer['purpose']}")
            if 'filter' in layer:
                print(f"      过滤条件: {layer['filter']}")
        
        # 3. 样式设置指南
        print(f"\n🎨 3. 样式设置指南")
        print("-" * 40)
        style_guide = self.get_styling_guide()
        
        for style_name, style_config in style_guide['style_configurations'].items():
            print(f"\n   📌 {style_config['description']}")
            for step in style_config['steps']:
                print(f"      {step}")
        
        # 4. 标签设置指南
        print(f"\n🏷️  4. 标签设置指南")
        print("-" * 40)
        label_guide = self.get_labeling_guide()
        
        print("推荐标签配置:")
        for label_config in label_guide['labeling_options']:
            print(f"\n   • {label_config['name']}")
            print(f"     字段: {label_config['field']}")
            print(f"     格式: {label_config['format']}")
            print(f"     位置: {label_config['positioning']}")
        
        # 5. 分析工作流程
        print(f"\n🔍 5. 分析工作流程")
        print("-" * 40)
        workflow = self.get_analysis_workflow()
        
        for step_info in workflow['workflow_steps']:
            print(f"\n   步骤{step_info['step']}: {step_info['title']}")
            print(f"   {step_info['description']}")
            for action in step_info['actions']:
                print(f"      • {action}")
        
        # 6. 最佳实践
        print(f"\n💡 6. 最佳实践建议")
        print("-" * 40)
        for practice in workflow['best_practices']:
            print(f"   • {practice}")
        
        # 7. 快速开始
        print(f"\n🚀 7. 快速开始检查清单")
        print("-" * 40)
        print(f"   ✅ 数据库连接已配置")
        print(f"   ✅ 分析已完成: {analysis_id}")
        print(f"   ⬜ 加载 clips_bbox_unified_qgis 底图")
        print(f"   ⬜ 加载 qgis_bbox_overlap_hotspots 热点图层")
        print(f"   ⬜ 设置密度等级颜色")
        print(f"   ⬜ 添加重叠数量标签")
        print(f"   ⬜ 识别TOP 3热点区域")
        print(f"   ⬜ 分析涉及的数据集类型")
        
        print("\n" + "="*80)
        print("📖 完整指南已显示，现在可以开始在QGIS中进行可视化分析！")
        print("="*80)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='BBox叠置分析QGIS可视化指南')
    parser.add_argument('--city', help='城市过滤（用于演示）')
    parser.add_argument('--analysis-id', help='使用现有的分析ID')
    parser.add_argument('--demo-mode', action='store_true', help='运行演示模式（执行新的分析）')
    parser.add_argument('--style-output', help='样式文件输出目录')
    
    args = parser.parse_args()
    
    print("🎯 BBox叠置分析QGIS可视化指南")
    print("=" * 60)
    
    guide = QGISBBoxOverlapGuide()
    analysis_id = args.analysis_id
    
    try:
        # 如果是演示模式或没有指定分析ID，运行新的分析
        if args.demo_mode or not analysis_id:
            print("\n🚀 运行演示分析...")
            analysis_id = guide.run_demo_analysis(
                city_filter=args.city
            )
        
        # 生成样式文件（如果指定了输出目录）
        if args.style_output:
            output_dir = Path(args.style_output)
            output_dir.mkdir(exist_ok=True, parents=True)
            style_files = guide.generate_style_files(output_dir)
            print(f"\n📁 样式文件已生成:")
            for name, path in style_files.items():
                print(f"   {name}: {path}")
        
        # 显示完整指南
        guide.print_comprehensive_guide(analysis_id)
        
    except Exception as e:
        print(f"\n❌ 指南生成失败: {str(e)}")
        logger.exception("详细错误信息")


if __name__ == "__main__":
    main()
