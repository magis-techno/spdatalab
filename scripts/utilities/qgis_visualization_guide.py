"""
QGIS可视化指南
===============

本指南展示如何将空间连接分析结果导出到数据库，并在QGIS中进行可视化。

工作流程：
1. 执行空间分析
2. 导出结果到PostgreSQL数据库
3. 在QGIS中连接数据库
4. 创建可视化图层
5. 设置样式和标签

支持的分析类型：
- intersection_type: 按路口类型分析
- intersection_subtype: 按路口子类型分析  
- scene_analysis: 按场景分析
- city_analysis: 按城市分析
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin,
    SpatialJoinConfig,
    build_cache,
    export_analysis_to_qgis,
    get_qgis_connection_info,
    get_available_cities,
    explain_intersection_types
)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def setup_qgis_visualization():
    """设置QGIS可视化环境"""
    
    print("🗺️ QGIS可视化设置指南")
    print("=" * 60)
    
    # 1. 获取连接信息
    print("\n1️⃣ 获取数据库连接信息")
    print("-" * 30)
    
    conn_info = get_qgis_connection_info()
    if 'error' in conn_info:
        print(f"❌ 连接信息获取失败: {conn_info['error']}")
        return
    
    print("📋 数据库连接信息:")
    print(f"   主机: {conn_info['host']}")
    print(f"   端口: {conn_info['port']}")  
    print(f"   数据库: {conn_info['database']}")
    print(f"   用户名: {conn_info['username']}")
    print(f"   分析结果表: {conn_info['results_table']}")
    
    # 2. 准备示例数据
    print("\n2️⃣ 准备分析数据")
    print("-" * 30)
    
    # 获取可用城市
    cities_df = get_available_cities()
    sample_city = None
    if not cities_df.empty and 'city_id' in cities_df.columns:
        sample_city = cities_df.iloc[0]['city_id']
        print(f"使用示例城市: {sample_city}")
    
    # 确保有缓存数据
    try:
        spatial_join = ProductionSpatialJoin()
        cached_count = spatial_join._get_cached_count(sample_city)
        
        if cached_count == 0:
            print("🔄 构建分析缓存...")
            build_cache(50, city_filter=sample_city)
        else:
            print(f"✅ 已有缓存数据: {cached_count} 条记录")
            
    except Exception as e:
        print(f"❌ 缓存检查失败: {e}")
        return
    
    # 3. 导出多种分析结果
    print("\n3️⃣ 导出分析结果到数据库")
    print("-" * 30)
    
    analysis_results = []
    
    try:
        # 路口类型分析
        print("📊 导出路口类型分析...")
        analysis_id1 = export_analysis_to_qgis(
            analysis_type="intersection_type",
            city_filter=sample_city,
            include_geometry=True
        )
        analysis_results.append(('路口类型', analysis_id1))
        
        # 路口子类型分析
        print("📊 导出路口子类型分析...")
        analysis_id2 = export_analysis_to_qgis(
            analysis_type="intersection_subtype",
            city_filter=sample_city, 
            include_geometry=True
        )
        analysis_results.append(('路口子类型', analysis_id2))
        
        # 场景分析（前20个最活跃的场景）
        print("📊 导出场景分析...")
        analysis_id3 = export_analysis_to_qgis(
            analysis_type="scene_analysis",
            city_filter=sample_city,
            include_geometry=True
        )
        analysis_results.append(('场景分析', analysis_id3))
        
        print("✅ 所有分析结果已导出!")
        
    except Exception as e:
        print(f"❌ 导出失败: {e}")
        return
    
    # 4. 生成QGIS连接指令
    print("\n4️⃣ QGIS连接指令")
    print("-" * 30)
    
    print("📝 复制以下信息到QGIS中:")
    print(f"""
🔗 PostgreSQL连接参数:
   主机名: {conn_info['host']}
   端口: {conn_info['port']}
   数据库: {conn_info['database']}
   用户名: {conn_info['username']}
   密码: {conn_info['password']}

📋 要连接的表:
   分析结果表: {conn_info['results_table']}
   原始缓存表: {conn_info['cache_table']} (可选)

🎨 导出的分析结果:""")
    
    for name, analysis_id in analysis_results:
        print(f"   - {name}: {analysis_id}")
    
    # 5. QGIS操作步骤
    print(f"\n5️⃣ QGIS操作步骤")
    print("-" * 30)
    
    print("""
📖 在QGIS中的操作步骤:

1. 打开QGIS
2. 在浏览器面板中，右键点击"PostgreSQL" → "新建连接"
3. 输入上面的连接参数
4. 测试连接并保存
5. 展开连接，找到表 '{table_name}'
6. 将表拖到地图画布中
7. 设置样式:
   - 右键图层 → 属性 → 符号系统
   - 选择"分类" 
   - 值选择 'group_value_name' 字段
   - 点击"分类"自动生成颜色
8. 设置标签:
   - 标签选项卡 → 启用标签
   - 标签内容选择 'group_value_name' 或 'intersection_count'

🎯 推荐可视化字段:
   - group_value_name: 路口类型名称 (用于符号化)
   - intersection_count: 相交数量 (用于符号大小)
   - unique_intersections: 唯一路口数 (用于标签)
   - analysis_time: 分析时间 (用于过滤)

🔍 过滤数据:
   - 属性表中可以按 analysis_id 过滤特定的分析
   - 按 analysis_type 过滤特定类型的分析
   - 按 city_filter 过滤特定城市的分析
""".format(table_name=conn_info['results_table']))
    
    # 6. 高级可视化建议
    print(f"\n6️⃣ 高级可视化建议")
    print("-" * 30)
    
    # 显示类型说明
    types_df = explain_intersection_types()
    print("📚 路口类型说明 (用于图例):")
    print(types_df.to_string(index=False))
    
    print(f"""
🎨 可视化建议:

1. 热力图可视化:
   - 使用 intersection_count 创建热力图
   - 颜色从冷色(少)到暖色(多)

2. 分级符号可视化:
   - 符号大小基于 intersection_count
   - 颜色基于 group_value_name
   
3. 多图层叠加:
   - 底图: 路口类型分析 (面积符号)
   - 叠加: 路口子类型分析 (点符号)
   
4. 时间序列分析:
   - 如果有多次分析，可按 analysis_time 制作动画
   
5. 统计图表:
   - 使用QGIS的图表功能创建柱状图
   - X轴: group_value_name
   - Y轴: intersection_count

💡 专业提示:
   - 使用表达式 'intersection_count' || ' intersections' 创建动态标签
   - 设置透明度让底层地图可见
   - 使用渐变色表现数据连续性
   - 添加图例说明路口类型含义
""")


def export_specific_analysis():
    """导出特定的分析结果"""
    
    print("\n🎯 导出特定分析")
    print("=" * 40)
    
    # 获取用户输入或使用默认值
    cities_df = get_available_cities()
    available_cities = cities_df['city_id'].tolist() if not cities_df.empty and 'city_id' in cities_df.columns else []
    
    if available_cities:
        print(f"可用城市: {available_cities[:5]}")
        city = available_cities[0]  # 使用第一个城市作为示例
    else:
        city = None
    
    # 分析类型选项
    analysis_options = {
        '1': ('intersection_type', '路口类型分析'),
        '2': ('intersection_subtype', '路口子类型分析'), 
        '3': ('scene_analysis', '场景分析'),
        '4': ('city_analysis', '城市分析')
    }
    
    print(f"\n📋 可用的分析类型:")
    for key, (_, name) in analysis_options.items():
        print(f"   {key}. {name}")
    
    # 执行所有分析类型作为示例
    for key, (analysis_type, name) in analysis_options.items():
        try:
            print(f"\n🔄 执行 {name}...")
            
            analysis_id = export_analysis_to_qgis(
                analysis_type=analysis_type,
                city_filter=city,
                include_geometry=True
            )
            
            print(f"✅ {name} 导出完成: {analysis_id}")
            
        except Exception as e:
            print(f"❌ {name} 导出失败: {e}")


if __name__ == "__main__":
    print("🌟 空间分析QGIS可视化指南")
    print("=" * 80)
    
    try:
        # 主要设置流程
        setup_qgis_visualization()
        
        # 导出特定分析
        export_specific_analysis()
        
        print("\n" + "=" * 80)
        print("🎉 QGIS可视化设置完成!")
        print("📌 现在您可以:")
        print("   1. 使用上述连接信息在QGIS中连接数据库")
        print("   2. 加载分析结果表进行可视化")
        print("   3. 根据建议设置样式和标签")
        print("   4. 创建专业的空间分析地图")
        
    except Exception as e:
        print(f"❌ 设置过程中出现错误: {e}")
        logger.exception("详细错误信息:") 