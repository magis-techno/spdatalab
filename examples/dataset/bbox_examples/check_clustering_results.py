#!/usr/bin/env python3
"""
聚类结果检查脚本
===============

检查聚类参数对比的结果数据和视图

使用方法：
    python examples/dataset/bbox_examples/check_clustering_results.py --city A263
"""

import sys
from pathlib import Path
import argparse

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from sqlalchemy import create_engine, text

def check_clustering_results(city_id='A263'):
    """检查聚类结果"""
    
    print("🔍 检查聚类参数对比结果")
    print("=" * 50)
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        
        # 1. 检查基础表是否存在
        print(f"\n📋 1. 检查基础表...")
        table_check_sql = text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'clustering_parameter_comparison'
            ) as table_exists;
        """)
        
        result = conn.execute(table_check_sql).fetchone()
        if result.table_exists:
            print(f"   ✅ 表 clustering_parameter_comparison 存在")
        else:
            print(f"   ❌ 表 clustering_parameter_comparison 不存在")
            return
        
        # 2. 检查数据量
        print(f"\n📊 2. 检查数据量...")
        data_count_sql = text(f"""
            SELECT 
                city_id,
                method_name,
                eps_value,
                COUNT(*) as cluster_count,
                MAX(bbox_count) as max_cluster_size,
                MIN(bbox_count) as min_cluster_size
            FROM clustering_parameter_comparison 
            WHERE city_id = '{city_id}'
            GROUP BY city_id, method_name, eps_value
            ORDER BY method_name, eps_value;
        """)
        
        results = conn.execute(data_count_sql).fetchall()
        
        if not results:
            print(f"   ⚠️ 没有找到城市 {city_id} 的数据")
            return
        
        print(f"   📈 找到 {len(results)} 个参数组合的结果:")
        print(f"   {'方法':<12} {'eps值':<8} {'聚类数':<8} {'最大簇':<8} {'最小簇':<8}")
        print(f"   {'-'*50}")
        
        for row in results:
            print(f"   {row.method_name:<12} {row.eps_value:<8} {row.cluster_count:<8} {row.max_cluster_size:<8} {row.min_cluster_size:<8}")
        
        # 3. 检查视图是否存在
        print(f"\n🎨 3. 检查QGIS视图...")
        view_check_sql = text("""
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_name IN ('qgis_parameter_comparison', 'qgis_parameter_stats')
            ORDER BY table_name;
        """)
        
        views = conn.execute(view_check_sql).fetchall()
        
        expected_views = ['qgis_parameter_comparison', 'qgis_parameter_stats']
        found_views = [v.table_name for v in views]
        
        for view_name in expected_views:
            if view_name in found_views:
                print(f"   ✅ 视图 {view_name} 存在")
            else:
                print(f"   ❌ 视图 {view_name} 不存在")
        
        # 4. 检查视图数据
        if found_views:
            print(f"\n📋 4. 检查视图数据...")
            
            for view_name in found_views:
                try:
                    view_data_sql = text(f"SELECT COUNT(*) as count FROM {view_name};")
                    count_result = conn.execute(view_data_sql).fetchone()
                    print(f"   📊 {view_name}: {count_result.count} 条记录")
                except Exception as e:
                    print(f"   ❌ {view_name}: 查询失败 - {str(e)}")
        
        # 5. 生成QGIS连接信息
        print(f"\n🎯 5. QGIS使用指南:")
        print(f"   数据库连接信息:")
        print(f"   • 主机: localhost (或你的数据库主机)")
        print(f"   • 端口: 5432")
        print(f"   • 数据库: spdatalab (或你的数据库名)")
        print(f"   ")
        print(f"   可用图层:")
        for view_name in found_views:
            print(f"   • {view_name}")
        print(f"   ")
        print(f"   建议可视化设置:")
        print(f"   • qgis_parameter_comparison: 按 method_name 和 eps_value 分类")
        print(f"   • qgis_parameter_stats: 按 method_name 分组，eps_value 作为标签")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='检查聚类参数对比结果')
    parser.add_argument('--city', default='A263', help='检查的城市ID')
    
    args = parser.parse_args()
    
    try:
        check_clustering_results(args.city)
        print(f"\n✅ 检查完成！")
        
    except Exception as e:
        print(f"\n❌ 检查失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
