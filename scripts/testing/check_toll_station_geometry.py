#!/usr/bin/env python3
"""
检查收费站的实际几何类型
"""

import sys
from pathlib import Path
from sqlalchemy import text

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.fusion.toll_station_analysis import TollStationAnalyzer
    print("✅ 模块导入成功")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

def main():
    """检查收费站几何类型"""
    print("🔍 检查收费站几何类型")
    print("-" * 40)
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 检查收费站几何类型
        check_sql = text("""
            SELECT 
                id,
                intersectiontype,
                ST_GeometryType(wkb_geometry) as geom_type,
                ST_AsText(wkb_geometry) as geom_text
            FROM full_intersection
            WHERE intersectiontype = 2
            AND wkb_geometry IS NOT NULL
            ORDER BY id
            LIMIT 5
        """)
        
        with analyzer.remote_engine.connect() as conn:
            results = conn.execute(check_sql).fetchall()
        
        print("收费站几何类型检查:")
        for result in results:
            print(f"  ID {result[0]}: {result[2]} - {result[3][:100]}...")
        
        # 统计几何类型分布
        stats_sql = text("""
            SELECT 
                ST_GeometryType(wkb_geometry) as geom_type,
                COUNT(*) as count
            FROM full_intersection
            WHERE intersectiontype = 2
            AND wkb_geometry IS NOT NULL
            GROUP BY ST_GeometryType(wkb_geometry)
            ORDER BY count DESC
        """)
        
        with analyzer.remote_engine.connect() as conn:
            stats = conn.execute(stats_sql).fetchall()
        
        print("\n几何类型统计:")
        for stat in stats:
            print(f"  {stat[0]}: {stat[1]} 个")
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 