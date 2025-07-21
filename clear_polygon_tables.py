#!/usr/bin/env python3
"""清理现有的polygon分析表，以便重新创建为3D支持的表"""

from sqlalchemy import create_engine, text

LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def clear_polygon_tables():
    """删除现有的polygon分析表"""
    engine = create_engine(LOCAL_DSN, future=True)
    
    # 需要删除的表名
    tables_to_drop = [
        'polygon_analysis',
        'polygon_roads', 
        'polygon_intersections',
        'polygon_lanes'
    ]
    
    with engine.connect() as conn:
        for table_name in tables_to_drop:
            try:
                drop_sql = text(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                conn.execute(drop_sql)
                print(f"✓ 删除表: {table_name}")
            except Exception as e:
                print(f"✗ 删除表失败 {table_name}: {e}")
        
        conn.commit()
        print("所有polygon分析表已清理完成")

if __name__ == "__main__":
    clear_polygon_tables() 