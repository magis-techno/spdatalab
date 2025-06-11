#!/usr/bin/env python3
"""
调试表创建问题的脚本
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from sqlalchemy import create_engine, text
from src.spdatalab.dataset.bbox import (
    normalize_subdataset_name,
    get_table_name_for_subdataset,
    create_table_for_subdataset
)

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def debug_table_creation():
    # 测试问题的原始名称
    problematic_name = "GOD_E2E_DDI_340023_340024_330004_lane_change_early_325197_sub_ddi_2773412e2e_2025_05_18_10_56_18"
    
    print("=== 调试表创建问题 ===")
    print(f"原始名称: {problematic_name}")
    
    # 步骤1: 名称规范化
    normalized = normalize_subdataset_name(problematic_name)
    print(f"规范化后: {normalized}")
    
    # 步骤2: 生成表名
    table_name = get_table_name_for_subdataset(problematic_name)
    print(f"最终表名: {table_name}")
    print(f"表名长度: {len(table_name)}")
    
    # 步骤3: 尝试创建表
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 先清理可能存在的表
        with eng.connect() as conn:
            drop_sql = text(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            conn.execute(drop_sql)
            conn.commit()
            print(f"已清理可能存在的表: {table_name}")
        
        # 尝试创建
        print("\n开始创建表...")
        success, created_table_name = create_table_for_subdataset(eng, problematic_name)
        
        if success:
            print(f"✅ 表创建成功: {created_table_name}")
            
            # 验证表是否真的存在
            with eng.connect() as conn:
                check_sql = text(f"""
                    SELECT table_name, column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    ORDER BY ordinal_position;
                """)
                result = conn.execute(check_sql)
                columns = result.fetchall()
                
                if columns:
                    print(f"✅ 表结构验证成功，共 {len(columns)} 列")
                    for col_name, col_type, data_type in columns:
                        print(f"   {col_name}: {data_type}")
                else:
                    print("❌ 表结构验证失败，表不存在或无列")
                    
        else:
            print(f"❌ 表创建失败: {created_table_name}")
            
    except Exception as e:
        print(f"❌ 调试过程出错: {str(e)}")

if __name__ == "__main__":
    debug_table_creation() 