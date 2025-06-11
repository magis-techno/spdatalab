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
    create_table_for_subdataset,
    validate_table_name
)

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def test_table_name_generation():
    """测试表名生成逻辑"""
    print("=== 测试表名生成逻辑 ===")
    
    test_cases = [
        "GOD_E2E_DDI_340023_340024_330004_lane_change_early_325197_sub_ddi_2773412e2e_2025_05_18_10_56_18",
        "GOD_E2E_very_long_dataset_name_that_might_cause_issues_sub_ddi_hash123e2e_2025_01_01_12_00_00",
        "GOD_E2E_short_name_2025_05_18_11_07_59",
        "normal_short_name",
        # 新增：测试连续下划线问题
        "GOD_E2E_dataset__with__double__underscores_2025_01_01_12_00_00",
        "GOD_E2E_name___with___triple___underscores",
        "dataset____multiple____underscores____problem",
        "GOD_E2E_mixed__single_and__double___underscores_sub_ddi_hash_2025_05_18_10_56_18"
    ]
    
    for i, original_name in enumerate(test_cases, 1):
        print(f"\n{i}. 测试: {original_name}")
        print(f"   长度: {len(original_name)}")
        
        # 规范化
        normalized = normalize_subdataset_name(original_name)
        print(f"   规范化: {normalized}")
        
        # 生成表名
        table_name = get_table_name_for_subdataset(original_name)
        print(f"   表名: {table_name}")
        print(f"   表名长度: {len(table_name)}")
        
        # 验证表名
        validation = validate_table_name(table_name)
        if validation['valid']:
            print(f"   ✅ 表名验证通过")
        else:
            print(f"   ❌ 表名验证失败: {', '.join(validation['issues'])}")

def test_single_table_creation(test_name):
    """测试单个表的创建"""
    print(f"\n=== 测试表创建: {test_name} ===")
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 生成表名
        table_name = get_table_name_for_subdataset(test_name)
        print(f"将要创建的表名: {table_name}")
        
        # 先清理可能存在的表
        with eng.connect() as conn:
            drop_sql = text(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            conn.execute(drop_sql)
            conn.commit()
            print(f"已清理可能存在的表: {table_name}")
        
        # 尝试创建
        print("开始创建表...")
        success, created_table_name = create_table_for_subdataset(eng, test_name)
        
        if success:
            print(f"✅ 表创建成功: {created_table_name}")
            
            # 验证表结构
            with eng.connect() as conn:
                check_sql = text(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    ORDER BY ordinal_position;
                """)
                result = conn.execute(check_sql)
                columns = result.fetchall()
                
                print(f"✅ 表结构验证: {len(columns)} 列")
                for col_name, data_type in columns:
                    print(f"   - {col_name}: {data_type}")
                    
        else:
            print(f"❌ 表创建失败: {created_table_name}")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ 测试过程出错: {str(e)}")
        return False

def main():
    """主函数"""
    # 测试表名生成
    test_table_name_generation()
    
    # 测试问题表名的创建
    problematic_names = [
        "GOD_E2E_DDI_340023_340024_330004_lane_change_early_325197_sub_ddi_2773412e2e_2025_05_18_10_56_18",
        "GOD_E2E_short_test",
        "GOD_E2E_medium_length_name_sub_ddi_hash_2025_01_01_12_00_00"
    ]
    
    print("\n" + "="*60)
    print("开始测试表创建")
    
    success_count = 0
    for name in problematic_names:
        if test_single_table_creation(name):
            success_count += 1
    
    print(f"\n=== 测试结果 ===")
    print(f"成功: {success_count}/{len(problematic_names)}")
    
    if success_count == len(problematic_names):
        print("🎉 所有表创建测试通过！")
    else:
        print("⚠️  部分表创建失败，但现在表名长度已经限制在安全范围内")

if __name__ == "__main__":
    main() 