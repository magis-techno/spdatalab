#!/usr/bin/env python3
# STATUS: one_time - 测试表结构修复效果的验证脚本
"""
测试city_top1_hotspots表结构修复效果
验证analysis_id字段问题是否解决
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from spdatalab.dataset.bbox import LOCAL_DSN
from sqlalchemy import create_engine, text

def test_table_structure():
    """测试目标表结构"""
    print("🔍 检查 city_top1_hotspots 表结构...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        table_name = 'city_top1_hotspots'
        
        # 检查表是否存在
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        table_exists = conn.execute(check_table_sql).scalar()
        
        if not table_exists:
            print(f"❌ {table_name} 表不存在")
            return False
        
        print(f"✅ {table_name} 表存在")
        
        # 检查表结构
        columns_sql = text(f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = 'public'
            ORDER BY ordinal_position;
        """)
        
        columns_result = conn.execute(columns_sql)
        columns = list(columns_result)
        
        print(f"\n📋 表结构 ({len(columns)} 个字段):")
        for col in columns:
            print(f"   {col.column_name}: {col.data_type}")
        
        # 检查关键字段
        column_names = [col.column_name for col in columns]
        
        required_fields = ['city_id', 'analysis_id', 'bbox_count', 'subdataset_count', 
                          'scene_count', 'total_overlap_area', 'geometry', 'grid_coords']
        missing_fields = [field for field in required_fields if field not in column_names]
        
        if missing_fields:
            print(f"\n⚠️ 缺少关键字段: {missing_fields}")
            return False
        else:
            print(f"\n✅ 所有关键字段都存在")
            return True

def test_create_function():
    """测试create_top1_summary_table函数"""
    print("\n🧪 测试create_top1_summary_table函数...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    # 导入修复后的函数
    sys.path.insert(0, str(Path(__file__).parent.parent / "dataset" / "bbox_examples"))
    from batch_top1_analysis import create_top1_summary_table
    
    with engine.connect() as conn:
        try:
            # 测试函数执行
            create_top1_summary_table(conn, 'city_top1_hotspots')
            print("✅ create_top1_summary_table 函数执行成功")
            return True
        except Exception as e:
            print(f"❌ create_top1_summary_table 函数执行失败: {str(e)}")
            return False

def main():
    """主测试函数"""
    print("🧪 测试表结构修复效果")
    print("=" * 50)
    
    # 测试表结构
    structure_ok = test_table_structure()
    
    # 测试创建函数
    function_ok = test_create_function()
    
    # 再次检查表结构
    structure_after_ok = test_table_structure()
    
    print("\n" + "=" * 50)
    print("📋 测试结果:")
    print(f"   初始表结构: {'✅ 正常' if structure_ok else '❌ 异常'}")
    print(f"   函数执行: {'✅ 成功' if function_ok else '❌ 失败'}")
    print(f"   修复后表结构: {'✅ 正常' if structure_after_ok else '❌ 异常'}")
    
    if structure_after_ok and function_ok:
        print("\n🎯 建议:")
        print("   可以重新运行 batch_top1_analysis.py 测试修复效果")
        print("   例如: python batch_top1_analysis.py --cities A72 --max-cities 1")
    else:
        print("\n⚠️ 仍有问题需要进一步排查")
    
    return structure_after_ok and function_ok

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
