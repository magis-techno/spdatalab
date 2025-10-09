#!/usr/bin/env python3
# STATUS: one_time - 测试batch_top1_analysis.py修复效果的验证脚本
"""
测试batch_top1_analysis.py修复效果
验证analysis_id字段问题是否解决
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from spdatalab.dataset.bbox import LOCAL_DSN
from sqlalchemy import create_engine, text

def test_source_table_structure():
    """测试源表结构"""
    print("🔍 检查 bbox_overlap_analysis_results 表结构...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        # 检查表是否存在
        check_table_sql = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'bbox_overlap_analysis_results'
            );
        """)
        
        table_exists = conn.execute(check_table_sql).scalar()
        
        if not table_exists:
            print("❌ bbox_overlap_analysis_results 表不存在")
            return False
        
        print("✅ bbox_overlap_analysis_results 表存在")
        
        # 检查表结构
        columns_sql = text("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = 'bbox_overlap_analysis_results' 
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
        
        required_fields = ['analysis_id', 'overlap_count', 'hotspot_rank', 'geometry']
        missing_fields = [field for field in required_fields if field not in column_names]
        
        if missing_fields:
            print(f"\n⚠️ 缺少关键字段: {missing_fields}")
            return False
        else:
            print(f"\n✅ 所有关键字段都存在")
            return True

def test_data_availability():
    """测试数据可用性"""
    print("\n🔍 检查数据可用性...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        # 检查是否有今天的数据
        data_sql = text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE analysis_time::date = CURRENT_DATE) as today_records,
                COUNT(DISTINCT analysis_params::json->>'city_filter') as cities_count
            FROM bbox_overlap_analysis_results
            WHERE hotspot_rank = 1;
        """)
        
        try:
            result = conn.execute(data_sql).fetchone()
            
            print(f"📊 数据统计:")
            print(f"   总记录数: {result.total_records}")
            print(f"   今日记录数: {result.today_records}")
            print(f"   城市数量: {result.cities_count}")
            
            if result.total_records > 0:
                print("✅ 有可用数据")
                return True
            else:
                print("⚠️ 暂无数据，需要先运行分析")
                return False
                
        except Exception as e:
            print(f"❌ 查询数据失败: {str(e)}")
            return False

def main():
    """主测试函数"""
    print("🧪 测试 batch_top1_analysis.py 修复效果")
    print("=" * 50)
    
    # 测试表结构
    structure_ok = test_source_table_structure()
    
    # 测试数据可用性
    data_ok = test_data_availability()
    
    print("\n" + "=" * 50)
    print("📋 测试结果:")
    print(f"   表结构: {'✅ 正常' if structure_ok else '❌ 异常'}")
    print(f"   数据可用性: {'✅ 有数据' if data_ok else '⚠️ 无数据'}")
    
    if structure_ok:
        print("\n🎯 建议:")
        if data_ok:
            print("   可以直接运行 batch_top1_analysis.py 测试修复效果")
        else:
            print("   先运行单个城市分析生成数据，再测试批量分析")
            print("   例如: python run_overlap_analysis.py --city A72 --top-n 1")
    else:
        print("\n⚠️ 需要先修复表结构问题")
    
    return structure_ok

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
