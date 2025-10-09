#!/usr/bin/env python3
# STATUS: one_time - 测试JSON布尔值修复效果的验证脚本
"""
测试run_overlap_analysis.py中JSON布尔值修复效果
验证analysis_params字段的JSON格式是否正确
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from spdatalab.dataset.bbox import LOCAL_DSN
from sqlalchemy import create_engine, text
import json

def test_existing_analysis_params():
    """测试现有的analysis_params字段"""
    print("🔍 检查现有的analysis_params字段...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        # 查询最近的analysis_params
        check_sql = text("""
            SELECT 
                analysis_id,
                analysis_params,
                analysis_time
            FROM bbox_overlap_analysis_results 
            WHERE analysis_time::date = CURRENT_DATE
            ORDER BY analysis_time DESC
            LIMIT 5;
        """)
        
        try:
            results = conn.execute(check_sql).fetchall()
            
            if not results:
                print("❌ 没有找到今天的分析记录")
                return False
            
            print(f"📋 找到 {len(results)} 条今天的分析记录:")
            
            for i, row in enumerate(results, 1):
                print(f"\n{i}. Analysis ID: {row.analysis_id}")
                print(f"   Time: {row.analysis_time}")
                print(f"   Params: {row.analysis_params}")
                
                # 尝试解析JSON
                try:
                    params = json.loads(row.analysis_params)
                    print(f"   ✅ JSON解析成功")
                    print(f"   calculate_area: {params.get('calculate_area')} (类型: {type(params.get('calculate_area'))})")
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON解析失败: {str(e)}")
                    return False
            
            return True
            
        except Exception as e:
            print(f"❌ 查询失败: {str(e)}")
            return False

def test_json_parsing():
    """测试JSON解析功能"""
    print("\n🧪 测试JSON布尔值解析...")
    
    # 测试不同的布尔值格式
    test_cases = [
        ('{"calculate_area": false}', True, "小写false（正确）"),
        ('{"calculate_area": true}', True, "小写true（正确）"),
        ('{"calculate_area": False}', False, "大写False（错误）"),
        ('{"calculate_area": True}', False, "大写True（错误）"),
    ]
    
    for json_str, should_succeed, description in test_cases:
        try:
            result = json.loads(json_str)
            if should_succeed:
                print(f"   ✅ {description}: 解析成功 -> {result['calculate_area']}")
            else:
                print(f"   ⚠️ {description}: 意外成功（可能环境不同）")
        except json.JSONDecodeError as e:
            if not should_succeed:
                print(f"   ✅ {description}: 预期失败 -> {str(e)}")
            else:
                print(f"   ❌ {description}: 意外失败 -> {str(e)}")
    
    return True

def main():
    """主测试函数"""
    print("🧪 测试JSON布尔值修复效果")
    print("=" * 50)
    
    # 测试JSON解析规则
    json_test_ok = test_json_parsing()
    
    # 测试现有数据
    existing_data_ok = test_existing_analysis_params()
    
    print("\n" + "=" * 50)
    print("📋 测试结果:")
    print(f"   JSON解析规则: {'✅ 正常' if json_test_ok else '❌ 异常'}")
    print(f"   现有数据检查: {'✅ 正常' if existing_data_ok else '❌ 异常'}")
    
    if not existing_data_ok:
        print("\n🎯 建议:")
        print("   1. 先运行单个城市分析生成新的数据:")
        print("      cd examples/dataset/bbox_examples")
        print("      python run_overlap_analysis.py --city A72 --top-n 1")
        print("   2. 然后重新测试批量分析:")
        print("      python batch_top1_analysis.py --cities A72 --max-cities 1")
    else:
        print("\n🎯 建议:")
        print("   现有数据JSON格式正常，可以重新运行批量分析测试")
    
    return json_test_ok and existing_data_ok

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
