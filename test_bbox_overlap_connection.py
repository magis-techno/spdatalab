#!/usr/bin/env python3
"""
测试bbox叠置分析的数据库连接管理
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.dataset.bbox import LOCAL_DSN
    from sqlalchemy import create_engine, text
    import pandas as pd
    
    print("✅ 模块导入成功")
    
    # 测试连接
    engine = create_engine(LOCAL_DSN, future=True)
    
    # 测试时间估算查询
    view_name = "clips_bbox_unified_qgis"
    city_filter = "A263"
    
    conn = engine.connect()
    try:
        # 检查视图是否存在
        check_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = '{view_name}'
            );
        """)
        
        view_exists = conn.execute(check_sql).scalar()
        print(f"📊 统一视图存在: {view_exists}")
        
        if view_exists:
            # 执行时间估算
            where_condition = f"WHERE city_id = '{city_filter}'" if city_filter else "WHERE city_id IS NOT NULL"
            time_estimate_sql = f"""
            SELECT 
                COUNT(*) FILTER (WHERE all_good = true) as analyzable_count,
                CASE 
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 100000 THEN '⚠️ 很长 (>30分钟)'
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 50000 THEN '⏳ 较长 (10-30分钟)'
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 10000 THEN '⏰ 中等 (2-10分钟)'
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 1000 THEN '⚡ 较快 (<2分钟)'
                    ELSE '🚀 很快 (<30秒)'
                END as time_estimate,
                '{city_filter if city_filter else "全部城市"}' as scope
            FROM {view_name}
            {where_condition};
            """
            
            print(f"🔍 执行时间估算查询...")
            estimate_result = conn.execute(text(time_estimate_sql)).fetchone()
            
            print(f"📊 分析范围: {estimate_result.scope}")
            print(f"📈 可分析数据: {estimate_result.analyzable_count:,} 个bbox")
            print(f"⏱️ 预估时间: {estimate_result.time_estimate}")
            
            print("✅ 时间估算查询成功执行")
        else:
            print("❌ 统一视图不存在")
            
    finally:
        conn.close()
        print("✅ 数据库连接已关闭")
        
except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
