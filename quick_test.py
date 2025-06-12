#!/usr/bin/env python3
"""
快速诊断脚本 - 检查收费站分析功能的基本配置
"""

import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_local_db():
    """检查本地数据库"""
    print("🔗 检查本地数据库连接...")
    
    local_dsn = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    
    try:
        engine = create_engine(local_dsn, future=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()")).scalar()
            print(f"✅ 本地数据库连接成功")
            print(f"   版本: {result[:50]}...")
            return True
    except Exception as e:
        print(f"❌ 本地数据库连接失败: {e}")
        return False

def check_remote_db():
    """检查远程数据库"""
    print("\n🔗 检查远程数据库连接...")
    
    remote_dsn = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"
    
    try:
        engine = create_engine(remote_dsn, future=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()")).scalar()
            print(f"✅ 远程数据库连接成功")
            print(f"   版本: {result[:50]}...")
            return engine
    except Exception as e:
        print(f"❌ 远程数据库连接失败: {e}")
        print("💡 请检查:")
        print("   - 网络连接是否正常")
        print("   - 数据库服务器是否可访问")
        print("   - 用户名密码是否正确")
        return None

def check_tables(engine):
    """检查关键表"""
    print("\n📋 检查关键表...")
    
    tables_to_check = [
        "full_intersection",
        "public.ddi_data_points"
    ]
    
    results = {}
    
    try:
        with engine.connect() as conn:
            for table in tables_to_check:
                try:
                    # 检查表是否存在
                    if "." in table:
                        schema, table_name = table.split(".", 1)
                        check_sql = text("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = :schema AND table_name = :table_name
                            )
                        """)
                        exists = conn.execute(check_sql, {"schema": schema, "table_name": table_name}).scalar()
                    else:
                        check_sql = text("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = :table_name
                            )
                        """)
                        exists = conn.execute(check_sql, {"table_name": table}).scalar()
                    
                    if exists:
                        # 获取记录数
                        count_sql = text(f"SELECT COUNT(*) FROM {table}")
                        count = conn.execute(count_sql).scalar()
                        results[table] = count
                        print(f"✅ {table}: {count:,} 条记录")
                    else:
                        results[table] = None
                        print(f"❌ {table}: 表不存在")
                        
                except Exception as e:
                    results[table] = None
                    print(f"❌ {table}: 检查失败 - {e}")
            
    except Exception as e:
        print(f"❌ 检查表时出错: {e}")
        return {}
    
    return results

def check_toll_stations(engine):
    """检查收费站数据"""
    print("\n🏛️ 检查收费站数据...")
    
    try:
        with engine.connect() as conn:
            # 检查intersection类型分布
            type_sql = text("""
                SELECT intersectiontype, COUNT(*) as count 
                FROM full_intersection 
                WHERE intersectiontype IS NOT NULL
                GROUP BY intersectiontype 
                ORDER BY intersectiontype
            """)
            
            types = conn.execute(type_sql).fetchall()
            
            if types:
                print("📊 路口类型分布:")
                for row in types:
                    type_name = "收费站" if row[0] == 2 else f"类型{row[0]}"
                    print(f"   {type_name}: {row[1]:,} 个")
                
                # 特别检查收费站
                toll_count = sum(row[1] for row in types if row[0] == 2)
                if toll_count > 0:
                    print(f"\n🎯 找到 {toll_count} 个收费站 (intersectiontype=2)")
                    return toll_count
                else:
                    print("\n⚠️ 没有找到收费站数据 (intersectiontype=2)")
                    return 0
            else:
                print("❌ 没有找到任何路口数据")
                return 0
                
    except Exception as e:
        print(f"❌ 检查收费站数据失败: {e}")
        return 0

def main():
    """主函数"""
    print("🚀 收费站分析功能快速诊断")
    print("=" * 50)
    
    # 检查本地数据库
    local_ok = check_local_db()
    
    # 检查远程数据库
    remote_engine = check_remote_db()
    
    if not remote_engine:
        print("\n❌ 无法继续检查，远程数据库连接失败")
        return 1
    
    # 检查关键表
    table_results = check_tables(remote_engine)
    
    # 检查收费站数据
    toll_count = 0
    if table_results.get("full_intersection"):
        toll_count = check_toll_stations(remote_engine)
    
    # 总结
    print("\n" + "=" * 50)
    print("📊 诊断结果:")
    print(f"   本地数据库: {'✅' if local_ok else '❌'}")
    print(f"   远程数据库: {'✅' if remote_engine else '❌'}")
    print(f"   full_intersection表: {'✅' if table_results.get('full_intersection') else '❌'}")
    print(f"   ddi_data_points表: {'✅' if table_results.get('public.ddi_data_points') else '❌'}")
    print(f"   收费站数据: {toll_count} 个")
    
    if toll_count > 0:
        print("\n🎉 基本配置正常，可以进行收费站分析！")
        return 0
    else:
        print("\n⚠️ 缺少关键数据或配置，请检查以上问题")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 