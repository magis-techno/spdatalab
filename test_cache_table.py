"""
测试缓存表创建脚本
用于验证 bbox_intersection_cache 表是否能正确创建
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin, 
    SpatialJoinConfig
)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_cache_table_creation():
    """测试缓存表创建"""
    
    print("🔧 测试缓存表创建")
    print("=" * 50)
    
    try:
        # 配置（启用缓存表）
        config = SpatialJoinConfig(
            enable_cache_table=True,
            intersection_table="bbox_intersection_cache"
        )
        
        print(f"📊 本地数据库: {config.local_dsn}")
        print(f"📊 缓存表名: {config.intersection_table}")
        
        # 初始化空间连接器（这会自动创建缓存表）
        print("\n🚀 初始化 ProductionSpatialJoin...")
        spatial_join = ProductionSpatialJoin(config)
        
        # 验证表是否创建成功
        print("\n🔍 验证表是否创建成功...")
        
        from sqlalchemy import text
        with spatial_join.local_engine.connect() as conn:
            # 检查表是否存在
            check_table_sql = text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = :table_name
                );
            """)
            
            result = conn.execute(check_table_sql, {"table_name": config.intersection_table})
            table_exists = result.fetchone()[0]
            
            if table_exists:
                print(f"✅ 表 {config.intersection_table} 创建成功！")
                
                # 获取表结构信息
                structure_sql = text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                    ORDER BY ordinal_position;
                """)
                
                columns = conn.execute(structure_sql, {"table_name": config.intersection_table}).fetchall()
                
                print("\n📋 表结构:")
                for column in columns:
                    nullable = "NULL" if column[2] == "YES" else "NOT NULL"
                    print(f"  - {column[0]}: {column[1]} ({nullable})")
                
                # 检查索引
                index_sql = text("""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE tablename = :table_name;
                """)
                
                indexes = conn.execute(index_sql, {"table_name": config.intersection_table}).fetchall()
                
                print(f"\n🔗 索引信息 ({len(indexes)} 个):")
                for index in indexes:
                    print(f"  - {index[0]}")
                
            else:
                print(f"❌ 表 {config.intersection_table} 创建失败！")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        logger.exception("详细错误信息:")
        return False

def test_basic_cache_operations():
    """测试基本缓存操作"""
    
    print(f"\n🧪 测试基本缓存操作")
    print("=" * 50)
    
    try:
        spatial_join = ProductionSpatialJoin()
        
        # 测试获取缓存计数（应该返回0）
        print("📊 测试缓存计数...")
        count = spatial_join._get_cached_count()
        print(f"当前缓存记录数: {count}")
        
        # 测试清理缓存（即使是空的也应该成功）
        print("🧹 测试缓存清理...")
        spatial_join._clear_cache()
        print("缓存清理完成")
        
        print("✅ 基本操作测试通过！")
        return True
        
    except Exception as e:
        print(f"❌ 基本操作测试失败: {e}")
        logger.exception("详细错误信息:")
        return False

def main():
    """主测试函数"""
    
    print("🌟 缓存表测试套件")
    print("=" * 60)
    
    # 测试1: 表创建
    test1_passed = test_cache_table_creation()
    
    # 测试2: 基本操作
    test2_passed = test_basic_cache_operations() if test1_passed else False
    
    # 测试结果
    print(f"\n" + "=" * 60)
    print("📊 测试结果:")
    print(f"  ✅ 表创建测试: {'通过' if test1_passed else '失败'}")
    print(f"  ✅ 基本操作测试: {'通过' if test2_passed else '失败'}")
    
    if test1_passed and test2_passed:
        print(f"\n🎉 所有测试通过！缓存表已准备就绪。")
        print(f"你现在可以使用以下功能:")
        print(f"  - build_cache() 构建相交关系缓存")
        print(f"  - analyze_cached_intersections() 进行分析")
    else:
        print(f"\n❌ 部分测试失败，请检查:")
        print(f"  1. local_pg 数据库是否运行")
        print(f"  2. 数据库连接配置是否正确")
        print(f"  3. 数据库用户是否有CREATE TABLE权限")

if __name__ == "__main__":
    main() 