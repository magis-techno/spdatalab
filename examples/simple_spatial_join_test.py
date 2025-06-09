"""
简化的空间连接测试示例
========================

专注于基本功能测试，避免使用可能不存在的字段
逐步验证：
1. 数据库连接
2. 获取bbox数据
3. 缓存功能
4. 基本查询分析
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin, 
    SpatialJoinConfig
)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_connection():
    """测试数据库连接"""
    print("🔌 测试数据库连接")
    print("-" * 40)
    
    try:
        config = SpatialJoinConfig(enable_cache_table=False)  # 先不启用缓存表
        spatial_join = ProductionSpatialJoin(config)
        
        # 测试本地数据库连接
        print("📊 测试本地数据库连接...")
        with spatial_join.local_engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            print(f"✅ 本地数据库连接成功: {result.fetchone()}")
        
        # 测试远程数据库连接
        print("🌐 测试远程数据库连接...")
        with spatial_join.remote_engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            print(f"✅ 远程数据库连接成功: {result.fetchone()}")
        
        return spatial_join
        
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return None

def test_bbox_data_structure():
    """测试bbox数据结构"""
    print("\n📋 测试bbox数据结构")
    print("-" * 40)
    
    try:
        config = SpatialJoinConfig(enable_cache_table=False)
        spatial_join = ProductionSpatialJoin(config)
        
        # 查看clips_bbox表结构
        print("🔍 查看clips_bbox表结构...")
        from sqlalchemy import text
        
        with spatial_join.local_engine.connect() as conn:
            # 检查表是否存在
            check_table = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'clips_bbox'
                );
            """)
            
            table_exists = conn.execute(check_table).fetchone()[0]
            
            if not table_exists:
                print("❌ clips_bbox表不存在")
                return None
            
            # 获取表结构
            structure_sql = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'clips_bbox'
                ORDER BY ordinal_position;
            """)
            
            columns = conn.execute(structure_sql).fetchall()
            print("📋 clips_bbox表结构:")
            for column in columns:
                nullable = "NULL" if column[2] == "YES" else "NOT NULL"
                print(f"  - {column[0]}: {column[1]} ({nullable})")
            
            # 获取样本数据
            sample_sql = text("SELECT * FROM clips_bbox LIMIT 3")
            sample_data = conn.execute(sample_sql).fetchall()
            
            print(f"\n📊 样本数据 ({len(sample_data)}条):")
            for i, row in enumerate(sample_data):
                print(f"  行{i+1}: {dict(row._mapping)}")
        
        return True
        
    except Exception as e:
        print(f"❌ bbox数据结构测试失败: {e}")
        logger.exception("详细错误信息:")
        return None

def test_intersection_data_structure():
    """测试路口数据结构"""
    print("\n🚦 测试路口数据结构")
    print("-" * 40)
    
    try:
        config = SpatialJoinConfig(enable_cache_table=False)
        spatial_join = ProductionSpatialJoin(config)
        
        from sqlalchemy import text
        
        with spatial_join.remote_engine.connect() as conn:
            # 检查表是否存在
            check_table = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'full_intersection'
                );
            """)
            
            table_exists = conn.execute(check_table).fetchone()[0]
            
            if not table_exists:
                print("❌ full_intersection表不存在")
                return None
            
            # 获取表结构
            structure_sql = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'full_intersection'
                ORDER BY ordinal_position;
            """)
            
            columns = conn.execute(structure_sql).fetchall()
            print("📋 full_intersection表结构:")
            for column in columns:
                nullable = "NULL" if column[2] == "YES" else "NOT NULL"
                print(f"  - {column[0]}: {column[1]} ({nullable})")
            
            # 获取样本数据
            sample_sql = text("SELECT * FROM full_intersection LIMIT 3")
            sample_data = conn.execute(sample_sql).fetchall()
            
            print(f"\n📊 样本数据 ({len(sample_data)}条):")
            for i, row in enumerate(sample_data):
                row_dict = dict(row._mapping)
                # 截断geometry字段显示
                if 'wkb_geometry' in row_dict and row_dict['wkb_geometry']:
                    row_dict['wkb_geometry'] = str(row_dict['wkb_geometry'])[:50] + "..."
                print(f"  行{i+1}: {row_dict}")
        
        return True
        
    except Exception as e:
        print(f"❌ 路口数据结构测试失败: {e}")
        logger.exception("详细错误信息:")
        return None

def test_simple_bbox_query():
    """测试简单的bbox查询（不使用city过滤）"""
    print("\n📦 测试简单bbox查询")
    print("-" * 40)
    
    try:
        config = SpatialJoinConfig(enable_cache_table=False)
        spatial_join = ProductionSpatialJoin(config)
        
        # 修改_fetch_bbox_data方法，不使用city_filter
        from sqlalchemy import text
        
        print("🔍 获取bbox数据（不使用城市过滤）...")
        
        sql = text("""
            SELECT 
                scene_token,
                ST_AsText(geometry) as bbox_wkt
            FROM clips_bbox 
            ORDER BY scene_token
            LIMIT 5
        """)
        
        with spatial_join.local_engine.connect() as conn:
            import pandas as pd
            bbox_data = pd.read_sql(sql, conn)
        
        print(f"✅ 成功获取 {len(bbox_data)} 条bbox数据")
        print("\n📊 bbox数据预览:")
        for i, row in bbox_data.iterrows():
            wkt_preview = str(row['bbox_wkt'])[:50] + "..." if len(str(row['bbox_wkt'])) > 50 else str(row['bbox_wkt'])
            print(f"  {row['scene_token']}: {wkt_preview}")
        
        return bbox_data
        
    except Exception as e:
        print(f"❌ bbox查询失败: {e}")
        logger.exception("详细错误信息:")
        return None

def test_simple_intersection_query(bbox_data):
    """测试简单的相交查询"""
    print("\n🔗 测试简单相交查询")
    print("-" * 40)
    
    if bbox_data is None or bbox_data.empty:
        print("❌ 没有bbox数据，跳过相交查询测试")
        return None
    
    try:
        config = SpatialJoinConfig(enable_cache_table=False)
        spatial_join = ProductionSpatialJoin(config)
        
        # 只测试第一个bbox
        first_bbox = bbox_data.iloc[0]
        scene_token = str(first_bbox['scene_token'])
        bbox_wkt = str(first_bbox['bbox_wkt'])
        
        print(f"🎯 测试场景: {scene_token}")
        
        from sqlalchemy import text
        
        # 简单的相交查询
        intersection_sql = text(f"""
            SELECT 
                COUNT(*) as intersect_count
            FROM full_intersection 
            WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
        """)
        
        with spatial_join.remote_engine.connect() as conn:
            result = conn.execute(intersection_sql)
            count = result.fetchone()[0]
        
        print(f"✅ 场景 {scene_token} 与 {count} 个路口相交")
        
        if count > 0:
            # 获取详细相交信息
            detail_sql = text(f"""
                SELECT 
                    id as intersection_id,
                    intersection_type
                FROM full_intersection 
                WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
                LIMIT 5
            """)
            
            with spatial_join.remote_engine.connect() as conn:
                import pandas as pd
                details = pd.read_sql(detail_sql, conn)
            
            print(f"\n📋 相交路口详情 (前5个):")
            for _, row in details.iterrows():
                print(f"  - 路口ID: {row['intersection_id']}, 类型: {row['intersection_type']}")
        
        return count > 0
        
    except Exception as e:
        print(f"❌ 相交查询失败: {e}")
        logger.exception("详细错误信息:")
        return None

def test_cache_functionality():
    """测试缓存功能（简化版）"""
    print("\n💾 测试缓存功能")
    print("-" * 40)
    
    try:
        # 启用缓存表
        config = SpatialJoinConfig(enable_cache_table=True)
        spatial_join = ProductionSpatialJoin(config)
        
        print("✅ 缓存表初始化成功")
        
        # 测试基本缓存操作
        print("🧹 清理旧缓存...")
        spatial_join._clear_cache()
        
        print("📊 检查缓存计数...")
        count = spatial_join._get_cached_count()
        print(f"当前缓存记录数: {count}")
        
        return spatial_join
        
    except Exception as e:
        print(f"❌ 缓存功能测试失败: {e}")
        logger.exception("详细错误信息:")
        return None

def main():
    """主测试流程"""
    print("🌟 简化空间连接功能测试")
    print("=" * 60)
    
    # 测试1: 数据库连接
    spatial_join = test_database_connection()
    if not spatial_join:
        return
    
    # 测试2: bbox数据结构
    bbox_structure_ok = test_bbox_data_structure()
    if not bbox_structure_ok:
        return
    
    # 测试3: 路口数据结构
    intersection_structure_ok = test_intersection_data_structure()
    if not intersection_structure_ok:
        return
    
    # 测试4: 简单bbox查询
    bbox_data = test_simple_bbox_query()
    if bbox_data is None:
        return
    
    # 测试5: 简单相交查询
    intersection_ok = test_simple_intersection_query(bbox_data)
    if not intersection_ok:
        print("⚠️  相交查询可能有问题，但继续测试缓存功能")
    
    # 测试6: 缓存功能
    cache_spatial_join = test_cache_functionality()
    
    # 总结
    print(f"\n" + "=" * 60)
    print("📊 测试结果总结:")
    print(f"  ✅ 数据库连接: {'通过' if spatial_join else '失败'}")
    print(f"  ✅ bbox数据结构: {'通过' if bbox_structure_ok else '失败'}")
    print(f"  ✅ 路口数据结构: {'通过' if intersection_structure_ok else '失败'}")
    print(f"  ✅ bbox查询: {'通过' if bbox_data is not None else '失败'}")
    print(f"  ✅ 相交查询: {'通过' if intersection_ok else '可能有问题'}")
    print(f"  ✅ 缓存功能: {'通过' if cache_spatial_join else '失败'}")
    
    if all([spatial_join, bbox_structure_ok, intersection_structure_ok, bbox_data is not None]):
        print(f"\n🎉 基础功能测试基本通过！")
        print(f"下一步可以:")
        print(f"  1. 调试相交查询问题（如果存在）")
        print(f"  2. 测试缓存构建功能")
        print(f"  3. 逐步添加更复杂的分析功能")
    else:
        print(f"\n❌ 基础功能存在问题，请先解决基础配置")

if __name__ == "__main__":
    main() 