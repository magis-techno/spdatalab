#!/usr/bin/env python3
"""
调试版本测试 - 输出详细的调试信息
"""

import sys
from pathlib import Path
import logging

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from spdatalab.fusion.spatial_join import SpatialJoin, SpatialRelation
from sqlalchemy import text

# 配置详细日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def debug_test():
    """详细调试测试"""
    print("🔍 开始详细调试测试...")
    
    try:
        # 创建连接器
        joiner = SpatialJoin()
        print("✅ 连接器创建成功")
        
        # 测试连接
        print("📡 测试数据库连接...")
        with joiner.remote_engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
            print(f"远端数据库版本: {version}")
        
        # 获取一个最小的城市进行测试
        print("🔍 查找测试城市...")
        with joiner.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT city_id, COUNT(*) as count
                FROM clips_bbox 
                GROUP BY city_id 
                ORDER BY count ASC 
                LIMIT 1
            """)).fetchone()
            
            test_city = result[0]
            city_count = result[1]
            
        print(f"🏙️  测试城市: {test_city} ({city_count}条记录)")
        
        # 获取这个城市的数据
        print("📥 获取本地数据...")
        local_batch = joiner._fetch_local_batch_by_city("clips_bbox", test_city, None)
        print(f"✅ 获取到 {len(local_batch)} 条本地数据")
        print(f"数据列: {local_batch.columns.tolist()}")
        print(f"示例数据：\n{local_batch.head(2)}")
        
        # 手动推送到远端
        print("🚀 推送数据到远端...")
        temp_table_name = f"debug_test_table_{test_city}".lower()  # 确保小写
        
        # 清理可能存在的表
        joiner._cleanup_remote_temp_table(temp_table_name)
        
        # 推送数据
        local_batch.to_postgis(
            temp_table_name,
            joiner.remote_engine,
            if_exists='replace',
            index=False
        )
        print(f"✅ 数据推送完成，表名: {temp_table_name}")
        
        # 验证表是否存在
        print("🔍 验证远端表...")
        with joiner.remote_engine.connect() as conn:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {temp_table_name}")).scalar()
                print(f"✅ 远端表创建成功，包含 {count} 条记录")
                
                # 检查表结构
                columns = conn.execute(text(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{temp_table_name}'
                """)).fetchall()
                print(f"表结构: {columns}")
                
            except Exception as e:
                print(f"❌ 表验证失败: {str(e)}")
                return
        
        # 测试简单的空间查询
        print("🔍 测试空间查询...")
        with joiner.remote_engine.connect() as conn:
            try:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) as total_scenes,
                           COUNT(CASE WHEN ST_Intersects(t.geometry, r.wkb_geometry) THEN 1 END) as intersecting_scenes
                    FROM {temp_table_name} t
                    LEFT JOIN full_intersection r ON ST_Intersects(t.geometry, r.wkb_geometry)
                """)).fetchone()
                
                print(f"✅ 空间查询成功:")
                print(f"   总场景数: {result[0]}")
                print(f"   相交场景数: {result[1]}")
                
            except Exception as e:
                print(f"❌ 空间查询失败: {str(e)}")
                return
        
        # 清理
        print("🧹 清理临时表...")
        joiner._cleanup_remote_temp_table(temp_table_name)
        print("✅ 清理完成")
        
        print("\n🎉 调试测试完全成功！")
        
    except Exception as e:
        print(f"❌ 调试测试失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_test() 