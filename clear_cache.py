"""
清理缓存表工具
==============

用于清理或重置bbox_intersection_cache缓存表
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

def clear_cache_table():
    """清理缓存表"""
    print("🧹 清理缓存表工具")
    print("=" * 40)
    
    try:
        from src.spdatalab.fusion.spatial_join_production import (
            ProductionSpatialJoin, 
            SpatialJoinConfig
        )
        
        # 初始化
        config = SpatialJoinConfig(enable_cache_table=True)
        spatial_join = ProductionSpatialJoin(config)
        
        # 检查表是否存在
        from sqlalchemy import text
        with spatial_join.local_engine.connect() as conn:
            check_sql = text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = :table_name
                );
            """)
            table_exists = conn.execute(check_sql, {"table_name": config.intersection_table}).fetchone()[0]
        
        if not table_exists:
            print(f"❌ 缓存表 {config.intersection_table} 不存在")
            return
        
        # 获取当前记录数
        count = spatial_join._get_cached_count()
        print(f"📊 当前缓存记录数: {count}")
        
        if count == 0:
            print("✅ 缓存表已经是空的")
            return
        
        # 确认清理
        confirm = input(f"\n⚠️  确定要清理 {count} 条缓存记录吗？(y/N): ").strip().lower()
        
        if confirm in ['y', 'yes']:
            print("🧹 正在清理缓存...")
            spatial_join._clear_cache()
            
            # 验证清理结果
            new_count = spatial_join._get_cached_count()
            if new_count == 0:
                print("✅ 缓存清理完成！")
            else:
                print(f"⚠️  清理后仍有 {new_count} 条记录")
        else:
            print("❌ 取消清理操作")
            
    except Exception as e:
        print(f"❌ 清理失败: {e}")
        import traceback
        traceback.print_exc()

def drop_cache_table():
    """删除整个缓存表"""
    print("\n💥 删除缓存表工具")
    print("=" * 40)
    
    try:
        from src.spdatalab.fusion.spatial_join_production import (
            ProductionSpatialJoin, 
            SpatialJoinConfig
        )
        
        config = SpatialJoinConfig(enable_cache_table=False)  # 不自动创建
        spatial_join = ProductionSpatialJoin(config)
        
        # 确认删除
        confirm = input(f"⚠️  确定要删除整个缓存表 {config.intersection_table} 吗？(y/N): ").strip().lower()
        
        if confirm in ['y', 'yes']:
            from sqlalchemy import text
            
            with spatial_join.local_engine.connect() as conn:
                drop_sql = text(f"DROP TABLE IF EXISTS {config.intersection_table}")
                conn.execute(drop_sql)
                conn.commit()
                
            print("✅ 缓存表删除完成！")
            print("💡 下次使用时会自动重新创建")
        else:
            print("❌ 取消删除操作")
            
    except Exception as e:
        print(f"❌ 删除失败: {e}")

def main():
    """主菜单"""
    print("🛠️  缓存管理工具")
    print("=" * 50)
    print("1. 清理缓存数据（保留表结构）")
    print("2. 删除整个缓存表")
    print("3. 退出")
    
    while True:
        choice = input("\n请选择操作 (1-3): ").strip()
        
        if choice == '1':
            clear_cache_table()
            break
        elif choice == '2':
            drop_cache_table()
            break
        elif choice == '3':
            print("👋 再见！")
            break
        else:
            print("❌ 无效选择，请输入 1-3")

if __name__ == "__main__":
    main() 