#!/usr/bin/env python3
"""
QGIS集成验证脚本

验证分表模式下QGIS的连接、查询和可视化功能
"""

import argparse
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd

def test_database_connectivity(dsn):
    """测试数据库连接"""
    print("🔗 测试数据库连接...")
    
    try:
        eng = create_engine(dsn, future=True)
        with eng.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.scalar()
            print(f"✅ 数据库连接成功")
            print(f"   PostgreSQL版本: {version}")
            
            # 测试PostGIS扩展
            result = conn.execute(text("SELECT PostGIS_Version();"))
            postgis_version = result.scalar()
            print(f"   PostGIS版本: {postgis_version}")
            
        return True
    except Exception as e:
        print(f"❌ 数据库连接失败: {str(e)}")
        return False

def list_bbox_tables_for_qgis(eng):
    """列出所有可用于QGIS的bbox表"""
    print("\n📋 列出可用的bbox表...")
    
    try:
        # 列出所有分表
        tables_sql = text("""
            SELECT table_name, 
                   pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'clips_bbox%'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        with eng.connect() as conn:
            result = conn.execute(tables_sql)
            tables = result.fetchall()
            
            if tables:
                print(f"✅ 发现 {len(tables)} 个bbox表:")
                for table_name, size in tables:
                    print(f"   📊 {table_name}: {size}")
            else:
                print("❌ 没有发现bbox表")
                return False
            
        # 列出所有视图
        views_sql = text("""
            SELECT table_name, 
                   view_definition
            FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'clips_bbox%'
            ORDER BY table_name;
        """)
        
        with eng.connect() as conn:
            result = conn.execute(views_sql)
            views = result.fetchall()
            
            if views:
                print(f"\n🔍 发现 {len(views)} 个bbox视图:")
                for view_name, _ in views:
                    print(f"   👁️  {view_name}")
            
        return True
        
    except Exception as e:
        print(f"❌ 列出表失败: {str(e)}")
        return False

def test_unified_view_query(eng, view_name='clips_bbox_unified'):
    """测试统一视图查询功能"""
    print(f"\n🔍 测试统一视图查询: {view_name}")
    
    try:
        # 检查视图是否存在
        check_view_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = '{view_name}'
            );
        """)
        
        with eng.connect() as conn:
            result = conn.execute(check_view_sql)
            view_exists = result.scalar()
            
            if not view_exists:
                print(f"❌ 统一视图 {view_name} 不存在")
                return False
            
            # 测试基本查询
            basic_query = text(f"SELECT COUNT(*) as total_records FROM {view_name};")
            result = conn.execute(basic_query)
            total_records = result.scalar()
            print(f"✅ 统一视图查询成功，总记录数: {total_records:,}")
            
            # 测试子数据集分组查询
            subdataset_query = text(f"""
                SELECT subdataset_name, 
                       COUNT(*) as record_count,
                       source_table
                FROM {view_name} 
                GROUP BY subdataset_name, source_table
                ORDER BY record_count DESC
                LIMIT 10;
            """)
            
            result = conn.execute(subdataset_query)
            subdatasets = result.fetchall()
            
            if subdatasets:
                print(f"\n📊 按子数据集统计（前10个）:")
                for subdataset_name, record_count, source_table in subdatasets:
                    print(f"   📂 {subdataset_name}: {record_count:,} 条 (表: {source_table})")
            
            # 测试空间查询
            spatial_query = text(f"""
                SELECT COUNT(*) as records_with_geometry
                FROM {view_name} 
                WHERE geometry IS NOT NULL;
            """)
            
            result = conn.execute(spatial_query)
            spatial_records = result.scalar()
            print(f"\n🗺️  包含几何数据的记录: {spatial_records:,}")
            
            # 测试边界框查询
            bbox_query = text(f"""
                SELECT 
                    ST_XMin(ST_Extent(geometry)) as min_x,
                    ST_YMin(ST_Extent(geometry)) as min_y,
                    ST_XMax(ST_Extent(geometry)) as max_x,
                    ST_YMax(ST_Extent(geometry)) as max_y
                FROM {view_name} 
                WHERE geometry IS NOT NULL;
            """)
            
            result = conn.execute(bbox_query)
            bbox = result.fetchone()
            if bbox:
                min_x, min_y, max_x, max_y = bbox
                print(f"📍 数据范围: X({min_x:.6f}, {max_x:.6f}) Y({min_y:.6f}, {max_y:.6f})")
            
        return True
        
    except Exception as e:
        print(f"❌ 统一视图查询失败: {str(e)}")
        return False

def test_qgis_compatible_queries(eng):
    """测试QGIS兼容的查询"""
    print(f"\n🎨 测试QGIS兼容查询...")
    
    qgis_queries = [
        {
            "name": "基础几何查询",
            "sql": """
                SELECT id, scene_token, data_name, subdataset_name, 
                       source_table, all_good, geometry
                FROM clips_bbox_unified 
                WHERE geometry IS NOT NULL 
                LIMIT 100;
            """
        },
        {
            "name": "按子数据集过滤",
            "sql": """
                SELECT id, scene_token, data_name, geometry
                FROM clips_bbox_unified 
                WHERE subdataset_name LIKE '%lane_change%'
                AND geometry IS NOT NULL 
                LIMIT 50;
            """
        },
        {
            "name": "空间范围查询",
            "sql": """
                SELECT id, scene_token, data_name, geometry
                FROM clips_bbox_unified 
                WHERE ST_Intersects(
                    geometry, 
                    ST_MakeEnvelope(-122.5, 37.7, -122.3, 37.8, 4326)
                )
                LIMIT 50;
            """
        },
        {
            "name": "有效数据过滤",
            "sql": """
                SELECT id, scene_token, data_name, geometry
                FROM clips_bbox_unified 
                WHERE all_good = true 
                AND geometry IS NOT NULL
                LIMIT 50;
            """
        }
    ]
    
    success_count = 0
    
    for query_info in qgis_queries:
        try:
            with eng.connect() as conn:
                result = conn.execute(text(query_info["sql"]))
                rows = result.fetchall()
                print(f"✅ {query_info['name']}: 返回 {len(rows)} 条记录")
                success_count += 1
        except Exception as e:
            print(f"❌ {query_info['name']}: 查询失败 - {str(e)}")
    
    print(f"\n📊 QGIS兼容查询测试: {success_count}/{len(qgis_queries)} 通过")
    return success_count == len(qgis_queries)

def generate_qgis_connection_guide(dsn, view_name='clips_bbox_unified'):
    """生成QGIS连接指南"""
    print(f"\n📖 生成QGIS连接指南...")
    
    # 解析DSN
    try:
        import re
        match = re.match(r'postgresql\+psycopg://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', dsn)
        if match:
            username, password, host, port, database = match.groups()
        else:
            print("❌ 无法解析DSN格式")
            return
        
        guide_content = f"""
# QGIS连接配置指南

## 数据库连接参数
- **连接名称**: SPDataLab_BBox
- **主机**: {host}
- **端口**: {port}
- **数据库**: {database}
- **用户名**: {username}
- **密码**: {password}
- **SSL模式**: prefer

## 推荐的数据源

### 1. 统一视图（推荐）
- **表/视图名**: {view_name}
- **几何字段**: geometry
- **主键**: id, source_table
- **描述**: 包含所有分表数据的统一视图

### 2. 常用查询示例

#### 按子数据集过滤
```sql
SELECT * FROM {view_name} 
WHERE subdataset_name = 'your_subdataset_name'
```

#### 按有效性过滤
```sql
SELECT * FROM {view_name} 
WHERE all_good = true
```

#### 空间范围查询
```sql
SELECT * FROM {view_name} 
WHERE ST_Intersects(geometry, ST_MakeEnvelope(min_x, min_y, max_x, max_y, 4326))
```

## 样式建议
- **填充颜色**: 按 `subdataset_name` 分类着色
- **边框颜色**: 按 `all_good` 状态设置（绿色=有效，红色=无效）
- **透明度**: 50-70%

## 性能优化建议
1. 使用空间索引进行范围查询
2. 添加 `all_good = true` 过滤条件
3. 按子数据集分批加载大量数据
4. 使用适当的缩放级别显示详细信息

## 故障排除
- 如果连接失败，检查网络和防火墙设置
- 如果查询慢，添加适当的WHERE条件
- 如果几何数据不显示，检查坐标系设置(EPSG:4326)
        """
        
        guide_file = Path("qgis_connection_guide.md")
        guide_file.write_text(guide_content, encoding='utf-8')
        
        print(f"✅ QGIS连接指南已生成: {guide_file}")
        print(f"📍 连接信息: {username}@{host}:{port}/{database}")
        
    except Exception as e:
        print(f"❌ 生成QGIS连接指南失败: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="QGIS集成验证工具")
    parser.add_argument('--dsn', default='postgresql+psycopg://postgres:postgres@local_pg:5432/postgres', 
                       help='数据库连接字符串')
    parser.add_argument('--view-name', default='clips_bbox_unified', help='统一视图名称')
    parser.add_argument('--generate-guide', action='store_true', help='生成QGIS连接指南')
    
    args = parser.parse_args()
    
    print("🎨 QGIS集成验证开始")
    print("=" * 60)
    
    # 测试数据库连接
    if not test_database_connectivity(args.dsn):
        print("❌ 数据库连接失败，终止测试")
        return
    
    eng = create_engine(args.dsn, future=True)
    
    # 列出可用表
    if not list_bbox_tables_for_qgis(eng):
        print("❌ 没有可用的bbox表，终止测试")
        return
    
    # 测试统一视图查询
    if not test_unified_view_query(eng, args.view_name):
        print("❌ 统一视图查询失败")
    
    # 测试QGIS兼容查询
    if test_qgis_compatible_queries(eng):
        print("✅ QGIS兼容查询测试通过")
    else:
        print("❌ QGIS兼容查询测试失败")
    
    # 生成连接指南
    if args.generate_guide:
        generate_qgis_connection_guide(args.dsn, args.view_name)
    
    print("\n" + "="*60)
    print("✅ QGIS集成验证完成")
    print("""
🎯 下一步操作:
1. 打开QGIS桌面应用
2. 添加PostgreSQL数据源
3. 使用生成的连接参数
4. 加载clips_bbox_unified视图
5. 验证数据显示和查询功能
    """)

if __name__ == "__main__":
    main() 