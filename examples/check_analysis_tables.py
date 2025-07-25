"""
检查分析结果表状态
===================

用于诊断QGIS连接问题，检查：
1. 表是否存在
2. 表结构是否正确
3. 数据是否成功插入
4. 几何信息是否正确
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin,
    SpatialJoinConfig,
    get_qgis_connection_info
)
import pandas as pd
from sqlalchemy import create_engine, text

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_database_connection():
    """检查数据库连接"""
    print("🔗 检查数据库连接")
    print("-" * 40)
    
    try:
        config = SpatialJoinConfig()
        engine = create_engine(config.local_dsn)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()")).fetchone()
            print(f"✅ 数据库连接成功")
            print(f"   PostgreSQL版本: {result[0]}")
            return True
            
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False


def check_tables_exist():
    """检查表是否存在"""
    print("\n📋 检查表存在状态")
    print("-" * 40)
    
    config = SpatialJoinConfig()
    engine = create_engine(config.local_dsn)
    
    tables_to_check = [
        ('clips_bbox', '场景边界框表'),
        (config.intersection_table, '路口缓存表'),
        (config.analysis_results_table, '分析结果表')
    ]
    
    results = {}
    
    with engine.connect() as conn:
        for table_name, description in tables_to_check:
            try:
                check_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table_name}'
                    );
                """)
                exists = conn.execute(check_sql).fetchone()[0]
                
                if exists:
                    # 获取记录数
                    count_sql = text(f"SELECT COUNT(*) FROM {table_name}")
                    count = conn.execute(count_sql).fetchone()[0]
                    print(f"✅ {description} ({table_name}): 存在，{count} 条记录")
                    results[table_name] = {'exists': True, 'count': count}
                else:
                    print(f"❌ {description} ({table_name}): 不存在")
                    results[table_name] = {'exists': False, 'count': 0}
                    
            except Exception as e:
                print(f"❌ {description} ({table_name}): 检查失败 - {e}")
                results[table_name] = {'exists': False, 'count': 0, 'error': str(e)}
    
    return results


def check_table_structure():
    """检查分析结果表结构"""
    print("\n🏗️ 检查分析结果表结构")
    print("-" * 40)
    
    config = SpatialJoinConfig()
    engine = create_engine(config.local_dsn)
    
    try:
        with engine.connect() as conn:
            structure_sql = text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{config.analysis_results_table}'
                ORDER BY ordinal_position;
            """)
            
            columns = conn.execute(structure_sql).fetchall()
            
            if columns:
                print("📋 表结构:")
                for col in columns:
                    nullable = "可空" if col[2] == 'YES' else "非空"
                    print(f"   - {col[0]}: {col[1]} ({nullable})")
                return True
            else:
                print("❌ 表不存在或无权限访问")
                return False
                
    except Exception as e:
        print(f"❌ 检查表结构失败: {e}")
        return False


def check_sample_data():
    """检查样本数据"""
    print("\n📊 检查样本数据")
    print("-" * 40)
    
    config = SpatialJoinConfig()
    engine = create_engine(config.local_dsn)
    
    try:
        with engine.connect() as conn:
            # 先做一个简单的查询测试
            try:
                simple_sql = text(f"SELECT COUNT(*) FROM {config.analysis_results_table}")
                count = conn.execute(simple_sql).fetchone()[0]
                print(f"   表记录总数: {count}")
                
                if count == 0:
                    print("❌ 表中没有数据")
                    return False
            except Exception as e:
                print(f"❌ 无法查询表: {e}")
                return False
            
            # 首先检查字段是否存在
            columns_sql = text(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{config.analysis_results_table}'
                ORDER BY ordinal_position
            """)
            columns_result = conn.execute(columns_sql).fetchall()
            columns = [row[0] for row in columns_result]
            
            print(f"   表字段: {columns}")
            
            if not columns:
                print("❌ 表中没有字段或表不存在")
                return False
            
            # 构建查询，只使用存在的字段
            select_fields = []
            
            # 按优先级添加字段
            priority_fields = ['analysis_id', 'analysis_type', 'group_value_name', 'intersection_count']
            for field in priority_fields:
                if field in columns:
                    select_fields.append(field)
            
            # 添加几何检查
            if 'geometry' in columns:
                select_fields.append("""
                    CASE 
                        WHEN geometry IS NOT NULL THEN 'YES'
                        ELSE 'NO'
                    END as has_geometry
                """)
            
            # 如果没有常用字段，就选择前几个字段
            if not select_fields:
                select_fields = columns[:5]  # 取前5个字段
            
            # 确定排序字段
            order_field = None
            for possible_order in ['created_at', 'id', columns[0] if columns else 'analysis_id']:
                if possible_order in columns:
                    order_field = possible_order
                    break
            
            if not order_field:
                order_field = columns[0] if columns else 'analysis_id'
            
            sample_sql = text(f"""
                SELECT {', '.join(select_fields)}
                FROM {config.analysis_results_table}
                ORDER BY {order_field} DESC
                LIMIT 5
            """)
            
            samples = pd.read_sql(sample_sql, conn)
            
            if not samples.empty:
                print("📋 最新的5条记录:")
                print(samples.to_string(index=False))
                return True
            else:
                print("❌ 表中没有数据")
                return False
                
    except Exception as e:
        print(f"❌ 检查样本数据失败: {e}")
        return False


def check_geometry_data():
    """检查几何数据"""
    print("\n🗺️ 检查几何数据")
    print("-" * 40)
    
    config = SpatialJoinConfig()
    engine = create_engine(config.local_dsn)
    
    try:
        with engine.connect() as conn:
            # 检查geometry字段类型
            geom_type_sql = text(f"""
                SELECT udt_name FROM information_schema.columns
                WHERE table_name = '{config.analysis_results_table}' AND column_name = 'geometry'
            """)
            geom_type = conn.execute(geom_type_sql).fetchone()
            is_postgis = geom_type and geom_type[0] == 'geometry'
            
            if is_postgis:
                # PostGIS几何类型
                geom_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(geometry) as records_with_geometry,
                        COUNT(CASE WHEN geometry IS NOT NULL AND NOT ST_IsEmpty(geometry) THEN 1 END) as valid_geometry
                    FROM {config.analysis_results_table}
                """)
            else:
                # TEXT类型
                geom_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(geometry) as records_with_geometry,
                        COUNT(CASE WHEN geometry IS NOT NULL AND LENGTH(geometry) > 10 THEN 1 END) as valid_geometry
                    FROM {config.analysis_results_table}
                """)
            
            stats = conn.execute(geom_sql).fetchone()
            
            print(f"📊 几何数据统计:")
            print(f"   - 总记录数: {stats[0]}")
            print(f"   - 有几何信息的记录: {stats[1]}")
            print(f"   - 有效几何信息的记录: {stats[2]}")
            
            if stats[2] > 0:
                # 显示一个几何样本
                if is_postgis:
                    # PostGIS几何类型 - 转换为WKT显示
                    sample_geom_sql = text(f"""
                        SELECT LEFT(ST_AsText(geometry), 100) as geometry_sample,
                               ST_GeometryType(geometry) as geom_type
                        FROM {config.analysis_results_table}
                        WHERE geometry IS NOT NULL AND NOT ST_IsEmpty(geometry)
                        LIMIT 1
                    """)
                    sample = conn.execute(sample_geom_sql).fetchone()
                    if sample:
                        print(f"   - 几何类型: {sample[1]}")
                        print(f"   - 几何样本: {sample[0]}...")
                else:
                    # TEXT类型
                    sample_geom_sql = text(f"""
                        SELECT LEFT(geometry, 100) as geometry_sample
                        FROM {config.analysis_results_table}
                        WHERE geometry IS NOT NULL AND LENGTH(geometry) > 10
                        LIMIT 1
                    """)
                    sample = conn.execute(sample_geom_sql).fetchone()
                    if sample:
                        print(f"   - 几何样本: {sample[0]}...")
            
            return stats[2] > 0
                
    except Exception as e:
        print(f"❌ 检查几何数据失败: {e}")
        return False


def provide_qgis_guidance():
    """提供QGIS连接指导"""
    print("\n🎯 QGIS连接指导")
    print("-" * 40)
    
    conn_info = get_qgis_connection_info()
    
    if 'error' not in conn_info:
        print("📝 QGIS连接参数:")
        print(f"   主机: {conn_info['host']}")
        print(f"   端口: {conn_info['port']}")
        print(f"   数据库: {conn_info['database']}")
        print(f"   用户名: {conn_info['username']}")
        print(f"   密码: {conn_info['password']}")
        
        print(f"\n📋 要查看的表:")
        print(f"   - {conn_info['results_table']} (分析结果表)")
        print(f"   - {conn_info['cache_table']} (缓存表)")
        
        print(f"\n🔧 QGIS操作步骤:")
        print("1. 在QGIS中添加PostgreSQL连接")
        print("2. 使用上述连接参数")
        print("3. 连接成功后，在浏览器中展开连接")
        print("4. 查找分析结果表并拖到画布中")
        print("5. 如果表不显示，检查表是否有几何字段")
        
        print(f"\n💡 故障排除:")
        print("- 如果看不到表，检查用户权限")
        print("- 如果表存在但无法加载，检查几何字段")
        print("- 如果几何为空，先运行数据导出")
    else:
        print(f"❌ 连接信息解析失败: {conn_info}")


def run_export_if_needed():
    """如果需要，运行数据导出"""
    print("\n🔄 检查是否需要导出数据")
    print("-" * 40)
    
    config = SpatialJoinConfig()
    engine = create_engine(config.local_dsn)
    
    try:
        with engine.connect() as conn:
            count_sql = text(f"SELECT COUNT(*) FROM {config.analysis_results_table}")
            count = conn.execute(count_sql).fetchone()[0]
            
            if count == 0:
                print("📊 分析结果表为空，尝试导出数据...")
                
                # 运行简单的导出
                from src.spdatalab.fusion.spatial_join_production import export_analysis_to_qgis, get_available_cities
                
                # 获取示例城市
                cities_df = get_available_cities()
                sample_city = None
                if not cities_df.empty and 'city_id' in cities_df.columns:
                    sample_city = cities_df.iloc[0]['city_id']
                
                # 导出路口类型分析
                analysis_id = export_analysis_to_qgis(
                    analysis_type="intersection_type",
                    city_filter=sample_city,
                    include_geometry=True
                )
                
                print(f"✅ 数据导出完成: {analysis_id}")
                return True
            else:
                print(f"✅ 分析结果表已有 {count} 条记录")
                return True
                
    except Exception as e:
        print(f"❌ 数据导出失败: {e}")
        return False


if __name__ == "__main__":
    print("🔍 分析结果表诊断工具")
    print("=" * 60)
    
    # 1. 检查数据库连接
    if not check_database_connection():
        exit(1)
    
    # 2. 检查表存在状态
    table_results = check_tables_exist()
    
    # 3. 检查表结构
    structure_ok = check_table_structure()
    
    # 4. 检查样本数据
    data_ok = check_sample_data()
    
    # 5. 检查几何数据
    geometry_ok = check_geometry_data()
    
    # 6. 如果需要，导出数据
    if not data_ok:
        export_ok = run_export_if_needed()
        if export_ok:
            check_sample_data()
            check_geometry_data()
    
    # 7. 提供QGIS指导
    provide_qgis_guidance()
    
    # 总结
    print("\n" + "=" * 60)
    print("🎯 诊断总结:")
    
    config = SpatialJoinConfig()
    if table_results.get(config.analysis_results_table, {}).get('exists', False):
        count = table_results[config.analysis_results_table].get('count', 0)
        if count > 0:
            print(f"✅ 分析结果表正常，包含 {count} 条记录")
            print("📌 在QGIS中应该能看到这个表")
        else:
            print("⚠️ 分析结果表存在但为空")
            print("📌 需要先运行数据导出")
    else:
        print("❌ 分析结果表不存在")
        print("📌 需要先运行 python examples/qgis_visualization_guide.py") 