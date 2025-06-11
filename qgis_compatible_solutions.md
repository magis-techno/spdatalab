# QGIS兼容的统一视图解决方案

## 🎯 问题分析

QGIS可以加载PostgreSQL视图，但有严格的要求：

### 📋 QGIS视图要求
1. **主键要求**：视图必须有唯一的整数标识符列
2. **几何列要求**：必须有正确的geometry列和SRID
3. **数据完整性**：每行都必须有唯一标识

### ❌ 当前问题
- 统一视图可能有重复的id（来自不同表）
- 没有全局唯一的主键
- QGIS无法正确识别记录

## 💡 解决方案

### 方案1: 使用ROW_NUMBER创建全局唯一ID（推荐）

```python
def create_qgis_compatible_unified_view(eng, view_name: str = 'clips_bbox_unified') -> bool:
    """创建QGIS兼容的统一视图，带全局唯一ID"""
    try:
        # 获取分表列表
        all_tables = list_bbox_tables(eng)
        bbox_tables = filter_partition_tables(all_tables, exclude_view=view_name)
        
        if not bbox_tables:
            print("没有找到任何分表，无法创建统一视图")
            return False
        
        # 构建带ROW_NUMBER的UNION查询
        union_parts = []
        for table_name in bbox_tables:
            subdataset_name = table_name.replace('clips_bbox_', '') if table_name.startswith('clips_bbox_') else table_name
            
            union_part = f"""
            SELECT 
                {table_name}.id as original_id,
                {table_name}.scene_token,
                {table_name}.data_name,
                {table_name}.event_id,
                {table_name}.city_id,
                {table_name}.timestamp,
                {table_name}.all_good,
                {table_name}.geometry,
                '{subdataset_name}' as subdataset_name,
                '{table_name}' as source_table
            FROM {table_name}
            """
            union_parts.append(union_part)
        
        # 包装在ROW_NUMBER中创建全局唯一ID
        inner_query = "UNION ALL\n".join(union_parts)
        
        view_query = f"""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY source_table, original_id) as qgis_id,
            original_id,
            scene_token,
            data_name,
            event_id,
            city_id,
            timestamp,
            all_good,
            geometry,
            subdataset_name,
            source_table
        FROM (
            {inner_query}
        ) as unified_data
        """
        
        # 创建视图
        drop_view_sql = text(f"DROP VIEW IF EXISTS {view_name};")
        create_view_sql = text(f"CREATE OR REPLACE VIEW {view_name} AS {view_query};")
        
        with eng.connect() as conn:
            conn.execute(drop_view_sql)
            conn.execute(create_view_sql)
            conn.commit()
        
        print(f"✅ 成功创建QGIS兼容的统一视图 {view_name}")
        print(f"📋 在QGIS中加载时，请选择 'qgis_id' 作为主键列")
        
        return True
        
    except Exception as e:
        print(f"创建QGIS兼容统一视图失败: {str(e)}")
        return False
```

### 方案2: 创建物化视图（更好的性能）

```python
def create_materialized_unified_view(eng, view_name: str = 'clips_bbox_unified_mat') -> bool:
    """创建物化视图，提供更好的QGIS性能"""
    try:
        # 获取分表列表
        all_tables = list_bbox_tables(eng)
        bbox_tables = filter_partition_tables(all_tables, exclude_view=view_name)
        
        if not bbox_tables:
            print("没有找到任何分表，无法创建物化视图")
            return False
        
        # 构建UNION查询
        union_parts = []
        for table_name in bbox_tables:
            subdataset_name = table_name.replace('clips_bbox_', '') if table_name.startswith('clips_bbox_') else table_name
            
            union_part = f"""
            SELECT 
                {table_name}.id as original_id,
                {table_name}.scene_token,
                {table_name}.data_name,
                {table_name}.event_id,
                {table_name}.city_id,
                {table_name}.timestamp,
                {table_name}.all_good,
                {table_name}.geometry,
                '{subdataset_name}' as subdataset_name,
                '{table_name}' as source_table
            FROM {table_name}
            """
            union_parts.append(union_part)
        
        inner_query = "UNION ALL\n".join(union_parts)
        
        # 创建物化视图SQL
        drop_view_sql = text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
        
        create_mat_view_sql = text(f"""
            CREATE MATERIALIZED VIEW {view_name} AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY source_table, original_id) as qgis_id,
                original_id,
                scene_token,
                data_name,
                event_id,
                city_id,
                timestamp,
                all_good,
                geometry,
                subdataset_name,
                source_table
            FROM (
                {inner_query}
            ) as unified_data;
        """)
        
        # 创建索引
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {view_name}_qgis_id_idx 
            ON {view_name} (qgis_id);
        """)
        
        create_spatial_index_sql = text(f"""
            CREATE INDEX IF NOT EXISTS {view_name}_geom_idx 
            ON {view_name} USING GIST (geometry);
        """)
        
        with eng.connect() as conn:
            conn.execute(drop_view_sql)
            conn.execute(create_mat_view_sql)
            conn.execute(create_index_sql)
            conn.execute(create_spatial_index_sql)
            conn.commit()
        
        print(f"✅ 成功创建物化视图 {view_name}")
        print(f"📋 在QGIS中使用 'qgis_id' 作为主键列")
        print(f"💡 提示：数据更新后需要刷新物化视图：REFRESH MATERIALIZED VIEW {view_name};")
        
        return True
        
    except Exception as e:
        print(f"创建物化视图失败: {str(e)}")
        return False

def refresh_materialized_view(eng, view_name: str = 'clips_bbox_unified_mat') -> bool:
    """刷新物化视图"""
    try:
        refresh_sql = text(f"REFRESH MATERIALIZED VIEW {view_name};")
        
        with eng.connect() as conn:
            conn.execute(refresh_sql)
            conn.commit()
        
        print(f"✅ 物化视图 {view_name} 刷新完成")
        return True
        
    except Exception as e:
        print(f"刷新物化视图失败: {str(e)}")
        return False
```

### 方案3: 创建聚合表（最佳QGIS性能）

```python
def create_aggregated_table(eng, table_name: str = 'clips_bbox_aggregated') -> bool:
    """创建聚合表，提供最佳QGIS性能"""
    try:
        # 获取分表列表
        all_tables = list_bbox_tables(eng)
        bbox_tables = filter_partition_tables(all_tables, exclude_view=table_name)
        
        if not bbox_tables:
            print("没有找到任何分表，无法创建聚合表")
            return False
        
        # 构建INSERT语句
        drop_table_sql = text(f"DROP TABLE IF EXISTS {table_name};")
        
        # 创建表结构（基于第一个分表）
        create_table_sql = text(f"""
            CREATE TABLE {table_name} (
                qgis_id SERIAL PRIMARY KEY,
                original_id INTEGER,
                scene_token VARCHAR(255),
                data_name VARCHAR(255),
                event_id VARCHAR(255),
                city_id VARCHAR(255),
                timestamp TIMESTAMP,
                all_good BOOLEAN,
                geometry GEOMETRY(MULTIPOLYGON, 4326),
                subdataset_name VARCHAR(255),
                source_table VARCHAR(255)
            );
        """)
        
        with eng.connect() as conn:
            conn.execute(drop_table_sql)
            conn.execute(create_table_sql)
            
            # 从每个分表插入数据
            for table_name_src in bbox_tables:
                subdataset_name = table_name_src.replace('clips_bbox_', '') if table_name_src.startswith('clips_bbox_') else table_name_src
                
                insert_sql = text(f"""
                    INSERT INTO {table_name} 
                    (original_id, scene_token, data_name, event_id, city_id, timestamp, all_good, geometry, subdataset_name, source_table)
                    SELECT 
                        id, scene_token, data_name, event_id, city_id, timestamp, all_good, geometry,
                        '{subdataset_name}', '{table_name_src}'
                    FROM {table_name_src};
                """)
                
                conn.execute(insert_sql)
                print(f"✅ 已插入 {table_name_src} 的数据")
            
            # 创建空间索引
            spatial_index_sql = text(f"""
                CREATE INDEX {table_name}_geom_idx 
                ON {table_name} USING GIST (geometry);
            """)
            conn.execute(spatial_index_sql)
            
            conn.commit()
        
        print(f"✅ 成功创建聚合表 {table_name}")
        print(f"📋 在QGIS中使用 'qgis_id' 作为主键列")
        
        return True
        
    except Exception as e:
        print(f"创建聚合表失败: {str(e)}")
        return False
```

## 📝 QGIS加载指南

### 1. 在QGIS中添加PostgreSQL连接
1. Layer → Add Layer → Add PostGIS Layers
2. 配置数据库连接信息
3. 测试连接

### 2. 加载统一视图/表
1. 选择创建的视图或表
2. **重要**：在Primary key列中选择 `qgis_id`
3. 在Geometry column中选择 `geometry`
4. 点击Add加载

### 3. 如果遇到问题
- 确保geometry列有正确的SRID
- 检查是否选择了正确的主键列
- 尝试使用DB Manager中的SQL窗口手动加载

## 🎯 推荐方案

1. **开发/测试阶段**：使用方案1（普通视图+ROW_NUMBER）
2. **生产环境**：使用方案2（物化视图）或方案3（聚合表）
3. **大数据量**：使用方案3（聚合表）获得最佳性能

## 🔄 更新策略

- **普通视图**：自动反映数据变化
- **物化视图**：需要手动或定时刷新
- **聚合表**：需要重新创建或增量更新 