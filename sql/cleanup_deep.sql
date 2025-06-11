-- 深度清理脚本
-- 警告：此脚本会删除所有用户创建的schema和数据，只保留系统schema
-- 用途：彻底重置数据库到初始状态

\echo '=========================================='
\echo '警告：即将执行深度清理！'
\echo '此操作将删除所有用户数据和schema'
\echo '=========================================='

-- ==============================================
-- 第一步：列出所有用户schema
-- ==============================================
\echo '列出所有用户schema...'

DO $$
DECLARE
    schema_record RECORD;
    schema_count INTEGER := 0;
BEGIN
    RAISE NOTICE '发现以下用户schema：';
    FOR schema_record IN 
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN (
            'information_schema', 
            'pg_catalog', 
            'pg_toast', 
            'pg_temp_1', 
            'pg_toast_temp_1'
        )
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name
    LOOP
        schema_count := schema_count + 1;
        RAISE NOTICE '  %: %', schema_count, schema_record.schema_name;
    END LOOP;
    
    RAISE NOTICE '总计发现 % 个用户schema', schema_count;
END $$;

-- ==============================================
-- 第二步：删除所有用户schema（谨慎操作）
-- ==============================================
\echo '删除所有用户schema...'

DO $$
DECLARE
    schema_record RECORD;
    schema_count INTEGER := 0;
    protected_schemas TEXT[] := ARRAY[
        'information_schema', 
        'pg_catalog', 
        'pg_toast',
        'pg_temp_1',
        'pg_toast_temp_1'
    ];
BEGIN
    -- 查找所有非系统schema
    FOR schema_record IN 
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN (
            'information_schema', 
            'pg_catalog', 
            'pg_toast', 
            'pg_temp_1', 
            'pg_toast_temp_1'
        )
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name
    LOOP
        -- 删除schema及其所有内容
        BEGIN
            EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(schema_record.schema_name) || ' CASCADE';
            schema_count := schema_count + 1;
            RAISE NOTICE '已删除schema: %', schema_record.schema_name;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '删除schema % 失败: %', schema_record.schema_name, SQLERRM;
        END;
    END LOOP;
    
    RAISE NOTICE '总计删除了 % 个schema', schema_count;
END $$;

-- ==============================================
-- 第三步：重新创建public schema和PostGIS扩展
-- ==============================================
\echo '重新创建public schema...'

-- 创建public schema（如果被删除了）
CREATE SCHEMA IF NOT EXISTS public;

-- 设置默认权限
GRANT ALL ON SCHEMA public TO PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;

-- 重新创建PostGIS扩展（如果需要）
\echo '重新创建PostGIS扩展...'
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ==============================================
-- 第四步：创建干净的clips_bbox表
-- ==============================================
\echo '创建干净的clips_bbox表...'

CREATE TABLE clips_bbox(
    id serial PRIMARY KEY,
    scene_token text,
    data_name text UNIQUE,
    event_id text,
    city_id text,
    "timestamp" bigint,
    all_good boolean
);

-- 添加PostGIS几何列
SELECT AddGeometryColumn('public', 'clips_bbox', 'geometry', 4326, 'POLYGON', 2);

-- 添加几何约束
ALTER TABLE clips_bbox ADD CONSTRAINT check_geom_type 
    CHECK (ST_GeometryType(geometry) IN ('ST_Polygon', 'ST_Point'));

-- 创建索引
CREATE INDEX idx_clips_bbox_geometry ON clips_bbox USING GIST(geometry);
CREATE INDEX idx_clips_bbox_data_name ON clips_bbox(data_name);
CREATE INDEX idx_clips_bbox_scene_token ON clips_bbox(scene_token);

-- ==============================================
-- 第五步：列出清理后的状态
-- ==============================================
\echo '清理完成后的schema状态：'

DO $$
DECLARE
    schema_record RECORD;
    schema_count INTEGER := 0;
BEGIN
    FOR schema_record IN 
        SELECT schema_name 
        FROM information_schema.schemata 
        ORDER BY schema_name
    LOOP
        schema_count := schema_count + 1;
        RAISE NOTICE '  %: %', schema_count, schema_record.schema_name;
    END LOOP;
    
    RAISE NOTICE '清理后总计 % 个schema', schema_count;
END $$;

-- ==============================================
-- 完成深度清理
-- ==============================================
\echo '=========================================='
\echo '深度清理完成！'
\echo '- 已删除所有用户schema和数据'
\echo '- 已重建public schema'
\echo '- 已重新创建PostGIS扩展'
\echo '- 已创建干净的clips_bbox表'
\echo '- 数据库已重置到初始状态'
\echo '==========================================' 