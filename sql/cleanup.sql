-- 清理脚本
-- 支持两种清理级别：标准清理和彻底清理
-- 使用方法：
--   psql -f cleanup.sql                             # 标准清理（默认）
--   psql -v cleanup_level=2 -f cleanup.sql          # 彻底清理

\echo '=========================================='
\echo '数据库清理脚本'
\echo '=========================================='

-- 设置默认清理级别（如果没有指定）
\set cleanup_level_default 1
SELECT COALESCE(:'cleanup_level', :'cleanup_level_default') as current_cleanup_level \gset

\echo '清理级别：' :current_cleanup_level

-- ==============================================
-- 级别1：标准清理（clips_bbox相关数据）
-- ==============================================
\if :current_cleanup_level >= 1
\echo '执行标准清理：删除clips_bbox相关数据...'

-- 删除统一视图
DROP VIEW IF EXISTS clips_bbox_all CASCADE;

-- 删除所有分表
DO $$
DECLARE
    table_record RECORD;
    table_count INTEGER := 0;
BEGIN
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_name LIKE 'clips_bbox%'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_record.table_name) || ' CASCADE';
        table_count := table_count + 1;
        RAISE NOTICE '已删除表: %', table_record.table_name;
    END LOOP;
    
    RAISE NOTICE '总计删除了 % 个clips_bbox相关表', table_count;
END $$;

-- 删除clip_filter表
DROP TABLE IF EXISTS clip_filter CASCADE;

-- 清理相关的序列
DO $$
DECLARE
    seq_record RECORD;
    seq_count INTEGER := 0;
BEGIN
    FOR seq_record IN 
        SELECT sequence_name 
        FROM information_schema.sequences 
        WHERE sequence_schema = 'public' 
          AND sequence_name LIKE 'clips_bbox%'
    LOOP
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(seq_record.sequence_name) || ' CASCADE';
        seq_count := seq_count + 1;
        RAISE NOTICE '已删除序列: %', seq_record.sequence_name;
    END LOOP;
    
    IF seq_count > 0 THEN
        RAISE NOTICE '总计删除了 % 个相关序列', seq_count;
    END IF;
END $$;

\echo '✅ 标准清理完成：clips_bbox相关数据已删除'
\endif

-- ==============================================
-- 级别2：彻底清理（删除所有用户schema和数据）
-- ==============================================
\if :current_cleanup_level >= 2
\echo '执行彻底清理：删除所有用户schema和数据...'

-- 强制断开所有其他连接
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity 
WHERE datname = current_database() 
  AND pid <> pg_backend_pid();

-- 删除所有用户schema
DO $$
DECLARE
    schema_record RECORD;
    schema_count INTEGER := 0;
BEGIN
    FOR schema_record IN 
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN (
            'information_schema', 
            'pg_catalog', 
            'pg_toast'
        )
        AND schema_name NOT LIKE 'pg_temp_%'
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name
    LOOP
        BEGIN
            EXECUTE 'REVOKE ALL ON SCHEMA ' || quote_ident(schema_record.schema_name) || ' FROM PUBLIC CASCADE';
            EXECUTE 'DROP SCHEMA ' || quote_ident(schema_record.schema_name) || ' CASCADE';
            schema_count := schema_count + 1;
            RAISE NOTICE '已删除schema: %', schema_record.schema_name;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '删除schema % 失败: %', schema_record.schema_name, SQLERRM;
        END;
    END LOOP;
    
    RAISE NOTICE '总计删除了 % 个用户schema', schema_count;
END $$;

-- 删除所有非必需扩展
DO $$
DECLARE
    ext_record RECORD;
BEGIN
    FOR ext_record IN 
        SELECT extname 
        FROM pg_extension 
        WHERE extname NOT IN ('plpgsql')
    LOOP
        BEGIN
            EXECUTE 'DROP EXTENSION ' || quote_ident(ext_record.extname) || ' CASCADE';
            RAISE NOTICE '删除扩展: %', ext_record.extname;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '删除扩展 % 失败: %', ext_record.extname, SQLERRM;
        END;
    END LOOP;
END $$;

\echo '✅ 彻底清理完成：所有用户数据已删除'
\endif

-- ==============================================
-- 重新创建干净环境
-- ==============================================
\echo '重新创建干净环境...'

-- 确保public schema存在
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;

-- 安装PostGIS扩展
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- 创建干净的clips_bbox表
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

-- 创建clip_filter表
CREATE TABLE clip_filter(
    scene_token text PRIMARY KEY,
    dataset_name text
);

\echo '✅ 干净环境已创建'

-- ==============================================
-- 清理完成报告
-- ==============================================
\echo '=========================================='
\echo '清理完成！清理级别：' :current_cleanup_level
\echo ''
\echo '清理级别说明：'
\echo '  1 - 标准清理：删除clips_bbox相关数据，重建干净表'
\echo '  2 - 彻底清理：删除所有用户schema和数据'
\echo '==========================================' 