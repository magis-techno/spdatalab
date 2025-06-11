-- 表清理脚本
-- 用途：彻底清理所有clips_bbox相关的表、视图和schema

\echo '开始彻底清理clips_bbox相关数据...'

-- ==============================================
-- 第一步：删除统一视图
-- ==============================================
\echo '删除统一视图...'
DROP VIEW IF EXISTS clips_bbox_all CASCADE;

-- ==============================================
-- 第二步：删除所有分表（clips_bbox_*）
-- ==============================================
\echo '删除所有clips_bbox分表...'

-- 使用存储过程动态删除所有clips_bbox_*表
DO $$
DECLARE
    table_record RECORD;
    table_count INTEGER := 0;
BEGIN
    -- 查找所有clips_bbox_*表
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_name LIKE 'clips_bbox_%'
          AND table_type = 'BASE TABLE'
    LOOP
        -- 删除每个表
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_record.table_name) || ' CASCADE';
        table_count := table_count + 1;
        RAISE NOTICE '已删除表: %', table_record.table_name;
    END LOOP;
    
    RAISE NOTICE '总计删除了 % 个分表', table_count;
END $$;

-- ==============================================
-- 第三步：删除原来的主表
-- ==============================================
\echo '删除原始clips_bbox表...'
DROP TABLE IF EXISTS clips_bbox CASCADE;

-- ==============================================
-- 第四步：清理相关的schema（如果存在）
-- ==============================================
\echo '检查并清理相关schema...'

-- 删除可能存在的专用schema（如果有的话）
DO $$
DECLARE
    schema_record RECORD;
    schema_count INTEGER := 0;
BEGIN
    -- 查找可能相关的schema（根据实际情况调整模式）
    FOR schema_record IN 
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'bbox_%' 
           OR schema_name LIKE 'clips_%'
           OR schema_name LIKE 'dataset_%'
    LOOP
        -- 删除schema及其所有内容
        EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(schema_record.schema_name) || ' CASCADE';
        schema_count := schema_count + 1;
        RAISE NOTICE '已删除schema: %', schema_record.schema_name;
    END LOOP;
    
    IF schema_count = 0 THEN
        RAISE NOTICE '未找到需要删除的相关schema';
    ELSE
        RAISE NOTICE '总计删除了 % 个schema', schema_count;
    END IF;
END $$;

-- ==============================================
-- 第五步：清理相关的序列（如果有遗留）
-- ==============================================
\echo '清理遗留的序列...'

DO $$
DECLARE
    seq_record RECORD;
    seq_count INTEGER := 0;
BEGIN
    -- 查找可能遗留的序列
    FOR seq_record IN 
        SELECT sequence_name 
        FROM information_schema.sequences 
        WHERE sequence_schema = 'public' 
          AND sequence_name LIKE 'clips_bbox_%'
    LOOP
        -- 删除序列
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(seq_record.sequence_name) || ' CASCADE';
        seq_count := seq_count + 1;
        RAISE NOTICE '已删除序列: %', seq_record.sequence_name;
    END LOOP;
    
    IF seq_count = 0 THEN
        RAISE NOTICE '未找到需要删除的相关序列';
    ELSE
        RAISE NOTICE '总计删除了 % 个序列', seq_count;
    END IF;
END $$;

-- ==============================================
-- 第六步：重新创建干净的主表（可选）
-- ==============================================
\echo '重新创建干净的clips_bbox主表...'

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
-- 完成清理
-- ==============================================
\echo '======================================'
\echo 'clips_bbox彻底清理完成！'
\echo '- 已删除所有分表 (clips_bbox_*)'
\echo '- 已删除统一视图 (clips_bbox_all)'
\echo '- 已删除相关schema'
\echo '- 已删除遗留序列'
\echo '- 已重建干净的主表'
\echo '======================================' 