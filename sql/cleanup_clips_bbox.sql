-- =============================================================================
-- 清理并重建clips_bbox表
-- 
-- 目标：
-- 1. 完全删除现有的clips_bbox表
-- 2. 重新创建表结构
-- 3. 只创建必要的索引，避免重复
-- =============================================================================

\echo '开始清理clips_bbox表...'

-- 强制删除表（包括所有依赖关系）
DROP TABLE IF EXISTS clips_bbox CASCADE;

\echo 'clips_bbox表已删除'

-- 重新创建表
CREATE TABLE clips_bbox(
    id serial PRIMARY KEY,
    scene_token text,
    data_name text UNIQUE,
    event_id text,
    city_id text,
    "timestamp" bigint,
    all_good boolean
);

\echo 'clips_bbox表结构已创建'

-- 使用PostGIS添加几何列（不自动创建索引）
SELECT AddGeometryColumn('public', 'clips_bbox', 'geometry', 4326, 'POLYGON', 2);

\echo '几何列已添加'

-- 添加几何类型约束
ALTER TABLE clips_bbox ADD CONSTRAINT check_geom_type 
    CHECK (ST_GeometryType(geometry) IN ('ST_Polygon', 'ST_Point'));

\echo '几何约束已添加'

-- 创建索引（只创建需要的索引）
CREATE INDEX idx_clips_bbox_geometry ON clips_bbox USING GIST(geometry);
CREATE INDEX idx_clips_bbox_data_name ON clips_bbox(data_name);
CREATE INDEX idx_clips_bbox_scene_token ON clips_bbox(scene_token);

\echo '索引已创建'

-- 验证索引创建结果
\echo '验证clips_bbox表的索引:'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'clips_bbox'
ORDER BY indexname;

-- 验证几何列注册
\echo '验证几何列注册:'
SELECT * FROM geometry_columns WHERE f_table_name = 'clips_bbox';

\echo 'clips_bbox表清理和重建完成！' 