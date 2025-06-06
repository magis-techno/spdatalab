-- 表清理脚本
-- 用途：清理并重建clips_bbox表

\echo '清理clips_bbox表...'

-- 删除表
DROP TABLE IF EXISTS clips_bbox CASCADE;

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

-- 添加PostGIS几何列
SELECT AddGeometryColumn('public', 'clips_bbox', 'geometry', 4326, 'POLYGON', 2);

-- 添加几何约束
ALTER TABLE clips_bbox ADD CONSTRAINT check_geom_type 
    CHECK (ST_GeometryType(geometry) IN ('ST_Polygon', 'ST_Point'));

-- 创建索引
CREATE INDEX idx_clips_bbox_geometry ON clips_bbox USING GIST(geometry);
CREATE INDEX idx_clips_bbox_data_name ON clips_bbox(data_name);
CREATE INDEX idx_clips_bbox_scene_token ON clips_bbox(scene_token);

\echo '表重建完成' 