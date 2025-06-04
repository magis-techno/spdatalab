-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Drop existing for clean state
DROP TABLE IF EXISTS clips_bbox CASCADE;
DROP TABLE IF EXISTS clip_filter CASCADE;

CREATE TABLE clip_filter(
    scene_token text PRIMARY KEY,
    dataset_name text
);

CREATE TABLE clips_bbox(
    id serial PRIMARY KEY,
    scene_token text,
    data_name text UNIQUE,
    event_id text,
    city_id text,
    "timestamp" bigint,
    all_good boolean
);

-- Add geometry column using PostGIS function to ensure proper SRID registration
SELECT AddGeometryColumn('public', 'clips_bbox', 'geom', 4326, 'POLYGON', 2);

-- Alternatively support POINT geometry for single point data
ALTER TABLE clips_bbox ADD CONSTRAINT check_geom_type 
    CHECK (ST_GeometryType(geom) IN ('ST_Polygon', 'ST_Point'));

CREATE INDEX idx_clips_bbox_geom ON clips_bbox USING GIST(geom);
CREATE INDEX idx_clips_bbox_data_name ON clips_bbox(data_name);
CREATE INDEX idx_clips_bbox_scene_token ON clips_bbox(scene_token);

-- Verify the geometry column registration
SELECT * FROM geometry_columns WHERE f_table_name = 'clips_bbox';