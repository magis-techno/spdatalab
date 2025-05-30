-- Drop existing for clean state
DROP TABLE IF EXISTS clips_bbox CASCADE;
DROP TABLE IF EXISTS clip_filter;

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
    all_good boolean,
    geom geometry(Polygon,4326)
);

CREATE INDEX idx_clips_bbox_geom ON clips_bbox USING GIST(geom);
CREATE INDEX idx_clips_bbox_data_name ON clips_bbox(data_name);
CREATE INDEX idx_clips_bbox_scene_token ON clips_bbox(scene_token);