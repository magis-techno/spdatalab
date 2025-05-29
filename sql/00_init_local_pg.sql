-- clip_filter & clips_bbox tables
CREATE TABLE IF NOT EXISTS clip_filter(
    scene_token text PRIMARY KEY,
    dataset_name text
);

CREATE TABLE IF NOT EXISTS clips_bbox(
    clip_id text PRIMARY KEY,
    geom geometry(Polygon, 4326)
);

CREATE INDEX IF NOT EXISTS idx_clips_bbox_geom ON clips_bbox USING GIST(geom);