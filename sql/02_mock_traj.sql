-- Mock trajectory database schema & sample row
CREATE TABLE traj_tracks(
    track_id text PRIMARY KEY,
    dataset_name text,
    geom geometry(LineString, 4326)
);
INSERT INTO traj_tracks VALUES
  ('track_demo', 'dataset_demo', ST_GeomFromText('LINESTRING(0 0,1 1)',4326));