-- Mock junctions table
CREATE TABLE junctions(
    junc_id text PRIMARY KEY,
    geom geometry(Point, 4326)
);
INSERT INTO junctions VALUES
  ('j1', ST_GeomFromText('POINT(0.5 0.5)',4326));