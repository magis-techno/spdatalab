CREATE MATERIALIZED VIEW IF NOT EXISTS mv_clip_inter_overlay AS
SELECT  c.clip_id,
        i.inter_id,
        i.inter_type,
        ST_Area(i.geom::geography)                              AS size_m2,
        ST_Area(ST_Intersection(c.geom, i.geom)::geography)    AS overlap_m2
FROM    clips_bbox         AS c
JOIN    intersections      AS i ON c.city_id = i.city_id
WHERE   ST_Intersects(c.geom, i.geom);

CREATE INDEX IF NOT EXISTS mv_clip_inter_overlay_clip_idx  ON mv_clip_inter_overlay (clip_id);
CREATE INDEX IF NOT EXISTS mv_clip_inter_overlay_inter_idx ON mv_clip_inter_overlay (inter_id);