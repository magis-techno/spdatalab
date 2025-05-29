-- Mock business mapping table scene_token -> dataset_name
CREATE TABLE biz_scene_map(
    scene_token text PRIMARY KEY,
    dataset_name text
);
INSERT INTO biz_scene_map VALUES ('scene_demo','dataset_demo');