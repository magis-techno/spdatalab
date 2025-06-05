-- =============================================================================
-- FDW 远程数据源配置和标准化表名映射
-- 
-- 目标：建立统一的表名规范，使应用层可以使用标准名称访问各种数据源
-- =============================================================================

DROP SERVER IF EXISTS traj_srv CASCADE;
DROP SERVER IF EXISTS map_srv CASCADE;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- -----------------------------------------------------------------------------
-- 轨迹数据源 (trajectory data)
-- -----------------------------------------------------------------------------
CREATE SERVER traj_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '10.170.30.193', dbname 'dataset_gy1', port '9001');
CREATE USER MAPPING FOR postgres SERVER traj_srv
  OPTIONS (user '**', password '**');

-- 导入轨迹点数据
IMPORT FOREIGN SCHEMA public LIMIT TO (ddi_data_points)
  FROM SERVER traj_srv INTO public;

-- -----------------------------------------------------------------------------
-- 地图数据源 (map/GIS data) 
-- -----------------------------------------------------------------------------
CREATE SERVER map_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '10.170.30.193', dbname 'rcdatalake_gy1', port '9001');
CREATE USER MAPPING FOR postgres SERVER map_srv
  OPTIONS (user '**', password '*');

-- 导入路口数据
IMPORT FOREIGN SCHEMA public LIMIT TO (full_intersection)
  FROM SERVER map_srv INTO public;

-- TODO: 根据实际情况添加其他图层
-- IMPORT FOREIGN SCHEMA public LIMIT TO (roads, pois, regions) 
--   FROM SERVER map_srv INTO public;

-- =============================================================================
-- 标准化表名映射视图
-- 
-- 为所有外部表创建标准化的视图名称，使应用层可以使用统一的表名
-- =============================================================================

-- 标准路口表 (intersections)
DROP VIEW IF EXISTS intersections CASCADE;
CREATE VIEW intersections AS 
SELECT 
    *,
    -- 确保几何列统一命名为 geom
    wkb_geometry as geometry
FROM full_intersection;

-- 标准轨迹点表 (trajectory_points) 
DROP VIEW IF EXISTS trajectory_points CASCADE;
CREATE VIEW trajectory_points AS
SELECT * FROM ddi_data_points;

-- -----------------------------------------------------------------------------
-- 未来图层扩展模板
-- -----------------------------------------------------------------------------
/*
-- 道路网络标准化视图
DROP VIEW IF EXISTS roads CASCADE;
CREATE VIEW roads AS
SELECT 
    road_id,
    road_name,
    road_type,
    geom as geometry
FROM external_roads_table;

-- POI标准化视图  
DROP VIEW IF EXISTS pois CASCADE;
CREATE VIEW pois AS
SELECT
    poi_id,
    poi_name, 
    poi_category,
    geom as geometry
FROM external_poi_table;

-- 行政区域标准化视图
DROP VIEW IF EXISTS regions CASCADE; 
CREATE VIEW regions AS
SELECT
    region_id,
    region_name,
    region_type,
    geom as geometry  
FROM external_regions_table;
*/

-- =============================================================================
-- 性能优化说明
-- =============================================================================

-- 注意：外部表(Foreign Tables)依赖源数据库的索引来提升查询性能
-- 本地无需创建索引，源数据库通常已经有相应的空间索引

-- =============================================================================
-- 元数据视图：提供可用图层信息
-- =============================================================================

DROP VIEW IF EXISTS available_layers CASCADE;
CREATE VIEW available_layers AS
SELECT 
    'intersections' as layer_name,
    'road intersections/junctions' as description,
    'full_intersection' as source_table,
    'point' as geometry_type,
    count(*) as record_count
FROM full_intersection
UNION ALL
SELECT 
    'trajectory_points' as layer_name,
    'trajectory point data' as description, 
    'ddi_data_points' as source_table,
    'point' as geometry_type,
    count(*) as record_count
FROM ddi_data_points;

-- 显示配置完成信息
DO $$
BEGIN
    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'FDW 配置完成！';
    RAISE NOTICE '=============================================================================';
    RAISE NOTICE '可用的标准化图层:';
    RAISE NOTICE '  - intersections    (路口数据，映射自 full_intersection)';
    RAISE NOTICE '  - trajectory_points (轨迹点数据，映射自 ddi_data_points)';
    RAISE NOTICE '';
    RAISE NOTICE '使用示例:';
    RAISE NOTICE '  SELECT count(*) FROM intersections;';
    RAISE NOTICE '  SELECT * FROM available_layers;';
    RAISE NOTICE '=============================================================================';
END $$;