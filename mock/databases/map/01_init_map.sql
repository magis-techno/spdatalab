-- 地图数据库初始化脚本
-- 创建路口表，模拟 full_intersection

-- 创建schema
CREATE SCHEMA IF NOT EXISTS public;

-- 启用PostGIS扩展
CREATE EXTENSION IF NOT EXISTS postgis;

-- 创建路口表
CREATE TABLE public.full_intersection (
    id BIGSERIAL PRIMARY KEY,
    intersectiontype INTEGER NOT NULL,
    intersectionsubtype INTEGER NOT NULL,
    wkb_geometry GEOMETRY(POLYGON, 4326) NOT NULL,
    -- 额外的地图相关字段
    name VARCHAR(200),
    city_id VARCHAR(50),
    region_id VARCHAR(50),
    road_count INTEGER DEFAULT 4,
    traffic_light BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建空间索引
CREATE INDEX idx_full_intersection_geom ON public.full_intersection USING GIST (wkb_geometry);
CREATE INDEX idx_full_intersection_type ON public.full_intersection (intersectiontype);
CREATE INDEX idx_full_intersection_subtype ON public.full_intersection (intersectionsubtype);
CREATE INDEX idx_full_intersection_city ON public.full_intersection (city_id);

-- 创建路口类型映射表
CREATE TABLE public.intersection_type_mapping (
    type_id INTEGER PRIMARY KEY,
    type_name VARCHAR(100),
    description TEXT
);

-- 插入路口类型数据
INSERT INTO public.intersection_type_mapping (type_id, type_name, description) VALUES
(1, 'Intersection', 'Regular intersection'),
(2, 'Toll Station', 'Toll collection point'),
(3, 'Lane Change Area', 'Area designated for lane changes'),
(4, 'T-Junction Area', 'T-shaped intersection'),
(5, 'Roundabout', 'Circular intersection'),
(6, 'H-Junction Area', 'H-shaped intersection'),
(7, 'Invalid', 'Invalid or undefined intersection type'),
(8, 'Toll Booth Area', 'Individual toll booth area');

-- 创建路口子类型映射表
CREATE TABLE public.intersection_subtype_mapping (
    subtype_id INTEGER PRIMARY KEY,
    subtype_name VARCHAR(100),
    description TEXT
);

-- 插入路口子类型数据
INSERT INTO public.intersection_subtype_mapping (subtype_id, subtype_name, description) VALUES
(1, 'Regular', 'Regular intersection configuration'),
(2, 'T-Junction with Through Markings', 'T-junction with special lane markings'),
(3, 'Minor Junction (No Traffic Conflict)', 'Small junction without traffic conflicts'),
(4, 'Unmarked Junction', 'Junction without clear markings'),
(5, 'Secondary Junction', 'Secondary road intersection'),
(6, 'Conservative Through Junction', 'Junction with conservative through traffic'),
(7, 'Invalid', 'Invalid or undefined subtype');

-- 插入一些示例路口数据（在不同城市）
INSERT INTO public.full_intersection (
    intersectiontype, intersectionsubtype, wkb_geometry, name, city_id, region_id, road_count, traffic_light
) VALUES
-- 北京路口
(1, 1, ST_GeomFromText('POLYGON((116.407 39.904, 116.408 39.904, 116.408 39.905, 116.407 39.905, 116.407 39.904))', 4326), 
 'Beijing Test Intersection 1', 'BJ1', 'BJ', 4, true),
(4, 2, ST_GeomFromText('POLYGON((116.409 39.906, 116.410 39.906, 116.410 39.907, 116.409 39.907, 116.409 39.906))', 4326),
 'Beijing T-Junction 1', 'BJ1', 'BJ', 3, true),

-- 上海路口  
(1, 1, ST_GeomFromText('POLYGON((121.473 31.230, 121.474 31.230, 121.474 31.231, 121.473 31.231, 121.473 31.230))', 4326),
 'Shanghai Test Intersection 1', 'SH1', 'SH', 4, true),
(5, 1, ST_GeomFromText('POLYGON((121.475 31.232, 121.476 31.232, 121.476 31.233, 121.475 31.233, 121.475 31.232))', 4326),
 'Shanghai Roundabout 1', 'SH1', 'SH', 6, false),

-- 广州路口
(1, 1, ST_GeomFromText('POLYGON((113.264 23.129, 113.265 23.129, 113.265 23.130, 113.264 23.130, 113.264 23.129))', 4326),
 'Guangzhou Test Intersection 1', 'GZ1', 'GZ', 4, true),
(3, 1, ST_GeomFromText('POLYGON((113.266 23.131, 113.267 23.131, 113.267 23.132, 113.266 23.132, 113.266 23.131))', 4326),
 'Guangzhou Lane Change Area 1', 'GZ1', 'GZ', 2, false),

-- 深圳路口
(1, 1, ST_GeomFromText('POLYGON((114.057 22.543, 114.058 22.543, 114.058 22.544, 114.057 22.544, 114.057 22.543))', 4326),
 'Shenzhen Test Intersection 1', 'SZ1', 'SZ', 4, true),
(2, 1, ST_GeomFromText('POLYGON((114.059 22.545, 114.060 22.545, 114.060 22.546, 114.059 22.546, 114.059 22.545))', 4326),
 'Shenzhen Toll Station 1', 'SZ1', 'SZ', 6, true),

-- Mock城市路口
(1, 1, ST_GeomFromText('POLYGON((120.000 30.000, 120.001 30.000, 120.001 30.001, 120.000 30.001, 120.000 30.000))', 4326),
 'Mock City A01 Intersection 1', 'A01', 'A01', 4, true),
(1, 1, ST_GeomFromText('POLYGON((120.002 30.002, 120.003 30.002, 120.003 30.003, 120.002 30.003, 120.002 30.002))', 4326),
 'Mock City A72 Intersection 1', 'A72', 'A72', 4, true),
(1, 1, ST_GeomFromText('POLYGON((120.004 30.004, 120.005 30.004, 120.005 30.005, 120.004 30.005, 120.004 30.004))', 4326),
 'Mock City B15 Intersection 1', 'B15', 'B15', 4, true);

-- 创建统计视图
CREATE VIEW public.intersection_stats AS
SELECT 
    city_id,
    intersectiontype,
    intersectionsubtype,
    COUNT(*) as intersection_count,
    AVG(road_count) as avg_road_count,
    COUNT(CASE WHEN traffic_light THEN 1 END) as traffic_light_count
FROM public.full_intersection
GROUP BY city_id, intersectiontype, intersectionsubtype;

-- 创建带类型名称的详细视图
CREATE VIEW public.intersection_details AS
SELECT 
    i.id,
    i.intersectiontype,
    i.intersectionsubtype,
    tm.type_name,
    sm.subtype_name,
    i.wkb_geometry,
    i.name,
    i.city_id,
    i.region_id,
    i.road_count,
    i.traffic_light,
    i.created_at
FROM public.full_intersection i
LEFT JOIN public.intersection_type_mapping tm ON i.intersectiontype = tm.type_id
LEFT JOIN public.intersection_subtype_mapping sm ON i.intersectionsubtype = sm.subtype_id;

-- 创建更新触发器
CREATE OR REPLACE FUNCTION update_map_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_full_intersection_updated_at 
    BEFORE UPDATE ON public.full_intersection 
    FOR EACH ROW EXECUTE FUNCTION update_map_updated_at(); 