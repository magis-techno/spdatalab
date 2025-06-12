-- 轨迹数据库初始化脚本
-- 创建轨迹点数据表，模拟 public.ddi_data_points

-- 创建schema
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS transform;

-- 启用PostGIS扩展
CREATE EXTENSION IF NOT EXISTS postgis;

-- 创建主要的轨迹点数据表
CREATE TABLE public.ddi_data_points (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    point_lla GEOMETRY(POINT, 4326) NOT NULL,
    workstage INTEGER DEFAULT 2,
    scene_id VARCHAR(100),
    timestamp_ms BIGINT,
    vehicle_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建空间索引
CREATE INDEX idx_ddi_data_points_geom ON public.ddi_data_points USING GIST (point_lla);
CREATE INDEX idx_ddi_data_points_dataset ON public.ddi_data_points (dataset_name);
CREATE INDEX idx_ddi_data_points_workstage ON public.ddi_data_points (workstage);
CREATE INDEX idx_ddi_data_points_scene_id ON public.ddi_data_points (scene_id);

-- 创建统计视图
CREATE VIEW public.dataset_stats AS
SELECT 
    dataset_name,
    COUNT(*) as point_count,
    COUNT(DISTINCT scene_id) as scene_count,
    bool_and(workstage = 2) as all_good,
    ST_Extent(point_lla) as bbox,
    MIN(timestamp_ms) as min_timestamp,
    MAX(timestamp_ms) as max_timestamp
FROM public.ddi_data_points
GROUP BY dataset_name;

-- 创建transform schema中的视图（用于兼容现有代码）
CREATE VIEW transform.ods_t_data_fragment_datalake AS
SELECT 
    DISTINCT dataset_name as origin_name,
    scene_id as id,
    'mock_event_' || substring(scene_id, 1, 8) as event_id,
    CASE 
        WHEN dataset_name LIKE '%beijing%' THEN 'BJ1'
        WHEN dataset_name LIKE '%shanghai%' THEN 'SH1'
        WHEN dataset_name LIKE '%guangzhou%' THEN 'GZ1'
        WHEN dataset_name LIKE '%shenzhen%' THEN 'SZ1'
        ELSE 'A' || (abs(hashtext(dataset_name)) % 99 + 1)::text
    END as city_id,
    extract(epoch from created_at)::bigint * 1000 as timestamp
FROM (
    SELECT DISTINCT dataset_name, scene_id, created_at
    FROM public.ddi_data_points
    WHERE scene_id IS NOT NULL
) t;

-- 插入一些示例数据
INSERT INTO public.ddi_data_points (dataset_name, point_lla, workstage, scene_id, timestamp_ms, vehicle_id) VALUES
('test_dataset_001', ST_GeomFromText('POINT(116.4074 39.9042)', 4326), 2, 'scene_001', 1640995200000, 'vehicle_001'),
('test_dataset_001', ST_GeomFromText('POINT(116.4075 39.9043)', 4326), 2, 'scene_001', 1640995201000, 'vehicle_001'),
('test_dataset_002', ST_GeomFromText('POINT(121.4737 31.2304)', 4326), 2, 'scene_002', 1640995202000, 'vehicle_002'),
('test_dataset_002', ST_GeomFromText('POINT(121.4738 31.2305)', 4326), 2, 'scene_002', 1640995203000, 'vehicle_002'),
('test_dataset_003', ST_GeomFromText('POINT(113.2644 23.1291)', 4326), 2, 'scene_003', 1640995204000, 'vehicle_003');

-- 创建更新触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_ddi_data_points_updated_at 
    BEFORE UPDATE ON public.ddi_data_points 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column(); 