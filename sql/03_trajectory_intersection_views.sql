-- 轨迹交集分析的扩展SQL视图和函数
-- 用于支持高级的intersection overlay分析

-- 1. 创建轨迹与路口交集的物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trajectory_junction_intersections AS
WITH trajectory_buffered AS (
    SELECT 
        scene_token,
        data_name,
        city_id,
        timestamp,
        ST_Transform(
            ST_Buffer(ST_Transform(geometry, 3857), 20), 
            4326
        ) AS buffered_geom,
        geometry AS original_geom
    FROM clips_bbox
    WHERE geometry IS NOT NULL
),
intersection_analysis AS (
    SELECT 
        t.scene_token,
        t.data_name,
        t.city_id,
        t.timestamp,
        j.inter_id,
        j.inter_type,
        j.geom AS junction_geom,
        t.original_geom AS trajectory_geom,
        ST_Distance(
            ST_Transform(t.original_geom, 3857),
            ST_Transform(j.geom, 3857)
        ) AS distance_meters,
        ST_Area(
            ST_Intersection(t.buffered_geom, j.geom)::geography
        ) AS intersection_area_m2,
        CASE 
            WHEN ST_Intersects(t.original_geom, j.geom) THEN 'direct_intersection'
            WHEN ST_Intersects(t.buffered_geom, j.geom) THEN 'buffer_intersection'
            ELSE 'no_intersection'
        END AS intersection_type,
        ST_Centroid(
            ST_Intersection(t.buffered_geom, j.geom)
        ) AS intersection_point
    FROM trajectory_buffered t
    JOIN intersections j ON t.city_id = j.city_id
    WHERE ST_DWithin(
        ST_Transform(t.original_geom, 3857),
        ST_Transform(j.geom, 3857),
        20
    )
)
SELECT * FROM intersection_analysis
WHERE intersection_type != 'no_intersection';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_mv_traj_junction_scene_token 
    ON mv_trajectory_junction_intersections (scene_token);
CREATE INDEX IF NOT EXISTS idx_mv_traj_junction_city_id 
    ON mv_trajectory_junction_intersections (city_id);
CREATE INDEX IF NOT EXISTS idx_mv_traj_junction_inter_id 
    ON mv_trajectory_junction_intersections (inter_id);
CREATE INDEX IF NOT EXISTS idx_mv_traj_junction_intersection_point 
    ON mv_trajectory_junction_intersections USING GIST(intersection_point);

-- 2. 创建轨迹密度分析视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trajectory_density_analysis AS
WITH grid_cells AS (
    -- 创建网格单元（1km x 1km）
    SELECT 
        row_number() OVER() AS grid_id,
        ST_MakeEnvelope(
            x, y, x + 0.01, y + 0.01, 4326
        ) AS grid_geom
    FROM generate_series(
        floor((SELECT ST_XMin(ST_Extent(geometry)) FROM clips_bbox) * 100) / 100,
        ceil((SELECT ST_XMax(ST_Extent(geometry)) FROM clips_bbox) * 100) / 100,
        0.01
    ) AS x,
    generate_series(
        floor((SELECT ST_YMin(ST_Extent(geometry)) FROM clips_bbox) * 100) / 100,
        ceil((SELECT ST_YMax(ST_Extent(geometry)) FROM clips_bbox) * 100) / 100,
        0.01
    ) AS y
),
grid_trajectory_counts AS (
    SELECT 
        g.grid_id,
        g.grid_geom,
        COUNT(t.scene_token) AS trajectory_count,
        COUNT(DISTINCT t.city_id) AS unique_cities,
        AVG(ST_Area(t.geometry::geography)) AS avg_trajectory_area_m2,
        ST_Centroid(g.grid_geom) AS grid_center
    FROM grid_cells g
    LEFT JOIN clips_bbox t ON ST_Intersects(g.grid_geom, t.geometry)
    GROUP BY g.grid_id, g.grid_geom
)
SELECT 
    *,
    CASE 
        WHEN trajectory_count = 0 THEN 'empty'
        WHEN trajectory_count <= 5 THEN 'low_density'
        WHEN trajectory_count <= 20 THEN 'medium_density'
        WHEN trajectory_count <= 50 THEN 'high_density'
        ELSE 'very_high_density'
    END AS density_category
FROM grid_trajectory_counts;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_mv_density_grid_geom 
    ON mv_trajectory_density_analysis USING GIST(grid_geom);
CREATE INDEX IF NOT EXISTS idx_mv_density_category 
    ON mv_trajectory_density_analysis (density_category);

-- 3. 创建轨迹时空交集分析视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trajectory_spatiotemporal_intersections AS
WITH trajectory_pairs AS (
    SELECT 
        t1.scene_token AS scene_token_1,
        t1.data_name AS data_name_1,
        t1.timestamp AS timestamp_1,
        t1.city_id,
        t1.geometry AS geom_1,
        t2.scene_token AS scene_token_2,
        t2.data_name AS data_name_2,
        t2.timestamp AS timestamp_2,
        t2.geometry AS geom_2,
        ABS(t1.timestamp - t2.timestamp) AS time_diff_seconds,
        ST_Distance(
            ST_Transform(t1.geometry, 3857),
            ST_Transform(t2.geometry, 3857)
        ) AS spatial_distance_meters,
        ST_Area(
            ST_Intersection(
                ST_Buffer(ST_Transform(t1.geometry, 3857), 10),
                ST_Buffer(ST_Transform(t2.geometry, 3857), 10)
            )::geography
        ) AS intersection_area_m2
    FROM clips_bbox t1
    JOIN clips_bbox t2 ON t1.city_id = t2.city_id
    WHERE t1.scene_token < t2.scene_token  -- 避免重复配对
    AND ST_DWithin(
        ST_Transform(t1.geometry, 3857),
        ST_Transform(t2.geometry, 3857),
        100  -- 100米范围内
    )
    AND ABS(t1.timestamp - t2.timestamp) <= 3600  -- 1小时内
)
SELECT 
    *,
    CASE 
        WHEN time_diff_seconds <= 60 THEN 'simultaneous'
        WHEN time_diff_seconds <= 300 THEN 'near_simultaneous'
        WHEN time_diff_seconds <= 1800 THEN 'short_interval'
        ELSE 'long_interval'
    END AS temporal_category,
    CASE 
        WHEN spatial_distance_meters <= 10 THEN 'very_close'
        WHEN spatial_distance_meters <= 50 THEN 'close'
        WHEN spatial_distance_meters <= 100 THEN 'moderate'
        ELSE 'distant'
    END AS spatial_category,
    ST_Intersection(
        ST_Buffer(ST_Transform(geom_1, 3857), 10),
        ST_Buffer(ST_Transform(geom_2, 3857), 10)
    ) AS intersection_geom
FROM trajectory_pairs
WHERE intersection_area_m2 > 0;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_mv_spatiotemporal_city_id 
    ON mv_trajectory_spatiotemporal_intersections (city_id);
CREATE INDEX IF NOT EXISTS idx_mv_spatiotemporal_temporal_cat 
    ON mv_trajectory_spatiotemporal_intersections (temporal_category);
CREATE INDEX IF NOT EXISTS idx_mv_spatiotemporal_spatial_cat 
    ON mv_trajectory_spatiotemporal_intersections (spatial_category);

-- 4. 创建轨迹热点分析函数
CREATE OR REPLACE FUNCTION analyze_trajectory_hotspots(
    p_city_id TEXT DEFAULT NULL,
    p_buffer_meters FLOAT DEFAULT 50.0,
    p_min_trajectory_count INTEGER DEFAULT 5
)
RETURNS TABLE (
    hotspot_id INTEGER,
    center_point GEOMETRY,
    trajectory_count BIGINT,
    avg_area_m2 FLOAT,
    hotspot_level TEXT,
    city_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH clustered_trajectories AS (
        SELECT 
            t.scene_token,
            t.city_id,
            t.geometry,
            ST_ClusterDBSCAN(
                ST_Transform(t.geometry, 3857), 
                eps => p_buffer_meters, 
                minpoints => p_min_trajectory_count
            ) OVER() AS cluster_id
        FROM clips_bbox t
        WHERE (p_city_id IS NULL OR t.city_id = p_city_id)
        AND t.geometry IS NOT NULL
    ),
    hotspot_analysis AS (
        SELECT 
            cluster_id,
            ST_Centroid(ST_Union(geometry)) AS center_point,
            COUNT(*) AS trajectory_count,
            AVG(ST_Area(geometry::geography)) AS avg_area_m2,
            city_id
        FROM clustered_trajectories
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id, city_id
    )
    SELECT 
        cluster_id::INTEGER,
        center_point,
        trajectory_count,
        avg_area_m2::FLOAT,
        CASE 
            WHEN trajectory_count >= 50 THEN 'very_high'
            WHEN trajectory_count >= 20 THEN 'high'
            WHEN trajectory_count >= 10 THEN 'medium'
            ELSE 'low'
        END::TEXT,
        city_id
    FROM hotspot_analysis
    ORDER BY trajectory_count DESC;
END;
$$ LANGUAGE plpgsql;

-- 5. 创建轨迹覆盖度分析函数
CREATE OR REPLACE FUNCTION analyze_trajectory_coverage(
    p_city_id TEXT,
    p_reference_table TEXT DEFAULT 'intersections',
    p_buffer_meters FLOAT DEFAULT 30.0
)
RETURNS TABLE (
    reference_id TEXT,
    reference_type TEXT,
    total_trajectories BIGINT,
    covered_trajectories BIGINT,
    coverage_percentage FLOAT,
    avg_distance_meters FLOAT
) AS $$
DECLARE
    sql_query TEXT;
BEGIN
    sql_query := format('
        WITH reference_coverage AS (
            SELECT 
                r.inter_id AS reference_id,
                r.inter_type AS reference_type,
                COUNT(t.scene_token) AS total_trajectories,
                COUNT(CASE WHEN ST_DWithin(
                    ST_Transform(t.geometry, 3857),
                    ST_Transform(r.geom, 3857),
                    %L
                ) THEN 1 END) AS covered_trajectories,
                AVG(ST_Distance(
                    ST_Transform(t.geometry, 3857),
                    ST_Transform(r.geom, 3857)
                )) AS avg_distance_meters
            FROM %I r
            CROSS JOIN clips_bbox t
            WHERE r.city_id = %L AND t.city_id = %L
            GROUP BY r.inter_id, r.inter_type
        )
        SELECT 
            reference_id,
            reference_type,
            total_trajectories,
            covered_trajectories,
            ROUND((covered_trajectories::FLOAT / NULLIF(total_trajectories, 0)) * 100, 2) AS coverage_percentage,
            ROUND(avg_distance_meters, 2) AS avg_distance_meters
        FROM reference_coverage
        ORDER BY coverage_percentage DESC',
        p_buffer_meters, p_reference_table, p_city_id, p_city_id
    );
    
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

-- 6. 创建综合交集分析汇总视图
CREATE OR REPLACE VIEW v_intersection_analysis_summary AS
SELECT 
    'trajectory_junction' AS analysis_type,
    COUNT(*) AS total_intersections,
    COUNT(DISTINCT scene_token) AS unique_trajectories,
    COUNT(DISTINCT city_id) AS unique_cities,
    AVG(distance_meters) AS avg_distance_meters,
    AVG(intersection_area_m2) AS avg_intersection_area_m2,
    MIN(timestamp) AS earliest_timestamp,
    MAX(timestamp) AS latest_timestamp
FROM mv_trajectory_junction_intersections

UNION ALL

SELECT 
    'trajectory_spatiotemporal' AS analysis_type,
    COUNT(*) AS total_intersections,
    COUNT(DISTINCT scene_token_1) + COUNT(DISTINCT scene_token_2) AS unique_trajectories,
    COUNT(DISTINCT city_id) AS unique_cities,
    AVG(spatial_distance_meters) AS avg_distance_meters,
    AVG(intersection_area_m2) AS avg_intersection_area_m2,
    MIN(LEAST(timestamp_1, timestamp_2)) AS earliest_timestamp,
    MAX(GREATEST(timestamp_1, timestamp_2)) AS latest_timestamp
FROM mv_trajectory_spatiotemporal_intersections;

-- 7. 创建数据质量检查视图
CREATE OR REPLACE VIEW v_intersection_data_quality AS
WITH quality_checks AS (
    SELECT 
        'clips_bbox' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN geometry IS NULL THEN 1 END) AS null_geometries,
        COUNT(CASE WHEN NOT ST_IsValid(geometry) THEN 1 END) AS invalid_geometries,
        COUNT(CASE WHEN city_id IS NULL THEN 1 END) AS null_city_ids,
        COUNT(CASE WHEN timestamp IS NULL THEN 1 END) AS null_timestamps,
        MIN(timestamp) AS min_timestamp,
        MAX(timestamp) AS max_timestamp,
        ST_Extent(geometry) AS spatial_extent
    FROM clips_bbox
    
    UNION ALL
    
    SELECT 
        'intersections' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN geom IS NULL THEN 1 END) AS null_geometries,
        COUNT(CASE WHEN NOT ST_IsValid(geom) THEN 1 END) AS invalid_geometries,
        COUNT(CASE WHEN city_id IS NULL THEN 1 END) AS null_city_ids,
        NULL AS null_timestamps,
        NULL AS min_timestamp,
        NULL AS max_timestamp,
        ST_Extent(geom) AS spatial_extent
    FROM intersections
)
SELECT 
    *,
    ROUND((null_geometries::FLOAT / total_records) * 100, 2) AS null_geometry_percentage,
    ROUND((invalid_geometries::FLOAT / total_records) * 100, 2) AS invalid_geometry_percentage
FROM quality_checks;

-- 刷新物化视图的便利函数
CREATE OR REPLACE FUNCTION refresh_intersection_materialized_views()
RETURNS TEXT AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_trajectory_junction_intersections;
    REFRESH MATERIALIZED VIEW mv_trajectory_density_analysis;
    REFRESH MATERIALIZED VIEW mv_trajectory_spatiotemporal_intersections;
    
    RETURN 'All intersection materialized views refreshed successfully at ' || CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- 添加注释
COMMENT ON MATERIALIZED VIEW mv_trajectory_junction_intersections IS '轨迹与路口交集分析的物化视图';
COMMENT ON MATERIALIZED VIEW mv_trajectory_density_analysis IS '轨迹密度分析的物化视图，基于网格单元';
COMMENT ON MATERIALIZED VIEW mv_trajectory_spatiotemporal_intersections IS '轨迹时空交集分析的物化视图';
COMMENT ON FUNCTION analyze_trajectory_hotspots IS '分析轨迹热点，使用DBSCAN聚类算法';
COMMENT ON FUNCTION analyze_trajectory_coverage IS '分析轨迹对参考要素的覆盖度';
COMMENT ON VIEW v_intersection_analysis_summary IS '交集分析结果汇总视图';
COMMENT ON VIEW v_intersection_data_quality IS '数据质量检查视图';
COMMENT ON FUNCTION refresh_intersection_materialized_views IS '刷新所有交集分析相关的物化视图'; 