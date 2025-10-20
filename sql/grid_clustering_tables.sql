-- Grid轨迹聚类分析结果表
-- 用于存储grid内轨迹段的聚类分析结果

-- ============================================
-- 1. 轨迹段详细表
-- ============================================
DROP TABLE IF EXISTS grid_trajectory_segments CASCADE;

CREATE TABLE grid_trajectory_segments (
    id SERIAL PRIMARY KEY,
    
    -- Grid信息
    grid_id INTEGER NOT NULL,
    city_id TEXT NOT NULL,
    analysis_id TEXT NOT NULL,
    
    -- 轨迹标识
    dataset_name TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    
    -- 时间信息
    start_time BIGINT NOT NULL,
    end_time BIGINT NOT NULL,
    duration NUMERIC(12, 2),  -- 秒
    point_count INTEGER,
    
    -- 速度特征
    avg_speed NUMERIC(8, 2),
    std_speed NUMERIC(8, 2),
    max_speed NUMERIC(8, 2),
    min_speed NUMERIC(8, 2),
    
    -- 加速度特征
    avg_acceleration NUMERIC(8, 4),
    std_acceleration NUMERIC(8, 4),
    
    -- 航向角特征
    avg_yaw NUMERIC(8, 4),
    yaw_change_rate NUMERIC(8, 4),
    std_yaw NUMERIC(8, 4),
    
    -- 轨迹形态特征
    trajectory_length_m NUMERIC(12, 2),
    direction_cos NUMERIC(8, 4),
    direction_sin NUMERIC(8, 4),
    curvature NUMERIC(8, 4),
    
    -- 质量标记
    quality_flag TEXT,  -- valid, stationary, gps_jump, excessive_speed, etc.
    
    -- 聚类结果
    cluster_label INTEGER,  -- -1表示噪声点
    
    -- 几何
    geometry GEOMETRY(LineString, 4326),
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_grid_traj_seg_grid_id ON grid_trajectory_segments(grid_id);
CREATE INDEX idx_grid_traj_seg_city_id ON grid_trajectory_segments(city_id);
CREATE INDEX idx_grid_traj_seg_analysis_id ON grid_trajectory_segments(analysis_id);
CREATE INDEX idx_grid_traj_seg_dataset ON grid_trajectory_segments(dataset_name);
CREATE INDEX idx_grid_traj_seg_cluster ON grid_trajectory_segments(cluster_label);
CREATE INDEX idx_grid_traj_seg_quality ON grid_trajectory_segments(quality_flag);
CREATE INDEX idx_grid_traj_seg_geom ON grid_trajectory_segments USING GIST(geometry);
CREATE INDEX idx_grid_traj_seg_start_time ON grid_trajectory_segments(start_time);

-- 复合索引
CREATE INDEX idx_grid_traj_seg_grid_cluster ON grid_trajectory_segments(grid_id, cluster_label);
CREATE INDEX idx_grid_traj_seg_city_analysis ON grid_trajectory_segments(city_id, analysis_id);

COMMENT ON TABLE grid_trajectory_segments IS 'Grid轨迹段聚类分析详细结果';
COMMENT ON COLUMN grid_trajectory_segments.quality_flag IS '质量标记：valid, stationary, gps_jump, excessive_speed, insufficient_points';
COMMENT ON COLUMN grid_trajectory_segments.cluster_label IS '聚类标签，-1表示噪声点';


-- ============================================
-- 2. 聚类统计摘要表
-- ============================================
DROP TABLE IF EXISTS grid_clustering_summary CASCADE;

CREATE TABLE grid_clustering_summary (
    id SERIAL PRIMARY KEY,
    
    -- Grid信息
    grid_id INTEGER NOT NULL,
    city_id TEXT NOT NULL,
    analysis_id TEXT NOT NULL,
    
    -- 聚类标识
    cluster_label INTEGER NOT NULL,  -- -1表示噪声点
    
    -- 聚类统计
    segment_count INTEGER NOT NULL,  -- 该聚类包含的轨迹段数量
    
    -- 聚类中心特征
    centroid_avg_speed NUMERIC(8, 2),
    centroid_avg_acceleration NUMERIC(8, 4),
    centroid_yaw_change_rate NUMERIC(8, 4),
    centroid_direction_cos NUMERIC(8, 4),
    centroid_direction_sin NUMERIC(8, 4),
    
    -- 语义标签
    speed_range TEXT,       -- 速度范围标签，如："中速(5-10m/s)"
    behavior_label TEXT,    -- 行为标签，如："直行通过", "转弯/变道", "减速/刹车"
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_grid_clust_sum_grid_id ON grid_clustering_summary(grid_id);
CREATE INDEX idx_grid_clust_sum_city_id ON grid_clustering_summary(city_id);
CREATE INDEX idx_grid_clust_sum_analysis_id ON grid_clustering_summary(analysis_id);
CREATE INDEX idx_grid_clust_sum_cluster ON grid_clustering_summary(cluster_label);
CREATE INDEX idx_grid_clust_sum_behavior ON grid_clustering_summary(behavior_label);

-- 复合索引
CREATE INDEX idx_grid_clust_sum_grid_cluster ON grid_clustering_summary(grid_id, cluster_label);
CREATE INDEX idx_grid_clust_sum_city_analysis ON grid_clustering_summary(city_id, analysis_id);

COMMENT ON TABLE grid_clustering_summary IS 'Grid轨迹聚类统计摘要';
COMMENT ON COLUMN grid_clustering_summary.cluster_label IS '聚类标签，-1表示噪声点';
COMMENT ON COLUMN grid_clustering_summary.behavior_label IS '行为标签：直行通过、转弯/变道、加速、减速/刹车、缓慢移动、噪声/异常';


-- ============================================
-- 3. 创建视图：便于查询
-- ============================================

-- 有效轨迹段视图（过滤掉质量不合格的）
CREATE OR REPLACE VIEW grid_trajectory_segments_valid AS
SELECT *
FROM grid_trajectory_segments
WHERE quality_flag = 'valid';

COMMENT ON VIEW grid_trajectory_segments_valid IS '只包含质量合格的轨迹段';


-- 聚类详情视图（关联轨迹段和聚类摘要）
CREATE OR REPLACE VIEW grid_clustering_details AS
SELECT 
    s.id,
    s.grid_id,
    s.city_id,
    s.analysis_id,
    s.dataset_name,
    s.segment_index,
    s.cluster_label,
    c.behavior_label,
    c.speed_range,
    s.avg_speed,
    s.avg_acceleration,
    s.yaw_change_rate,
    s.trajectory_length_m,
    s.duration,
    s.point_count,
    s.geometry
FROM grid_trajectory_segments s
LEFT JOIN grid_clustering_summary c 
    ON s.grid_id = c.grid_id 
    AND s.cluster_label = c.cluster_label
WHERE s.quality_flag = 'valid';

COMMENT ON VIEW grid_clustering_details IS '轨迹段与聚类标签的关联详情';


-- ============================================
-- 4. 统计查询示例
-- ============================================

-- 查询某个城市的聚类统计
/*
SELECT 
    city_id,
    COUNT(DISTINCT grid_id) as grid_count,
    COUNT(*) as total_segments,
    COUNT(*) FILTER (WHERE quality_flag = 'valid') as valid_segments,
    COUNT(DISTINCT cluster_label) as cluster_count,
    COUNT(*) FILTER (WHERE cluster_label = -1) as noise_count
FROM grid_trajectory_segments
WHERE city_id = 'A72'
GROUP BY city_id;
*/

-- 查询某个grid的聚类分布
/*
SELECT 
    cluster_label,
    behavior_label,
    segment_count,
    speed_range,
    centroid_avg_speed
FROM grid_clustering_summary
WHERE grid_id = 12963
ORDER BY segment_count DESC;
*/

-- 查询所有行为类型的分布
/*
SELECT 
    behavior_label,
    COUNT(*) as cluster_count,
    SUM(segment_count) as total_segments,
    AVG(centroid_avg_speed) as avg_speed
FROM grid_clustering_summary
WHERE city_id = 'A72'
GROUP BY behavior_label
ORDER BY total_segments DESC;
*/
