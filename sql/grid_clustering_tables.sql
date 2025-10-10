-- ==========================================
-- Grid轨迹聚类分析表结构定义
-- ==========================================
-- 
-- 用途：存储基于city_hotspots的grid轨迹聚类分析结果
-- 
-- 表1: grid_trajectory_segments - 轨迹段特征和聚类标签
-- 表2: grid_clustering_summary - 聚类统计汇总
-- 
-- 使用方法：
--   psql -h local_pg -U postgres -d postgres -f sql/grid_clustering_tables.sql
-- ==========================================

-- ==========================================
-- 表1: grid_trajectory_segments
-- 存储每个轨迹段的详细特征和聚类结果
-- ==========================================

CREATE TABLE IF NOT EXISTS grid_trajectory_segments (
    id SERIAL PRIMARY KEY,
    
    -- 关联信息
    grid_id INTEGER NOT NULL,              -- 关联city_hotspots.id
    city_id VARCHAR(50),                   -- 冗余city_id便于查询
    analysis_id VARCHAR(100),              -- 关联的分析批次
    
    -- 轨迹基本信息
    dataset_name TEXT NOT NULL,            -- 轨迹ID（dataset_name）
    segment_index INTEGER,                 -- 该轨迹的第几段
    start_time BIGINT,                     -- 起始时间戳
    end_time BIGINT,                       -- 结束时间戳
    duration NUMERIC(8,2),                 -- 持续时间（秒）
    point_count INTEGER,                   -- 点数量
    
    -- 速度特征（4维）
    avg_speed NUMERIC(8,3),                -- 平均速度 m/s
    std_speed NUMERIC(8,3),                -- 速度标准差
    max_speed NUMERIC(8,3),                -- 最大速度
    min_speed NUMERIC(8,3),                -- 最小速度
    
    -- 加速度特征（2维）
    avg_acceleration NUMERIC(8,3),         -- 平均加速度 m/s²
    std_acceleration NUMERIC(8,3),         -- 加速度标准差
    
    -- 航向角特征（3维）
    avg_yaw NUMERIC(8,4),                  -- 平均航向角 rad
    yaw_change_rate NUMERIC(8,4),          -- 航向角变化率 rad/s
    std_yaw NUMERIC(8,4),                  -- 航向角标准差
    
    -- 轨迹形态特征（4维）
    trajectory_length_m NUMERIC(10,2),     -- 轨迹段长度（米）
    direction_cos NUMERIC(8,4),            -- 起终点方向余弦
    direction_sin NUMERIC(8,4),            -- 起终点方向正弦
    curvature NUMERIC(8,4),                -- 弯曲度（实际路径/直线距离）
    
    -- 质量标记
    quality_flag VARCHAR(50),              -- 过滤原因或'valid'
    
    -- 聚类结果
    cluster_label INTEGER,                 -- 聚类标签，-1表示噪声点
    cluster_probability NUMERIC(8,4),      -- 聚类置信度（可选）
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加几何列（轨迹段线段）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'grid_trajectory_segments' 
        AND column_name = 'geometry'
    ) THEN
        PERFORM AddGeometryColumn('public', 'grid_trajectory_segments', 'geometry', 4326, 'LINESTRING', 2);
    END IF;
END $$;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_segments_grid ON grid_trajectory_segments(grid_id);
CREATE INDEX IF NOT EXISTS idx_segments_city ON grid_trajectory_segments(city_id);
CREATE INDEX IF NOT EXISTS idx_segments_cluster ON grid_trajectory_segments(cluster_label);
CREATE INDEX IF NOT EXISTS idx_segments_analysis ON grid_trajectory_segments(analysis_id);
CREATE INDEX IF NOT EXISTS idx_segments_quality ON grid_trajectory_segments(quality_flag);
CREATE INDEX IF NOT EXISTS idx_segments_dataset ON grid_trajectory_segments(dataset_name);
CREATE INDEX IF NOT EXISTS idx_segments_geom ON grid_trajectory_segments USING GIST(geometry);

-- 添加表注释
COMMENT ON TABLE grid_trajectory_segments IS 'Grid轨迹段特征表，存储每个轨迹段的详细特征和聚类标签';
COMMENT ON COLUMN grid_trajectory_segments.grid_id IS '关联city_hotspots表的id';
COMMENT ON COLUMN grid_trajectory_segments.cluster_label IS '聚类标签，-1表示噪声点，>=0表示正常聚类';
COMMENT ON COLUMN grid_trajectory_segments.quality_flag IS '质量标记：valid/insufficient_points/stationary/gps_jump/excessive_speed';
COMMENT ON COLUMN grid_trajectory_segments.curvature IS '轨迹弯曲度，>1表示弯曲，=1表示直线';


-- ==========================================
-- 表2: grid_clustering_summary
-- 存储每个grid的聚类统计汇总
-- ==========================================

CREATE TABLE IF NOT EXISTS grid_clustering_summary (
    id SERIAL PRIMARY KEY,
    
    -- 关联信息
    grid_id INTEGER NOT NULL,              -- 关联city_hotspots.id
    city_id VARCHAR(50),                   -- 城市ID
    analysis_id VARCHAR(100),              -- 分析批次ID
    cluster_label INTEGER NOT NULL,        -- 聚类标签
    
    -- 统计信息
    segment_count INTEGER,                 -- 该聚类的轨迹段数量
    
    -- 聚类中心特征（5维）
    centroid_avg_speed NUMERIC(8,3),       -- 中心平均速度
    centroid_avg_acceleration NUMERIC(8,3),-- 中心平均加速度
    centroid_yaw_change_rate NUMERIC(8,4), -- 中心航向角变化率
    centroid_direction_cos NUMERIC(8,4),   -- 中心方向余弦
    centroid_direction_sin NUMERIC(8,4),   -- 中心方向正弦
    
    -- 统计特征（可读性标签）
    speed_range TEXT,                      -- 速度范围描述，如"低速(0-5m/s)"
    behavior_label TEXT,                   -- 行为标签，如"直行通过"、"减速转弯"
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_summary_grid ON grid_clustering_summary(grid_id);
CREATE INDEX IF NOT EXISTS idx_summary_city ON grid_clustering_summary(city_id);
CREATE INDEX IF NOT EXISTS idx_summary_cluster ON grid_clustering_summary(cluster_label);
CREATE INDEX IF NOT EXISTS idx_summary_analysis ON grid_clustering_summary(analysis_id);

-- 添加表注释
COMMENT ON TABLE grid_clustering_summary IS 'Grid聚类统计汇总表，存储每个grid的聚类中心和统计信息';
COMMENT ON COLUMN grid_clustering_summary.cluster_label IS '聚类标签，-1表示噪声点统计';
COMMENT ON COLUMN grid_clustering_summary.behavior_label IS '行为标签：直行通过/减速转弯/快速通过/慢速移动等';


-- ==========================================
-- 查看表结构
-- ==========================================

\echo '==========================================';
\echo 'Grid轨迹聚类表创建完成！';
\echo '==========================================';
\echo '';
\echo '表1: grid_trajectory_segments';
SELECT 
    column_name,
    data_type,
    column_default
FROM information_schema.columns 
WHERE table_name = 'grid_trajectory_segments' 
    AND table_schema = 'public'
ORDER BY ordinal_position;

\echo '';
\echo '表2: grid_clustering_summary';
SELECT 
    column_name,
    data_type,
    column_default
FROM information_schema.columns 
WHERE table_name = 'grid_clustering_summary' 
    AND table_schema = 'public'
ORDER BY ordinal_position;

\echo '';
\echo '索引列表:';
SELECT 
    tablename, 
    indexname 
FROM pg_indexes 
WHERE schemaname = 'public' 
    AND (tablename = 'grid_trajectory_segments' OR tablename = 'grid_clustering_summary')
ORDER BY tablename, indexname;


