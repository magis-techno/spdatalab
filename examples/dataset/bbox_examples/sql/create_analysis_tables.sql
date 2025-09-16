-- ========================================
-- BBox叠置分析结果表创建脚本
-- ========================================
-- 
-- 功能：创建用于存储bbox叠置分析结果的表结构
-- 作者：spdatalab
-- 日期：2024
-- 
-- 使用方法：
--   psql -d postgres -f create_analysis_tables.sql
--   或在Python中通过SQLAlchemy执行
-- 
-- ========================================

-- 创建bbox叠置分析结果表
CREATE TABLE IF NOT EXISTS bbox_overlap_analysis_results (
    -- 基础字段
    id SERIAL PRIMARY KEY,
    analysis_id VARCHAR(100) NOT NULL,
    analysis_type VARCHAR(50) DEFAULT 'bbox_overlap',
    analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 热点排名和统计信息
    hotspot_rank INTEGER,                    -- 热点排名（按重叠数量降序）
    overlap_count INTEGER,                   -- 重叠数量
    total_overlap_area NUMERIC,              -- 总重叠面积（平方度）
    subdataset_count INTEGER,                -- 涉及的子数据集数量
    scene_count INTEGER,                     -- 涉及的场景数量
    
    -- 详细信息（数组类型）
    involved_subdatasets TEXT[],             -- 涉及的子数据集列表
    involved_scenes TEXT[],                  -- 涉及的场景token列表
    
    -- 分析参数和时间戳
    analysis_params TEXT,                    -- JSON格式的分析参数
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 约束
    CONSTRAINT chk_overlap_count_positive CHECK (overlap_count > 0),
    CONSTRAINT chk_hotspot_rank_positive CHECK (hotspot_rank > 0),
    CONSTRAINT chk_area_non_negative CHECK (total_overlap_area >= 0)
);

-- 添加PostGIS几何列
-- 使用DO块来检查几何列是否已存在，避免重复添加
DO $$
BEGIN
    -- 检查几何列是否已存在
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public'
        AND table_name = 'bbox_overlap_analysis_results' 
        AND column_name = 'geometry'
    ) THEN
        -- 添加几何列：2D几何对象，SRID=4326（WGS84）
        PERFORM AddGeometryColumn(
            'public', 
            'bbox_overlap_analysis_results', 
            'geometry', 
            4326, 
            'GEOMETRY', 
            2
        );
        
        RAISE NOTICE '几何列已添加到 bbox_overlap_analysis_results 表';
    ELSE
        RAISE NOTICE '几何列已存在于 bbox_overlap_analysis_results 表';
    END IF;
END $$;

-- 创建索引以优化查询性能
-- 分析ID索引（最常用的过滤条件）
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_analysis_id 
ON bbox_overlap_analysis_results (analysis_id);

-- 热点排名索引（用于TOP N查询）
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_rank 
ON bbox_overlap_analysis_results (hotspot_rank);

-- 重叠数量索引（用于密度过滤）
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_count 
ON bbox_overlap_analysis_results (overlap_count);

-- 分析类型索引
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_type 
ON bbox_overlap_analysis_results (analysis_type);

-- 时间索引（用于历史分析）
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_time 
ON bbox_overlap_analysis_results (analysis_time);

-- 空间索引（PostGIS GIST索引，用于空间查询）
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_geom 
ON bbox_overlap_analysis_results USING GIST (geometry);

-- 复合索引（分析ID + 排名，用于特定分析的热点查询）
CREATE INDEX IF NOT EXISTS idx_bbox_overlap_analysis_rank 
ON bbox_overlap_analysis_results (analysis_id, hotspot_rank);

-- 添加表注释
COMMENT ON TABLE bbox_overlap_analysis_results IS 'BBox叠置分析结果表，存储重叠热点的统计信息和几何数据';
COMMENT ON COLUMN bbox_overlap_analysis_results.analysis_id IS '分析批次ID，用于区分不同次分析';
COMMENT ON COLUMN bbox_overlap_analysis_results.hotspot_rank IS '热点排名，1表示重叠数量最多的区域';
COMMENT ON COLUMN bbox_overlap_analysis_results.overlap_count IS '该热点区域的重叠bbox数量';
COMMENT ON COLUMN bbox_overlap_analysis_results.total_overlap_area IS '总重叠面积，单位为平方度';
COMMENT ON COLUMN bbox_overlap_analysis_results.involved_subdatasets IS '涉及的子数据集名称数组';
COMMENT ON COLUMN bbox_overlap_analysis_results.involved_scenes IS '涉及的场景token数组';
COMMENT ON COLUMN bbox_overlap_analysis_results.geometry IS '重叠热点的几何形状';
COMMENT ON COLUMN bbox_overlap_analysis_results.analysis_params IS 'JSON格式的分析参数，包含过滤条件等';

-- 显示创建结果
SELECT 
    'bbox_overlap_analysis_results表创建完成' as status,
    COUNT(*) as current_records
FROM bbox_overlap_analysis_results;

-- 显示表结构信息
\d+ bbox_overlap_analysis_results;
