-- ========================================
-- QGIS兼容视图创建脚本
-- ========================================
-- 
-- 功能：为BBox叠置分析结果创建QGIS兼容的视图
-- 目标：让QGIS能够正确加载和显示分析结果
-- 
-- QGIS要求：
-- 1. 必须有唯一的整数主键列
-- 2. 几何列必须有正确的SRID
-- 3. 数据类型必须兼容QGIS的渲染引擎
-- 
-- 创建的视图：
-- - qgis_bbox_overlap_hotspots: 主要的热点视图
-- - qgis_bbox_overlap_summary: 汇总统计视图
-- ========================================

-- ----------------------------------------
-- 主热点视图：用于在QGIS中显示重叠热点
-- ----------------------------------------
CREATE OR REPLACE VIEW qgis_bbox_overlap_hotspots AS
SELECT 
    -- QGIS主键：必须是唯一的整数
    id,
    
    -- 分析标识信息
    analysis_id,
    analysis_type,
    hotspot_rank,
    
    -- 重叠统计信息
    overlap_count,
    total_overlap_area,
    subdataset_count,
    scene_count,
    
    -- 涉及的数据集和场景信息
    involved_subdatasets,
    involved_scenes,
    
    -- 密度等级分类（用于QGIS符号化）
    CASE 
        WHEN overlap_count >= 20 THEN 'Very High Density'
        WHEN overlap_count >= 10 THEN 'High Density'
        WHEN overlap_count >= 5 THEN 'Medium Density'
        WHEN overlap_count >= 2 THEN 'Low Density'
        ELSE 'Single Overlap'
    END as density_level,
    
    -- 面积等级分类
    CASE 
        WHEN total_overlap_area >= 0.01 THEN 'Large Area'
        WHEN total_overlap_area >= 0.001 THEN 'Medium Area'
        ELSE 'Small Area'
    END as area_level,
    
    -- 热点重要性评分（综合重叠数量和面积）
    ROUND(
        (overlap_count * 0.7 + LOG(1 + total_overlap_area * 10000) * 0.3)::numeric, 
        2
    ) as hotspot_score,
    
    -- 格式化的显示文本（用于QGIS标签）
    overlap_count || ' overlaps' as overlap_label,
    'Rank ' || hotspot_rank || ': ' || overlap_count || ' overlaps' as rank_label,
    ROUND(total_overlap_area::numeric, 4) || ' sq.deg' as area_label,
    
    -- 几何信息（确保SRID正确）
    ST_Force2D(geometry) as geometry,
    
    -- 时间信息
    created_at,
    analysis_time,
    
    -- 分析参数（解析JSON为可读字段）
    analysis_params
    
FROM bbox_overlap_analysis_results
WHERE analysis_type = 'bbox_overlap'
ORDER BY hotspot_rank;

-- ----------------------------------------
-- 汇总统计视图：用于显示分析批次的整体统计
-- ----------------------------------------
CREATE OR REPLACE VIEW qgis_bbox_overlap_summary AS
SELECT 
    -- 为每个分析ID创建唯一的ID
    ROW_NUMBER() OVER (ORDER BY analysis_id) as id,
    
    -- 分析基本信息
    analysis_id,
    analysis_type,
    MIN(analysis_time) as analysis_time,
    
    -- 热点统计
    COUNT(*) as total_hotspots,
    MAX(overlap_count) as max_overlap_count,
    MIN(overlap_count) as min_overlap_count,
    ROUND(AVG(overlap_count)::numeric, 2) as avg_overlap_count,
    
    -- 面积统计
    ROUND(SUM(total_overlap_area)::numeric, 4) as total_area_sum,
    ROUND(AVG(total_overlap_area)::numeric, 6) as avg_area,
    ROUND(MAX(total_overlap_area)::numeric, 4) as max_area,
    
    -- 数据集统计
    MAX(subdataset_count) as max_subdatasets_involved,
    ROUND(AVG(subdataset_count)::numeric, 2) as avg_subdatasets_involved,
    
    -- 场景统计
    MAX(scene_count) as max_scenes_involved,
    ROUND(AVG(scene_count)::numeric, 2) as avg_scenes_involved,
    
    -- 密度分布统计
    COUNT(CASE WHEN overlap_count >= 10 THEN 1 END) as high_density_count,
    COUNT(CASE WHEN overlap_count >= 5 AND overlap_count < 10 THEN 1 END) as medium_density_count,
    COUNT(CASE WHEN overlap_count < 5 THEN 1 END) as low_density_count,
    
    -- 整体评分
    ROUND(
        AVG(overlap_count * 0.7 + LOG(1 + total_overlap_area * 10000) * 0.3)::numeric, 
        2
    ) as overall_score,
    
    -- 分析质量指标
    CASE 
        WHEN COUNT(*) >= 10 AND MAX(overlap_count) >= 5 THEN 'High Quality'
        WHEN COUNT(*) >= 5 AND MAX(overlap_count) >= 3 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as analysis_quality,
    
    -- 创建覆盖整个分析区域的几何对象（用于显示分析范围）
    ST_ConvexHull(ST_Collect(geometry)) as analysis_boundary
    
FROM bbox_overlap_analysis_results
WHERE analysis_type = 'bbox_overlap'
GROUP BY analysis_id, analysis_type;

-- ----------------------------------------
-- 详细热点信息视图：包含扩展的分析信息
-- ----------------------------------------
CREATE OR REPLACE VIEW qgis_bbox_overlap_details AS
SELECT 
    -- 主键和基本信息
    r.id,
    r.analysis_id,
    r.hotspot_rank,
    r.overlap_count,
    r.total_overlap_area,
    
    -- 几何信息和度量
    ST_Force2D(r.geometry) as geometry,
    ST_Area(r.geometry) as hotspot_area,
    ST_Perimeter(r.geometry) as hotspot_perimeter,
    ST_X(ST_Centroid(r.geometry)) as centroid_lon,
    ST_Y(ST_Centroid(r.geometry)) as centroid_lat,
    
    -- 形状复杂性指标
    ROUND(
        (ST_Perimeter(r.geometry) / (2 * SQRT(PI() * ST_Area(r.geometry))))::numeric, 
        3
    ) as shape_complexity,
    
    -- 密度指标
    ROUND(
        (r.overlap_count / NULLIF(ST_Area(r.geometry), 0))::numeric, 
        2
    ) as overlap_density,
    
    -- 数据集多样性
    r.subdataset_count,
    r.scene_count,
    ROUND(
        (r.scene_count::float / NULLIF(r.subdataset_count, 0))::numeric, 
        2
    ) as scenes_per_subdataset,
    
    -- 涉及的数据集列表（截取前5个显示）
    CASE 
        WHEN array_length(r.involved_subdatasets, 1) <= 5 
        THEN array_to_string(r.involved_subdatasets, ', ')
        ELSE array_to_string(r.involved_subdatasets[1:5], ', ') || '...'
    END as subdatasets_display,
    
    -- 完整的数据集和场景信息
    r.involved_subdatasets,
    r.involved_scenes,
    
    -- 分析参数信息
    r.analysis_params,
    r.created_at
    
FROM bbox_overlap_analysis_results r
WHERE r.analysis_type = 'bbox_overlap'
ORDER BY r.hotspot_rank;

-- ----------------------------------------
-- 创建索引以优化QGIS查询性能
-- ----------------------------------------

-- 为视图中经常用作过滤条件的字段创建索引
-- （注意：视图本身不能创建索引，但底层表的索引会被利用）

-- 添加视图注释，帮助QGIS用户理解字段含义
COMMENT ON VIEW qgis_bbox_overlap_hotspots IS 'QGIS兼容的bbox重叠热点视图，包含密度分级和格式化标签';
COMMENT ON VIEW qgis_bbox_overlap_summary IS 'bbox重叠分析的汇总统计视图，按分析批次聚合';
COMMENT ON VIEW qgis_bbox_overlap_details IS 'bbox重叠热点的详细信息视图，包含几何度量和复杂性指标';

-- ----------------------------------------
-- 显示创建结果
-- ----------------------------------------
SELECT 
    'QGIS视图创建完成' as status,
    COUNT(*) as total_views_created
FROM information_schema.views 
WHERE table_name LIKE 'qgis_bbox_overlap%';

-- 显示视图中的数据概况
SELECT 
    'qgis_bbox_overlap_hotspots' as view_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT analysis_id) as analysis_count
FROM qgis_bbox_overlap_hotspots

UNION ALL

SELECT 
    'qgis_bbox_overlap_summary' as view_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT analysis_id) as analysis_count
FROM qgis_bbox_overlap_summary;
