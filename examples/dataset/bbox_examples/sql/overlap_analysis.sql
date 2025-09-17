-- ========================================
-- BBox叠置分析核心SQL查询
-- ========================================
-- 
-- 功能：执行bbox数据的空间叠置分析，识别重叠热点
-- 输入：clips_bbox_unified_qgis视图中的bbox数据
-- 输出：插入到bbox_overlap_analysis_results表的分析结果
-- 
-- 分析逻辑：
-- 1. 找出所有空间相交的bbox对
-- 2. 计算重叠面积和几何形状
-- 3. 按空间网格聚合相邻的重叠区域
-- 4. 统计每个热点的重叠数量和涉及的数据集
-- 5. 按重叠数量排序，生成热点排名
-- 
-- 参数说明（需要在执行前替换）：
--   {unified_view}      - 统一视图名称，通常为 clips_bbox_unified_qgis
--   {analysis_table}    - 结果表名称，通常为 bbox_overlap_analysis_results  
--   {analysis_id}       - 本次分析的唯一ID
--   {where_clause}      - 额外的过滤条件（可选）
--   {min_overlap_area}  - 最小重叠面积阈值
--   {top_n}            - 返回的热点数量
-- 
-- 使用示例：
--   替换参数后直接在PostgreSQL中执行
--   或通过Python的format()方法替换参数
-- ========================================

-- 清理可能的重复分析结果
DELETE FROM {analysis_table} 
WHERE analysis_id = '{analysis_id}';

-- 主分析查询
WITH 
-- Step 1: 找出所有空间相交的bbox对
overlapping_pairs AS (
    SELECT 
        a.qgis_id as bbox_a_id,
        b.qgis_id as bbox_b_id,
        a.subdataset_name as subdataset_a,
        b.subdataset_name as subdataset_b,
        a.scene_token as scene_a,
        b.scene_token as scene_b,
        a.data_name as data_name_a,
        b.data_name as data_name_b,
        a.city_id as city_a,
        b.city_id as city_b,
        
        -- 计算重叠几何和面积
        ST_Intersection(a.geometry, b.geometry) as overlap_geometry,
        ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area,
        
        -- 计算重叠比例（相对于两个bbox的面积）
        ST_Area(ST_Intersection(a.geometry, b.geometry)) / NULLIF(ST_Area(a.geometry), 0) as overlap_ratio_a,
        ST_Area(ST_Intersection(a.geometry, b.geometry)) / NULLIF(ST_Area(b.geometry), 0) as overlap_ratio_b
        
    FROM {unified_view} a
    JOIN {unified_view} b ON a.qgis_id < b.qgis_id  -- 避免自连接和重复对
    WHERE 
        -- 基本空间相交条件
        ST_Intersects(a.geometry, b.geometry)
        -- 重叠面积大于阈值
        AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {min_overlap_area}
        -- 排除完全相同的几何对象
        AND NOT ST_Equals(a.geometry, b.geometry)
        -- 🎯 只分析相同城市的bbox（核心优化）
        AND a.city_id = b.city_id
        AND a.city_id IS NOT NULL
        -- 🎯 只分析质量合格的数据（all_good=true）
        AND a.all_good = true
        AND b.all_good = true
        -- 用户自定义过滤条件
        {where_clause}
),

-- Step 2: 基于空间位置聚合重叠区域，形成热点
-- 使用网格快照将相邻的重叠区域合并
overlap_hotspots AS (
    SELECT 
        -- 使用网格快照作为分组键（精度0.001度约100米）
        ST_SnapToGrid(ST_Centroid(overlap_geometry), 0.001) as grid_point,
        
        -- 合并几何：将相邻的重叠区域合并成一个热点
        ST_Union(overlap_geometry) as hotspot_geometry,
        
        -- 统计重叠数量
        COUNT(*) as overlap_count,
        
        -- 收集涉及的子数据集（去重）
        ARRAY_AGG(DISTINCT subdataset_a) || ARRAY_AGG(DISTINCT subdataset_b) as all_subdatasets,
        
        -- 收集涉及的场景token（去重）  
        ARRAY_AGG(DISTINCT scene_a) || ARRAY_AGG(DISTINCT scene_b) as all_scenes,
        
        -- 收集涉及的数据名称（去重）
        ARRAY_AGG(DISTINCT data_name_a) || ARRAY_AGG(DISTINCT data_name_b) as all_data_names,
        
        -- 统计面积信息
        SUM(overlap_area) as total_overlap_area,
        AVG(overlap_area) as avg_overlap_area,
        MAX(overlap_area) as max_overlap_area,
        
        -- 统计重叠比例
        AVG(GREATEST(overlap_ratio_a, overlap_ratio_b)) as avg_max_overlap_ratio,
        
        -- 收集城市信息
        ARRAY_AGG(DISTINCT city_a) || ARRAY_AGG(DISTINCT city_b) as involved_cities
        
    FROM overlapping_pairs
    GROUP BY ST_SnapToGrid(ST_Centroid(overlap_geometry), 0.001)
    -- 过滤掉重叠数量太少的区域
    HAVING COUNT(*) >= 2
),

-- Step 3: 清理和标准化数据
final_hotspots AS (
    SELECT 
        -- 过滤和去重数组中的空值
        ARRAY_REMOVE(ARRAY(SELECT DISTINCT unnest(all_subdatasets) ORDER BY 1), NULL) as involved_subdatasets,
        ARRAY_REMOVE(ARRAY(SELECT DISTINCT unnest(all_scenes) ORDER BY 1), NULL) as involved_scenes,
        ARRAY_REMOVE(ARRAY(SELECT DISTINCT unnest(all_data_names) ORDER BY 1), NULL) as involved_data_names,
        ARRAY_REMOVE(ARRAY(SELECT DISTINCT unnest(involved_cities) ORDER BY 1), NULL) as involved_cities,
        
        -- 几何和统计信息
        hotspot_geometry,
        overlap_count,
        total_overlap_area,
        avg_overlap_area,
        max_overlap_area,
        avg_max_overlap_ratio
        
    FROM overlap_hotspots
)

-- Step 4: 插入最终结果到分析表
INSERT INTO {analysis_table} (
    analysis_id,
    analysis_type,
    hotspot_rank,
    overlap_count,
    total_overlap_area,
    subdataset_count,
    scene_count,
    involved_subdatasets,
    involved_scenes,
    geometry,
    analysis_params
)
SELECT 
    '{analysis_id}' as analysis_id,
    'bbox_overlap' as analysis_type,
    ROW_NUMBER() OVER (ORDER BY overlap_count DESC, total_overlap_area DESC) as hotspot_rank,
    overlap_count,
    ROUND(total_overlap_area::numeric, 6) as total_overlap_area,
    ARRAY_LENGTH(involved_subdatasets, 1) as subdataset_count,
    ARRAY_LENGTH(involved_scenes, 1) as scene_count,
    involved_subdatasets,
    involved_scenes,
    hotspot_geometry as geometry,
    
    -- 分析参数（JSON格式）
    json_build_object(
        'min_overlap_area', {min_overlap_area},
        'top_n', {top_n},
        'analysis_time', NOW(),
        'grid_precision', 0.001,
        'min_overlap_pairs', 2,
        'avg_overlap_area', ROUND(avg_overlap_area::numeric, 6),
        'max_overlap_area', ROUND(max_overlap_area::numeric, 6),
        'avg_max_overlap_ratio', ROUND(avg_max_overlap_ratio::numeric, 4),
        'involved_cities', involved_cities,
        'total_data_names', ARRAY_LENGTH(involved_data_names, 1)
    )::text as analysis_params
    
FROM final_hotspots
ORDER BY overlap_count DESC, total_overlap_area DESC
LIMIT {top_n};

-- 显示插入结果的摘要
SELECT 
    '{analysis_id}' as analysis_id,
    COUNT(*) as hotspots_found,
    MAX(overlap_count) as max_overlap_count,
    MIN(overlap_count) as min_overlap_count,
    ROUND(AVG(overlap_count)::numeric, 2) as avg_overlap_count,
    ROUND(SUM(total_overlap_area)::numeric, 4) as total_overlap_area_sum
FROM {analysis_table}
WHERE analysis_id = '{analysis_id}';

-- 显示TOP 5热点的基本信息
SELECT 
    hotspot_rank,
    overlap_count,
    ROUND(total_overlap_area::numeric, 4) as total_overlap_area,
    subdataset_count,
    scene_count,
    involved_subdatasets[1:3] as sample_subdatasets  -- 显示前3个子数据集
FROM {analysis_table}
WHERE analysis_id = '{analysis_id}'
ORDER BY hotspot_rank
LIMIT 5;
