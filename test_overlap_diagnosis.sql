-- ========================================
-- 重叠分析问题诊断SQL
-- ========================================
-- 用于逐步诊断为什么重叠分析结果异常

-- 第一步：检查基础数据
SELECT 
    'Step 1: 基础数据检查' as step,
    COUNT(*) as total_count,
    COUNT(*) FILTER (WHERE city_id = 'A263') as city_count,
    COUNT(*) FILTER (WHERE city_id = 'A263' AND all_good = true) as city_good_count
FROM clips_bbox_unified_qgis
WHERE city_id IS NOT NULL;

-- 第二步：检查重叠对数量（不同阈值）
SELECT 
    'Step 2: 重叠对统计' as step,
    '阈值=0' as threshold_desc,
    COUNT(*) as overlap_pairs
FROM clips_bbox_unified_qgis a
JOIN clips_bbox_unified_qgis b ON a.qgis_id < b.qgis_id
WHERE ST_Intersects(a.geometry, b.geometry)
AND NOT ST_Equals(a.geometry, b.geometry)
AND a.city_id = b.city_id
AND a.city_id = 'A263'
AND a.all_good = true
AND b.all_good = true
AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0

UNION ALL

SELECT 
    'Step 2: 重叠对统计' as step,
    '阈值=1e-6' as threshold_desc,
    COUNT(*) as overlap_pairs
FROM clips_bbox_unified_qgis a
JOIN clips_bbox_unified_qgis b ON a.qgis_id < b.qgis_id
WHERE ST_Intersects(a.geometry, b.geometry)
AND NOT ST_Equals(a.geometry, b.geometry)
AND a.city_id = b.city_id
AND a.city_id = 'A263'
AND a.all_good = true
AND b.all_good = true
AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0.000001;

-- 第三步：测试现在的SQL逻辑，看实际产生的rank
WITH overlapping_pairs AS (
    SELECT 
        a.qgis_id as bbox_a_id,
        b.qgis_id as bbox_b_id,
        ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
    FROM clips_bbox_unified_qgis a
    JOIN clips_bbox_unified_qgis b ON a.qgis_id < b.qgis_id
    WHERE ST_Intersects(a.geometry, b.geometry)
    AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0
    AND NOT ST_Equals(a.geometry, b.geometry)
    AND a.city_id = b.city_id
    AND a.city_id = 'A263'
    AND a.all_good = true
    AND b.all_good = true
),
individual_overlaps AS (
    SELECT 
        1 as overlap_count,
        overlap_area as total_overlap_area,
        CONCAT(bbox_a_id, '_', bbox_b_id) as pair_id
    FROM overlapping_pairs
)
SELECT 
    'Step 3: 当前逻辑测试' as step,
    ROW_NUMBER() OVER (ORDER BY total_overlap_area DESC) as hotspot_rank,
    overlap_count,
    ROUND(total_overlap_area::numeric, 10) as total_overlap_area,
    pair_id
FROM individual_overlaps
ORDER BY total_overlap_area DESC
LIMIT 10;

-- 第四步：检查最近的分析结果
SELECT 
    'Step 4: 最近分析结果' as step,
    analysis_id,
    hotspot_rank,
    overlap_count,
    total_overlap_area,
    subdataset_count,
    scene_count,
    created_at
FROM bbox_overlap_analysis_results
WHERE analysis_id LIKE '%A263%' OR analysis_id LIKE '%docker%'
ORDER BY created_at DESC, hotspot_rank ASC
LIMIT 10;
