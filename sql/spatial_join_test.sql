-- =============================================================================
-- 空间连接性能测试
-- 
-- 目标：诊断clips_bbox与intersections的空间连接性能问题
-- =============================================================================

\echo '========================================='
\echo '开始空间连接性能测试'
\echo '========================================='

-- 设置输出格式
\pset pager off
\timing on

-- =============================================================================
-- 1. 基础信息检查
-- =============================================================================

\echo ''
\echo '1. 检查表记录数量'
\echo '-----------------------------------------'

SELECT 'clips_bbox表记录数:' as info, count(*) as count FROM clips_bbox;
SELECT 'intersections视图记录数:' as info, count(*) as count FROM intersections;

-- =============================================================================
-- 2. 空间索引检查
-- =============================================================================

\echo ''
\echo '2. 检查空间索引状态'
\echo '-----------------------------------------'

-- 检查clips_bbox的索引
\echo 'clips_bbox表的空间索引:'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'clips_bbox' AND indexdef LIKE '%gist%';

-- 检查intersections视图的索引（通常视图没有索引）
\echo 'intersections视图的索引:'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'intersections';

-- 检查基础表full_intersection的索引
\echo 'full_intersection基础表的索引:'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'full_intersection' AND indexdef LIKE '%gist%';

-- =============================================================================
-- 3. 最简单的空间相交测试
-- =============================================================================

\echo ''
\echo '3. 最简单的空间相交测试（前10个匹配）'
\echo '-----------------------------------------'

SELECT c.scene_token, c.id as bbox_id
FROM clips_bbox c
JOIN intersections i ON ST_Intersects(c.geometry, i.geometry)
LIMIT 10;

-- =============================================================================
-- 4. 100个bbox的相交测试
-- =============================================================================

\echo ''
\echo '4. 100个bbox与intersections相交测试'
\echo '-----------------------------------------'

SELECT c.scene_token, count(*) as intersection_count
FROM (SELECT scene_token, geometry FROM clips_bbox LIMIT 100) c
JOIN intersections i ON ST_Intersects(c.geometry, i.geometry)
GROUP BY c.scene_token
ORDER BY intersection_count DESC
LIMIT 10;

-- =============================================================================
-- 5. 查询执行计划分析
-- =============================================================================

\echo ''
\echo '5. 查询执行计划分析（100个bbox）'
\echo '-----------------------------------------'

EXPLAIN ANALYZE 
SELECT c.scene_token, count(*) 
FROM (SELECT scene_token, geometry FROM clips_bbox LIMIT 100) c
JOIN intersections i ON ST_Intersects(c.geometry, i.geometry)
GROUP BY c.scene_token;

-- =============================================================================
-- 6. 几何类型和范围检查
-- =============================================================================

\echo ''
\echo '6. 几何类型和空间范围检查'
\echo '-----------------------------------------'

-- 检查clips_bbox的几何类型和范围
SELECT 
    'clips_bbox' as table_name,
    ST_GeometryType(geometry) as geom_type,
    count(*) as count
FROM clips_bbox 
GROUP BY ST_GeometryType(geometry);

-- 检查intersections的几何类型和范围
SELECT 
    'intersections' as table_name,
    ST_GeometryType(geometry) as geom_type,
    count(*) as count
FROM intersections 
GROUP BY ST_GeometryType(geometry)
LIMIT 5;

-- 检查空间范围重叠
\echo 'clips_bbox空间范围:'
SELECT ST_AsText(ST_Extent(geometry)) as bbox_extent FROM clips_bbox;

\echo 'intersections空间范围:'
SELECT ST_AsText(ST_Extent(geometry)) as intersections_extent FROM intersections;

-- =============================================================================
-- 7. 单个bbox测试
-- =============================================================================

\echo ''
\echo '7. 单个bbox的相交测试'
\echo '-----------------------------------------'

-- 选择一个bbox进行详细测试
WITH test_bbox AS (
    SELECT scene_token, geometry FROM clips_bbox LIMIT 1
)
SELECT 
    tb.scene_token,
    count(*) as intersection_count,
    ST_AsText(ST_Centroid(tb.geometry)) as bbox_center
FROM test_bbox tb
JOIN intersections i ON ST_Intersects(tb.geometry, i.geometry)
GROUP BY tb.scene_token, tb.geometry;

-- =============================================================================
-- 8. 性能优化建议检查
-- =============================================================================

\echo ''
\echo '8. 检查PostgreSQL配置'
\echo '-----------------------------------------'

-- 检查work_mem设置（影响排序和连接性能）
SELECT name, setting, unit, short_desc 
FROM pg_settings 
WHERE name IN ('work_mem', 'shared_buffers', 'effective_cache_size');

-- 检查表统计信息更新时间
SELECT 
    schemaname, 
    tablename, 
    last_analyze, 
    last_autoanalyze,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM pg_stat_user_tables 
WHERE tablename IN ('clips_bbox', 'full_intersection');

-- =============================================================================
-- 9. 测试完成
-- =============================================================================

\echo ''
\echo '========================================='
\echo '空间连接性能测试完成'
\echo '========================================='
\echo ''
\echo '结果分析提示:'
\echo '1. 如果简单测试很快，问题可能在于数据量或索引'
\echo '2. 如果执行计划显示Seq Scan，说明没有使用空间索引'
\echo '3. 如果intersections视图没有索引，考虑在基础表上创建索引'
\echo '4. 检查空间范围是否重叠，如果不重叠则不会有结果'
\echo '' 