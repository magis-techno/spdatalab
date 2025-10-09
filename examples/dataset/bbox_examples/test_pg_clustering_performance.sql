-- ========================================
-- PostgreSQL 空间聚类性能测试脚本
-- ========================================
-- 
-- 目的：测试PostGIS聚类功能的性能和结果质量
-- 使用方法：psql -d postgres -f test_pg_clustering_performance.sql
-- 
-- ========================================

-- 设置计时显示
\timing on

-- 显示测试开始信息
SELECT 
    '🚀 开始PG聚类性能测试' as status,
    NOW() as test_start_time;

-- ----------------------------------------
-- 测试1: 基础数据统计
-- ----------------------------------------
SELECT '📊 测试1: 基础数据统计' as test_name;

SELECT 
    city_id,
    COUNT(*) as total_bbox,
    COUNT(*) FILTER (WHERE all_good = true) as good_bbox,
    ROUND(AVG(ST_Area(geometry))::numeric, 8) as avg_area,
    ST_Extent(geometry) as city_bounds
FROM clips_bbox_unified 
WHERE city_id IN ('A263', 'A72', 'A86')  -- 测试几个主要城市
GROUP BY city_id
ORDER BY total_bbox DESC;

-- ----------------------------------------
-- 测试2: ST_ClusterDBSCAN 性能测试
-- ----------------------------------------
SELECT '⚡ 测试2: DBSCAN聚类性能 (eps=0.002, minpts=5)' as test_name;

EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH dbscan_clusters AS (
    SELECT 
        id,
        geometry,
        ST_ClusterDBSCAN(geometry, 0.002, 5) OVER() as cluster_id
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
),
cluster_stats AS (
    SELECT 
        cluster_id,
        COUNT(*) as cluster_size,
        ST_Area(ST_ConvexHull(ST_Collect(geometry))) as cluster_area,
        ST_Centroid(ST_Collect(geometry)) as cluster_center
    FROM dbscan_clusters 
    WHERE cluster_id IS NOT NULL  -- 排除噪声点
    GROUP BY cluster_id
)
SELECT 
    '聚类统计' as result_type,
    COUNT(*) as total_clusters,
    MAX(cluster_size) as max_cluster_size,
    MIN(cluster_size) as min_cluster_size,
    ROUND(AVG(cluster_size)::numeric, 2) as avg_cluster_size,
    COUNT(*) FILTER (WHERE cluster_size >= 10) as significant_clusters
FROM cluster_stats;

-- ----------------------------------------
-- 测试3: 不同参数的DBSCAN对比
-- ----------------------------------------
SELECT '🔍 测试3: 不同DBSCAN参数对比' as test_name;

-- 小范围高密度
WITH small_eps AS (
    SELECT 
        'eps=0.001_minpts=3' as params,
        COUNT(DISTINCT ST_ClusterDBSCAN(geometry, 0.001, 3) OVER()) as cluster_count,
        COUNT(*) FILTER (WHERE ST_ClusterDBSCAN(geometry, 0.001, 3) OVER() IS NULL) as noise_points
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
),
-- 中等范围中密度  
medium_eps AS (
    SELECT 
        'eps=0.002_minpts=5' as params,
        COUNT(DISTINCT ST_ClusterDBSCAN(geometry, 0.002, 5) OVER()) as cluster_count,
        COUNT(*) FILTER (WHERE ST_ClusterDBSCAN(geometry, 0.002, 5) OVER() IS NULL) as noise_points
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
),
-- 大范围低密度
large_eps AS (
    SELECT 
        'eps=0.005_minpts=10' as params,
        COUNT(DISTINCT ST_ClusterDBSCAN(geometry, 0.005, 10) OVER()) as cluster_count,
        COUNT(*) FILTER (WHERE ST_ClusterDBSCAN(geometry, 0.005, 10) OVER() IS NULL) as noise_points
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
)
SELECT * FROM small_eps
UNION ALL SELECT * FROM medium_eps  
UNION ALL SELECT * FROM large_eps
ORDER BY params;

-- ----------------------------------------
-- 测试4: ST_ClusterKMeans 性能测试
-- ----------------------------------------
SELECT '🎯 测试4: KMeans聚类性能 (k=20)' as test_name;

EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH kmeans_clusters AS (
    SELECT 
        id,
        geometry,
        ST_ClusterKMeans(geometry, 20) OVER() as cluster_id
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
),
kmeans_stats AS (
    SELECT 
        cluster_id,
        COUNT(*) as cluster_size,
        ST_Area(ST_ConvexHull(ST_Collect(geometry))) as cluster_area,
        ST_Centroid(ST_Collect(geometry)) as cluster_center
    FROM kmeans_clusters
    GROUP BY cluster_id
)
SELECT 
    '聚类统计' as result_type,
    COUNT(*) as total_clusters,
    MAX(cluster_size) as max_cluster_size,
    MIN(cluster_size) as min_cluster_size,
    ROUND(AVG(cluster_size)::numeric, 2) as avg_cluster_size,
    ROUND(STDDEV(cluster_size)::numeric, 2) as cluster_size_stddev
FROM kmeans_stats;

-- ----------------------------------------
-- 测试5: 分层聚类模拟
-- ----------------------------------------
SELECT '🏗️ 测试5: 分层聚类性能测试' as test_name;

EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH 
-- 第1层：粗聚类识别热点区域
coarse_clusters AS (
    SELECT 
        id,
        geometry,
        ST_ClusterDBSCAN(geometry, 0.01, 20) OVER() as coarse_id
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
),
-- 第2层：在大聚类内部细分
fine_clusters AS (
    SELECT 
        id,
        geometry,
        coarse_id,
        ST_ClusterDBSCAN(geometry, 0.002, 5) OVER(PARTITION BY coarse_id) as fine_id
    FROM coarse_clusters 
    WHERE coarse_id IS NOT NULL  -- 只处理有效聚类
),
-- 统计分层结果
hierarchical_stats AS (
    SELECT 
        coarse_id,
        fine_id,
        COUNT(*) as final_cluster_size,
        ST_Centroid(ST_Collect(geometry)) as cluster_center
    FROM fine_clusters
    WHERE fine_id IS NOT NULL
    GROUP BY coarse_id, fine_id
)
SELECT 
    '分层聚类统计' as result_type,
    COUNT(DISTINCT coarse_id) as coarse_clusters,
    COUNT(*) as fine_clusters,
    MAX(final_cluster_size) as max_fine_cluster,
    ROUND(AVG(final_cluster_size)::numeric, 2) as avg_fine_cluster
FROM hierarchical_stats;

-- ----------------------------------------
-- 测试6: 内存和性能监控
-- ----------------------------------------
SELECT '📈 测试6: 系统资源使用情况' as test_name;

-- 查看当前连接的内存使用
SELECT 
    'Memory Usage' as metric,
    pg_size_pretty(pg_total_relation_size('clips_bbox_unified')) as table_size,
    (SELECT setting FROM pg_settings WHERE name = 'shared_buffers') as shared_buffers,
    (SELECT setting FROM pg_settings WHERE name = 'work_mem') as work_mem;

-- 查看最近的查询性能
SELECT 
    'Recent Query Performance' as metric,
    query,
    calls,
    ROUND(total_time::numeric, 2) as total_time_ms,
    ROUND(mean_time::numeric, 2) as mean_time_ms
FROM pg_stat_statements 
WHERE query LIKE '%ST_Cluster%'
ORDER BY total_time DESC
LIMIT 5;

-- ----------------------------------------
-- 测试7: 与网格方法的对比基准
-- ----------------------------------------
SELECT '⚖️ 测试7: 网格方法对比基准' as test_name;

-- 模拟网格密度计算的性能
EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH grid_density AS (
    SELECT 
        floor(ST_X(ST_Centroid(geometry)) / 0.002)::int as grid_x,
        floor(ST_Y(ST_Centroid(geometry)) / 0.002)::int as grid_y,
        COUNT(*) as bbox_count
    FROM clips_bbox_unified 
    WHERE city_id = 'A263' AND all_good = true
    GROUP BY grid_x, grid_y
    HAVING COUNT(*) >= 5
)
SELECT 
    '网格密度统计' as result_type,
    COUNT(*) as total_grids,
    MAX(bbox_count) as max_density,
    ROUND(AVG(bbox_count)::numeric, 2) as avg_density
FROM grid_density;

-- ----------------------------------------
-- 测试总结
-- ----------------------------------------
SELECT 
    '✅ PG聚类性能测试完成' as status,
    NOW() as test_end_time;

-- 性能建议输出
SELECT 
    '💡 性能建议' as category,
    CASE 
        WHEN (SELECT COUNT(*) FROM clips_bbox_unified WHERE city_id = 'A263' AND all_good = true) < 10000 
        THEN '数据量较小，建议直接使用网格方法'
        WHEN (SELECT COUNT(*) FROM clips_bbox_unified WHERE city_id = 'A263' AND all_good = true) < 50000
        THEN '数据量适中，可以考虑DBSCAN聚类 + 网格精化'
        ELSE '数据量较大，强烈建议分层聚类方法'
    END as recommendation;

\timing off
