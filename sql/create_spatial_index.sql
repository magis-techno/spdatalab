-- =============================================================================
-- 创建缺失的空间索引
-- 
-- 目标：为full_intersection表的wkb_geometry列创建空间索引
-- 这将显著提高intersections视图的空间查询性能
-- =============================================================================

\echo '创建full_intersection表的空间索引...'

-- 创建空间索引（可能需要几分钟时间，取决于13.5M记录的数据大小）
CREATE INDEX IF NOT EXISTS idx_full_intersection_wkb_geometry 
ON full_intersection 
USING gist (wkb_geometry);

\echo '空间索引创建完成！'

-- 更新表统计信息以优化查询规划
ANALYZE full_intersection;

\echo '表统计信息已更新'

-- 验证索引是否创建成功
\echo '验证索引创建结果:'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'full_intersection' AND indexdef LIKE '%gist%'; 