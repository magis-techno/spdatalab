-- ========================================
-- 快速索引优化脚本 (简化版)
-- ========================================
-- 
-- 功能：快速为前10个bbox分表创建关键性能索引
-- 适用：急需提升性能，但不想等待全量索引创建
-- 
-- 使用方法：
--   psql -d postgres -f quick_index_optimization.sql
-- 
-- 预计耗时：1-5分钟（取决于数据量）
-- ========================================

\echo '🚀 快速索引优化开始...'
\echo ''

-- 获取前10个数据量较大的分表
DO $$
DECLARE
    table_record RECORD;
    table_count INTEGER := 0;
    start_time TIMESTAMP;
    total_time INTERVAL := '0 seconds';
BEGIN
    RAISE NOTICE '📋 开始为前10个分表创建关键索引...';
    RAISE NOTICE '';
    
    -- 遍历前10个分表（按表名排序）
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'clips_bbox_%'
        AND table_name != 'clips_bbox'
        ORDER BY table_name
        LIMIT 10
    LOOP
        table_count := table_count + 1;
        start_time := clock_timestamp();
        
        RAISE NOTICE '[%/10] 🔧 优化表: %', table_count, table_record.table_name;
        
        BEGIN
            -- 🚀 关键索引1：城市+质量复合索引
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_city_good 
                           ON %s (city_id, all_good) 
                           WHERE city_id IS NOT NULL', 
                          table_record.table_name, table_record.table_name);
            
            -- 🚀 关键索引2：确保空间索引存在
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE tablename = table_record.table_name 
                AND indexdef ILIKE '%gist%geometry%'
            ) THEN
                EXECUTE format('CREATE INDEX idx_%s_geom 
                               ON %s USING GIST (geometry)', 
                              table_record.table_name, table_record.table_name);
            END IF;
            
            total_time := total_time + (clock_timestamp() - start_time);
            RAISE NOTICE '   ✅ 完成 (耗时: %)', (clock_timestamp() - start_time);
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE '   ❌ 失败: %', SQLERRM;
        END;
        
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE '✅ 快速索引优化完成！';
    RAISE NOTICE '⏱️ 总耗时: %', total_time;
    RAISE NOTICE '';
    RAISE NOTICE '💡 如需完整优化所有分表，请运行: create_performance_indexes.sql';
    
END $$;

-- 验证索引创建结果
\echo '📊 验证前10个表的索引状态:'
\echo ''

SELECT 
    t.table_name,
    COUNT(i.indexname) as total_indexes,
    BOOL_OR(i.indexdef ILIKE '%gist%') as has_spatial_index,
    BOOL_OR(i.indexdef ILIKE '%city_id%') as has_city_index,
    CASE 
        WHEN BOOL_OR(i.indexdef ILIKE '%gist%') AND BOOL_OR(i.indexdef ILIKE '%city_id%') THEN '🚀 已优化'
        WHEN BOOL_OR(i.indexdef ILIKE '%gist%') THEN '📍 仅空间索引'
        ELSE '❌ 需要优化'
    END as status
FROM (
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE 'clips_bbox_%'
    AND table_name != 'clips_bbox'
    ORDER BY table_name
    LIMIT 10
) t
LEFT JOIN pg_indexes i ON t.table_name = i.tablename AND i.schemaname = 'public'
GROUP BY t.table_name
ORDER BY t.table_name;

\echo ''
\echo '✅ 快速索引优化完成！现在可以测试性能改进：'
\echo '   python examples/dataset/bbox_examples/run_overlap_analysis.py --city A86 --intersect-only --top-n 5'
\echo ''
