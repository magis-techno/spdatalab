-- ========================================
-- BBox叠置分析性能索引优化脚本
-- ========================================
-- 
-- 功能：为bbox分表创建专门的性能优化索引
-- 目标：大幅提升overlap分析的查询性能
-- 
-- 使用方法：
--   1. 在PostgreSQL中直接执行此脚本
--   2. 或通过psql命令行：psql -d postgres -f create_performance_indexes.sql
-- 
-- 注意事项：
--   - 索引创建需要时间，分表越多耗时越长
--   - 建议在非高峰时段执行
--   - 索引会占用额外存储空间，但能显著提升查询性能
-- 
-- 优化策略：
--   1. 复合索引：(city_id, all_good) - 快速过滤高质量城市数据
--   2. 空间索引：geometry - PostGIS空间查询必需
--   3. 主键索引：id - 避免重复配对时的查找
--   4. 部分索引：只为质量数据创建索引，节省空间
-- ========================================

\echo ''
\echo '🚀 开始创建BBox叠置分析性能优化索引...'
\echo ''

-- 创建索引统计函数
CREATE OR REPLACE FUNCTION create_bbox_performance_indexes() 
RETURNS TABLE(
    table_name TEXT,
    status TEXT,
    index_count INTEGER,
    processing_time INTERVAL
) 
LANGUAGE plpgsql AS $$
DECLARE
    bbox_table RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    created_indexes INTEGER;
    table_count INTEGER := 0;
    total_tables INTEGER;
BEGIN
    -- 获取分表总数
    SELECT COUNT(*) INTO total_tables
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE 'clips_bbox_%'
    AND table_name != 'clips_bbox';
    
    RAISE NOTICE '📋 发现 % 个bbox分表需要优化', total_tables;
    RAISE NOTICE '';
    
    -- 遍历所有bbox分表
    FOR bbox_table IN 
        SELECT t.table_name as tname
        FROM information_schema.tables t
        WHERE t.table_schema = 'public' 
        AND t.table_name LIKE 'clips_bbox_%'
        AND t.table_name != 'clips_bbox'
        ORDER BY t.table_name
    LOOP
        table_count := table_count + 1;
        start_time := clock_timestamp();
        created_indexes := 0;
        
        RAISE NOTICE '[%/%] 🔧 优化表: %', table_count, total_tables, bbox_table.tname;
        
        BEGIN
            -- 🚀 索引1：复合索引 (city_id, all_good) - 最重要的过滤索引
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_city_good 
                           ON %s (city_id, all_good) 
                           WHERE city_id IS NOT NULL', 
                          bbox_table.tname, bbox_table.tname);
            created_indexes := created_indexes + 1;
            
            -- 🚀 索引2：空间索引 (geometry) - 确保存在且优化
            -- 先检查是否已存在空间索引
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE tablename = bbox_table.tname 
                AND indexdef ILIKE '%gist%geometry%'
            ) THEN
                EXECUTE format('CREATE INDEX idx_%s_geom 
                               ON %s USING GIST (geometry)', 
                              bbox_table.tname, bbox_table.tname);
                created_indexes := created_indexes + 1;
            END IF;
            
            -- 🚀 索引3：主键优化索引 (id) - 通常已存在，但确保高效
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE tablename = bbox_table.tname 
                AND indexdef ILIKE '%\bid\b%'
                AND indexdef NOT ILIKE '%city_id%'
            ) THEN
                EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_id 
                               ON %s (id)', 
                              bbox_table.tname, bbox_table.tname);
                created_indexes := created_indexes + 1;
            END IF;
            
            -- 🚀 索引4：部分索引 - 只为高质量数据创建，节省空间
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_quality_geom 
                           ON %s USING GIST (geometry) 
                           WHERE all_good = true AND city_id IS NOT NULL', 
                          bbox_table.tname, bbox_table.tname);
            created_indexes := created_indexes + 1;
            
            -- 🚀 索引5：子数据集索引 - 用于子数据集过滤
            IF EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = bbox_table.tname 
                AND column_name = 'subdataset_name'
            ) THEN
                EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_subdataset 
                               ON %s (subdataset_name) 
                               WHERE subdataset_name IS NOT NULL', 
                              bbox_table.tname, bbox_table.tname);
                created_indexes := created_indexes + 1;
            END IF;
            
            end_time := clock_timestamp();
            
            -- 返回结果
            table_name := bbox_table.tname;
            status := '✅ 成功';
            index_count := created_indexes;
            processing_time := end_time - start_time;
            
            RETURN NEXT;
            
        EXCEPTION WHEN OTHERS THEN
            end_time := clock_timestamp();
            
            -- 返回错误结果
            table_name := bbox_table.tname;
            status := '❌ 失败: ' || SQLERRM;
            index_count := created_indexes;
            processing_time := end_time - start_time;
            
            RETURN NEXT;
            
            RAISE NOTICE '⚠️ 表 % 索引创建失败: %', bbox_table.tname, SQLERRM;
        END;
        
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE '✅ 索引优化完成！共处理 % 个分表', total_tables;
    
END $$;

-- 执行索引创建
\echo '开始执行索引创建...'
\echo ''

SELECT 
    table_name,
    status,
    index_count,
    EXTRACT(EPOCH FROM processing_time)::INTEGER || 's' as time_taken
FROM create_bbox_performance_indexes()
ORDER BY table_name;

-- 创建索引使用情况统计视图
\echo ''
\echo '📊 创建索引使用情况统计视图...'

CREATE OR REPLACE VIEW bbox_index_stats AS
WITH bbox_tables AS (
    SELECT table_name
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE 'clips_bbox_%'
    AND table_name != 'clips_bbox'
),
index_info AS (
    SELECT 
        t.table_name,
        COUNT(i.indexname) as total_indexes,
        COUNT(i.indexname) FILTER (WHERE i.indexdef ILIKE '%gist%') as spatial_indexes,
        COUNT(i.indexname) FILTER (WHERE i.indexdef ILIKE '%city_id%') as city_indexes,
        COUNT(i.indexname) FILTER (WHERE i.indexdef ILIKE '%all_good%') as quality_indexes,
        BOOL_OR(i.indexdef ILIKE '%city_id%all_good%' OR i.indexdef ILIKE '%all_good%city_id%') as has_composite_index
    FROM bbox_tables t
    LEFT JOIN pg_indexes i ON t.table_name = i.tablename
    WHERE i.schemaname = 'public' OR i.schemaname IS NULL
    GROUP BY t.table_name
)
SELECT 
    table_name,
    total_indexes,
    spatial_indexes,
    city_indexes,
    quality_indexes,
    has_composite_index,
    CASE 
        WHEN has_composite_index AND spatial_indexes > 0 THEN '🚀 优化完成'
        WHEN spatial_indexes > 0 AND city_indexes > 0 THEN '⚡ 部分优化'
        WHEN spatial_indexes > 0 THEN '📍 仅空间索引'
        ELSE '❌ 缺少索引'
    END as optimization_status
FROM index_info
ORDER BY 
    CASE 
        WHEN has_composite_index AND spatial_indexes > 0 THEN 1
        WHEN spatial_indexes > 0 AND city_indexes > 0 THEN 2
        WHEN spatial_indexes > 0 THEN 3
        ELSE 4
    END,
    table_name;

\echo '✅ 索引统计视图创建完成'
\echo ''

-- 显示优化结果统计
\echo '📊 索引优化结果统计:'
\echo ''

SELECT 
    optimization_status,
    COUNT(*) as table_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percentage
FROM bbox_index_stats
GROUP BY optimization_status
ORDER BY 
    CASE optimization_status
        WHEN '🚀 优化完成' THEN 1
        WHEN '⚡ 部分优化' THEN 2  
        WHEN '📍 仅空间索引' THEN 3
        ELSE 4
    END;

\echo ''
\echo '💡 查看详细索引状态：SELECT * FROM bbox_index_stats ORDER BY optimization_status;'
\echo ''

-- 创建索引维护建议函数
CREATE OR REPLACE FUNCTION get_bbox_index_recommendations()
RETURNS TABLE(
    recommendation_type TEXT,
    table_name TEXT,
    suggested_action TEXT,
    priority TEXT
) 
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        '🚨 缺少关键索引'::TEXT as rec_type,
        b.table_name,
        '需要创建复合索引: CREATE INDEX idx_' || b.table_name || '_city_good ON ' || b.table_name || ' (city_id, all_good);'::TEXT as action,
        '高'::TEXT as priority
    FROM bbox_index_stats b
    WHERE NOT b.has_composite_index
    
    UNION ALL
    
    SELECT 
        '📍 缺少空间索引'::TEXT,
        b.table_name,
        '需要创建空间索引: CREATE INDEX idx_' || b.table_name || '_geom ON ' || b.table_name || ' USING GIST (geometry);'::TEXT,
        '高'::TEXT
    FROM bbox_index_stats b
    WHERE b.spatial_indexes = 0
    
    UNION ALL
    
    SELECT 
        '⚡ 建议优化'::TEXT,
        b.table_name,
        '建议创建部分索引: CREATE INDEX idx_' || b.table_name || '_quality_geom ON ' || b.table_name || ' USING GIST (geometry) WHERE all_good = true;'::TEXT,
        '中'::TEXT
    FROM bbox_index_stats b
    WHERE b.has_composite_index AND b.spatial_indexes > 0 AND b.optimization_status != '🚀 优化完成'
    
    ORDER BY 
        CASE priority 
            WHEN '高' THEN 1 
            WHEN '中' THEN 2 
            ELSE 3 
        END,
        rec_type;
END $$;

\echo '🔧 使用以下命令获取索引优化建议:'
\echo 'SELECT * FROM get_bbox_index_recommendations();'
\echo ''

-- 清理临时函数（可选）
-- DROP FUNCTION create_bbox_performance_indexes();

\echo '✅ BBox叠置分析性能索引优化完成！'
\echo ''
\echo '📋 接下来可以测试性能改进:'
\echo '   python examples/dataset/bbox_examples/run_overlap_analysis.py --city A86 --intersect-only --top-n 5'
\echo ''
