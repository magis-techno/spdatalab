-- 分表模式清理脚本
-- 用途：只清理分表和视图，保留主表

\echo '开始清理clips_bbox分表模式相关数据...'

-- ==============================================
-- 第一步：删除统一视图
-- ==============================================
\echo '删除统一视图...'
DROP VIEW IF EXISTS clips_bbox_all CASCADE;

-- ==============================================
-- 第二步：删除所有分表（clips_bbox_*），但保留主表
-- ==============================================
\echo '删除所有clips_bbox分表（保留主表）...'

-- 使用存储过程动态删除所有clips_bbox_*表（排除主表clips_bbox）
DO $$
DECLARE
    table_record RECORD;
    table_count INTEGER := 0;
BEGIN
    -- 查找所有clips_bbox_*表，但排除主表clips_bbox
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_name LIKE 'clips_bbox_%'
          AND table_name != 'clips_bbox'
          AND table_type = 'BASE TABLE'
    LOOP
        -- 删除每个分表
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_record.table_name) || ' CASCADE';
        table_count := table_count + 1;
        RAISE NOTICE '已删除分表: %', table_record.table_name;
    END LOOP;
    
    IF table_count = 0 THEN
        RAISE NOTICE '未找到需要删除的分表';
    ELSE
        RAISE NOTICE '总计删除了 % 个分表', table_count;
    END IF;
END $$;

-- ==============================================
-- 第三步：清理相关的序列（分表相关）
-- ==============================================
\echo '清理分表相关的序列...'

DO $$
DECLARE
    seq_record RECORD;
    seq_count INTEGER := 0;
BEGIN
    -- 查找分表相关的序列（排除主表序列）
    FOR seq_record IN 
        SELECT sequence_name 
        FROM information_schema.sequences 
        WHERE sequence_schema = 'public' 
          AND sequence_name LIKE 'clips_bbox_%'
          AND sequence_name != 'clips_bbox_id_seq'
    LOOP
        -- 删除序列
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(seq_record.sequence_name) || ' CASCADE';
        seq_count := seq_count + 1;
        RAISE NOTICE '已删除序列: %', seq_record.sequence_name;
    END LOOP;
    
    IF seq_count = 0 THEN
        RAISE NOTICE '未找到需要删除的分表相关序列';
    ELSE
        RAISE NOTICE '总计删除了 % 个序列', seq_count;
    END IF;
END $$;

-- ==============================================
-- 完成清理
-- ==============================================
\echo '======================================'
\echo '分表模式清理完成！'
\echo '- 已删除所有分表 (clips_bbox_*)'
\echo '- 已删除统一视图 (clips_bbox_all)'
\echo '- 已删除分表相关序列'
\echo '- 保留了主表 (clips_bbox)'
\echo '======================================' 