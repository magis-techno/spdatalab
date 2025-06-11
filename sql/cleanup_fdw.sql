-- ==============================================================================
-- FDW清理脚本
-- ==============================================================================
-- 
-- 此脚本用于清理所有FDW相关的设置，包括外部表、用户映射和服务器连接
-- 
-- 使用方法：
-- psql -d postgres -f sql/cleanup_fdw.sql
-- ==============================================================================

-- 删除外部表（如果存在）
DROP FOREIGN TABLE IF EXISTS public.ddi_data_points CASCADE;
DROP FOREIGN TABLE IF EXISTS public.intersections CASCADE;

-- 删除用户映射
DROP USER MAPPING IF EXISTS FOR postgres SERVER traj_srv;
DROP USER MAPPING IF EXISTS FOR postgres SERVER map_srv;

-- 删除服务器连接
DROP SERVER IF EXISTS traj_srv CASCADE;
DROP SERVER IF EXISTS map_srv CASCADE;

-- 验证清理结果
DO $$
BEGIN
    -- 检查外部表是否已删除
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'ddi_data_points'
    ) THEN
        RAISE NOTICE '✅ ddi_data_points 外部表已删除';
    ELSE
        RAISE WARNING '❌ ddi_data_points 外部表仍存在';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'intersections'
    ) THEN
        RAISE NOTICE '✅ intersections 外部表已删除';
    ELSE
        RAISE WARNING '❌ intersections 外部表仍存在';
    END IF;

    -- 检查FDW服务器是否已删除
    IF NOT EXISTS (
        SELECT 1 FROM pg_foreign_server 
        WHERE srvname IN ('traj_srv', 'map_srv')
    ) THEN
        RAISE NOTICE '✅ 所有FDW服务器已删除';
    ELSE
        RAISE WARNING '❌ 仍有FDW服务器存在';
    END IF;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'FDW清理完成！';
    RAISE NOTICE '==========================================';
END $$; 