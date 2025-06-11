-- FDW 清理脚本
-- 用途：清理所有Foreign Data Wrapper相关的连接和表
-- 使用方法：make clean-fdw 或 直接运行此脚本

-- ===============================================
-- 清理外部表
-- ===============================================

-- 清理trajectory相关的外部表
DROP FOREIGN TABLE IF EXISTS public.ddi_data_points CASCADE;

-- 清理map相关的外部表  
DROP FOREIGN TABLE IF EXISTS public.intersections CASCADE;

-- ===============================================
-- 清理用户映射
-- ===============================================

-- 清理trajectory服务器的用户映射
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_user_mappings 
        WHERE srvname = 'traj_srv' AND usename = 'postgres'
    ) THEN
        DROP USER MAPPING FOR postgres SERVER traj_srv;
        RAISE NOTICE '✅ 已清理trajectory服务器用户映射';
    ELSE
        RAISE NOTICE '⚠️  trajectory服务器用户映射不存在';
    END IF;
END $$;

-- 清理map服务器的用户映射
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_user_mappings 
        WHERE srvname = 'map_srv' AND usename = 'postgres'
    ) THEN
        DROP USER MAPPING FOR postgres SERVER map_srv;
        RAISE NOTICE '✅ 已清理map服务器用户映射';
    ELSE
        RAISE NOTICE '⚠️  map服务器用户映射不存在';
    END IF;
END $$;

-- ===============================================
-- 清理服务器连接
-- ===============================================

-- 清理trajectory服务器连接
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_foreign_server 
        WHERE srvname = 'traj_srv'
    ) THEN
        DROP SERVER traj_srv CASCADE;
        RAISE NOTICE '✅ 已清理trajectory服务器连接';
    ELSE
        RAISE NOTICE '⚠️  trajectory服务器连接不存在';
    END IF;
END $$;

-- 清理map服务器连接
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_foreign_server 
        WHERE srvname = 'map_srv'
    ) THEN
        DROP SERVER map_srv CASCADE;
        RAISE NOTICE '✅ 已清理map服务器连接';
    ELSE
        RAISE NOTICE '⚠️  map服务器连接不存在';
    END IF;
END $$;

-- ===============================================
-- 验证清理结果
-- ===============================================

-- 检查剩余的外部服务器
DO $$
DECLARE
    remaining_servers INTEGER;
BEGIN
    SELECT COUNT(*) INTO remaining_servers
    FROM pg_foreign_server 
    WHERE srvname IN ('traj_srv', 'map_srv');
    
    IF remaining_servers = 0 THEN
        RAISE NOTICE '✅ 所有FDW服务器已清理完成';
    ELSE
        RAISE WARNING '⚠️  仍有 % 个FDW服务器未清理', remaining_servers;
    END IF;
END $$;

-- 检查剩余的外部表
DO $$
DECLARE
    remaining_tables INTEGER;
BEGIN
    SELECT COUNT(*) INTO remaining_tables
    FROM pg_foreign_tables 
    WHERE tablename IN ('ddi_data_points', 'intersections');
    
    IF remaining_tables = 0 THEN
        RAISE NOTICE '✅ 所有FDW外部表已清理完成';
    ELSE
        RAISE WARNING '⚠️  仍有 % 个FDW外部表未清理', remaining_tables;
    END IF;
END $$;

RAISE NOTICE '================================================';
RAISE NOTICE 'FDW 清理完成';
RAISE NOTICE '如需重新连接，请运行: make init-fdw';
RAISE NOTICE '================================================'; 