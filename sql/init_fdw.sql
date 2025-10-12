-- Foreign Data Wrapper 初始化脚本
-- 用途：连接远程数据库的trajectory和intersection数据
-- 使用方法：
--   1. 设置环境变量或手动替换占位符
--   2. 运行 make init-fdw

-- 启用postgres_fdw扩展
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- ===============================================
-- Trajectory Database FDW Connection
-- ===============================================

-- 创建trajectory数据库服务器连接
CREATE SERVER IF NOT EXISTS traj_srv FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'REMOTE_TRAJ_HOST',  -- 替换为实际的trajectory数据库主机
        dbname 'trajdb',          -- 替换为实际的trajectory数据库名称
        port '5432'               -- 替换为实际的端口
    );

-- 创建用户映射（用于连接trajectory数据库）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_user_mappings 
        WHERE srvname = 'traj_srv' AND usename = 'postgres'
    ) THEN
        CREATE USER MAPPING FOR postgres SERVER traj_srv
            OPTIONS (
                user :fdw_user,      -- 替换为远程数据库用户名
                password :fdw_pwd    -- 替换为远程数据库密码
            );
    END IF;
END $$;

-- 导入trajectory相关的外部表
-- 如果表已存在则先删除
DROP FOREIGN TABLE IF EXISTS public.ddi_data_points;

-- 导入ddi_data_points表
IMPORT FOREIGN SCHEMA public
    LIMIT TO (ddi_data_points)
    FROM SERVER traj_srv 
    INTO public;

-- ===============================================
-- Map/Intersection Database FDW Connection  
-- ===============================================

-- 创建map数据库服务器连接
CREATE SERVER IF NOT EXISTS map_srv FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'REMOTE_MAP_HOST',   -- 替换为实际的map数据库主机
        dbname 'mapdb',           -- 替换为实际的map数据库名称
        port '5432'               -- 替换为实际的端口
    );

-- 创建用户映射（用于连接map数据库）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_user_mappings 
        WHERE srvname = 'map_srv' AND usename = 'postgres'
    ) THEN
        CREATE USER MAPPING FOR postgres SERVER map_srv
            OPTIONS (
                user :fdw_user,      -- 替换为远程数据库用户名
                password :fdw_pwd    -- 替换为远程数据库密码
            );
    END IF;
END $$;

-- 导入map相关的外部表
-- 如果表已存在则先删除
DROP FOREIGN TABLE IF EXISTS public.intersections;

-- 导入intersections表
IMPORT FOREIGN SCHEMA public
    LIMIT TO (intersections)
    FROM SERVER map_srv 
    INTO public;

-- ===============================================
-- 验证FDW连接
-- ===============================================

-- 验证ddi_data_points表是否可访问
DO $$
BEGIN
    BEGIN
        PERFORM 1 FROM public.ddi_data_points LIMIT 1;
        RAISE NOTICE '✅ ddi_data_points 表连接成功';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '❌ ddi_data_points 表连接失败: %', SQLERRM;
    END;
END $$;

-- 验证intersections表是否可访问
DO $$
BEGIN
    BEGIN
        PERFORM 1 FROM public.intersections LIMIT 1;
        RAISE NOTICE '✅ intersections 表连接成功';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '❌ intersections 表连接失败: %', SQLERRM;
    END;
END $$;

-- 显示FDW状态信息
SELECT 
    srvname as server_name,
    srvowner::regrole as owner,
    srvoptions as options
FROM pg_foreign_server 
WHERE srvname IN ('traj_srv', 'map_srv');

-- 显示外部表信息
SELECT 
    schemaname,
    tablename,
    servername
FROM pg_foreign_tables 
WHERE tablename IN ('ddi_data_points', 'intersections');

RAISE NOTICE '================================================';
RAISE NOTICE 'FDW 初始化完成';
RAISE NOTICE '请检查上述验证结果，确保远程表连接正常';
RAISE NOTICE '================================================'; 