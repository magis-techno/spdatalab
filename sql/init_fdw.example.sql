-- FDW 配置示例文件
-- 用途：展示如何配置实际的FDW连接参数
-- 使用方法：
--   1. 复制此文件为 init_fdw.sql
--   2. 修改下面的实际参数
--   3. 运行 make init-fdw

-- 启用postgres_fdw扩展
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- ===============================================
-- Trajectory Database FDW Connection
-- ===============================================

-- 创建trajectory数据库服务器连接
CREATE SERVER IF NOT EXISTS traj_srv FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host '192.168.1.100',     -- 实际的trajectory数据库主机IP
        dbname 'trajectory_db',   -- 实际的trajectory数据库名称
        port '5432'               -- 实际的端口
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
                user 'trajectory_user',      -- 实际的远程数据库用户名
                password 'trajectory_pass'   -- 实际的远程数据库密码
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
        host '192.168.1.101',     -- 实际的map数据库主机IP
        dbname 'map_database',    -- 实际的map数据库名称
        port '5432'               -- 实际的端口
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
                user 'map_user',          -- 实际的远程数据库用户名
                password 'map_password'   -- 实际的远程数据库密码
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

-- ===============================================
-- 配置说明和注意事项
-- ===============================================

/*
配置说明：

1. 替换连接参数：
   - host: 远程数据库的IP地址或主机名
   - dbname: 远程数据库名称
   - port: 远程数据库端口（通常是5432）
   - user: 远程数据库用户名
   - password: 远程数据库密码

2. 网络要求：
   - 确保本地数据库能够访问远程数据库主机
   - 检查防火墙设置，开放相应端口
   - 确保远程数据库允许外部连接

3. 权限要求：
   - 远程数据库用户需要对目标表有SELECT权限
   - 如果需要写入，还需要INSERT/UPDATE/DELETE权限

4. 测试连接：
   - 运行 make check-fdw 检查连接状态
   - 在psql中执行 SELECT COUNT(*) FROM ddi_data_points; 测试访问

5. 故障排除：
   - 连接失败：检查网络连通性和认证信息
   - 表不存在：确认远程数据库中表名正确
   - 权限错误：检查用户权限设置
*/ 