-- ==============================================================================
-- Foreign Data Wrapper (FDW) Setup Script
-- ==============================================================================
-- 
-- 此脚本设置PostgreSQL FDW连接到远程数据库，用于访问轨迹和交叉口数据
-- 
-- 使用方法：
-- 1. 确保远程数据库可访问
-- 2. 设置环境变量或直接替换连接参数
-- 3. 执行: psql -d postgres -f sql/setup_fdw.sql
-- 
-- 注意：需要在执行前设置以下变量：
-- - REMOTE_TRAJ_HOST: 轨迹数据库主机地址
-- - REMOTE_MAP_HOST: 地图数据库主机地址
-- - FDW_USER: 远程数据库用户名
-- - FDW_PASSWORD: 远程数据库密码
-- ==============================================================================

-- 创建postgres_fdw扩展（如果不存在）
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- ==============================================================================
-- 轨迹数据库FDW设置 (ddi_data_points)
-- ==============================================================================

-- 创建轨迹数据库服务器连接
DROP SERVER IF EXISTS traj_srv CASCADE;
CREATE SERVER traj_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'REMOTE_TRAJ_HOST',           -- 替换为实际的轨迹数据库主机
    dbname 'trajdb', 
    port '5432',
    fetch_size '1000',                 -- 优化批量获取
    connect_timeout '10'               -- 连接超时设置
  );

-- 创建用户映射
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER traj_srv
  OPTIONS (
    user 'FDW_USER',                   -- 替换为实际用户名
    password 'FDW_PASSWORD'            -- 替换为实际密码
  );

-- 导入ddi_data_points表
IMPORT FOREIGN SCHEMA public
  LIMIT TO (ddi_data_points)
  FROM SERVER traj_srv INTO public;

-- ==============================================================================
-- 地图数据库FDW设置 (intersections)
-- ==============================================================================

-- 创建地图数据库服务器连接
DROP SERVER IF EXISTS map_srv CASCADE;
CREATE SERVER map_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'REMOTE_MAP_HOST',            -- 替换为实际的地图数据库主机
    dbname 'mapdb', 
    port '5432',
    fetch_size '1000',                 -- 优化批量获取
    connect_timeout '10'               -- 连接超时设置
  );

-- 创建用户映射
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER map_srv
  OPTIONS (
    user 'FDW_USER',                   -- 替换为实际用户名
    password 'FDW_PASSWORD'            -- 替换为实际密码
  );

-- 导入intersections表
IMPORT FOREIGN SCHEMA public
  LIMIT TO (intersections)
  FROM SERVER map_srv INTO public;

-- ==============================================================================
-- 验证FDW表创建
-- ==============================================================================

-- 检查ddi_data_points表是否创建成功
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'ddi_data_points'
    ) THEN
        RAISE NOTICE '✅ ddi_data_points 表创建成功';
    ELSE
        RAISE WARNING '❌ ddi_data_points 表创建失败';
    END IF;
END $$;

-- 检查intersections表是否创建成功
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'intersections'
    ) THEN
        RAISE NOTICE '✅ intersections 表创建成功';
    ELSE
        RAISE WARNING '❌ intersections 表创建失败';
    END IF;
END $$;

-- 显示外部表信息
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename IN ('ddi_data_points', 'intersections');

-- 显示FDW服务器信息
SELECT 
    srvname as server_name,
    srvoptions as server_options
FROM pg_foreign_server;

-- 最终提示信息
DO $$
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'FDW设置完成！';
    RAISE NOTICE '请验证远程表是否可以正常查询：';
    RAISE NOTICE 'SELECT COUNT(*) FROM ddi_data_points LIMIT 1;';
    RAISE NOTICE 'SELECT COUNT(*) FROM intersections LIMIT 1;';
    RAISE NOTICE '==========================================';
END $$; 