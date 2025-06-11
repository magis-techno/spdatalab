-- ==============================================================================
-- FDW设置脚本（使用psql变量）
-- ==============================================================================
-- 
-- 使用方法：
-- psql -d postgres -v fdw_user=username -v fdw_pwd=password \
--      -v traj_host=traj.example.com -v map_host=map.example.com \
--      -f sql/setup_fdw_with_params.sql
-- 
-- 或者在psql中设置变量后执行：
-- \set fdw_user 'your_username'
-- \set fdw_pwd 'your_password'  
-- \set traj_host 'your_traj_host'
-- \set map_host 'your_map_host'
-- \i sql/setup_fdw_with_params.sql
-- ==============================================================================

-- 创建postgres_fdw扩展
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 轨迹数据库FDW设置
DROP SERVER IF EXISTS traj_srv CASCADE;
CREATE SERVER traj_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host :'traj_host', dbname 'trajdb', port '5432');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER traj_srv
  OPTIONS (user :'fdw_user', password :'fdw_pwd');

IMPORT FOREIGN SCHEMA public
       LIMIT TO (ddi_data_points)
       FROM SERVER traj_srv INTO public;

-- 地图数据库FDW设置
DROP SERVER IF EXISTS map_srv CASCADE;
CREATE SERVER map_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host :'map_host', dbname 'mapdb', port '5432');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER map_srv
  OPTIONS (user :'fdw_user', password :'fdw_pwd');

IMPORT FOREIGN SCHEMA public
       LIMIT TO (intersections)
       FROM SERVER map_srv INTO public;

-- 验证表创建
\dt public.ddi_data_points
\dt public.intersections

SELECT 'FDW设置完成' as status; 