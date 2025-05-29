-- Foreign‑table mapping for trajectory & intersection layers
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- trajectory DB
CREATE SERVER traj_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'REMOTE_TRAJ_HOST', dbname 'trajdb', port '5432');
CREATE USER MAPPING FOR postgres SERVER traj_srv
  OPTIONS (user :fdw_user, password :fdw_pwd);
IMPORT FOREIGN SCHEMA public
       LIMIT TO (ddi_data_points)
       FROM SERVER traj_srv INTO public;

-- intersection DB
CREATE SERVER map_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'REMOTE_MAP_HOST', dbname 'mapdb', port '5432');
CREATE USER MAPPING FOR postgres SERVER map_srv
  OPTIONS (user :fdw_user, password :fdw_pwd);
IMPORT FOREIGN SCHEMA public
       LIMIT TO (intersections)
       FROM SERVER map_srv INTO public;