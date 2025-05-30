DROP SERVER IF EXISTS traj_srv CASCADE;
DROP SERVER IF EXISTS map_srv CASCADE;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER traj_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '10.170.30.193', dbname 'dataset_gy1', port '9001');
CREATE USER MAPPING FOR postgres SERVER traj_srv
  OPTIONS (user '**', password '**');
IMPORT FOREIGN SCHEMA public LIMIT TO (ddi_data_points)
  FROM SERVER traj_srv INTO public;

CREATE SERVER map_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '10.170.30.193', dbname 'rcdatalake_gy1', port '9001');
CREATE USER MAPPING FOR postgres SERVER map_srv
  OPTIONS (user '**', password '*');
IMPORT FOREIGN SCHEMA public LIMIT TO (intersections)
  FROM SERVER map_srv INTO public;