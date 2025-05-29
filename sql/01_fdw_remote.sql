-- Example FDW mapping template (fill host/user/pwd)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER traj_srv FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'REMOTE_TRAJ_HOST', dbname 'trajdb', port '5432');
CREATE USER MAPPING FOR postgres SERVER traj_srv
  OPTIONS (user 'readonly', password '***');
IMPORT FOREIGN SCHEMA public LIMIT TO (traj_tracks)
  FROM SERVER traj_srv INTO public;