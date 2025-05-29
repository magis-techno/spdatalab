"""Batch‑generate BBox for each scene_token list entry."""
from __future__ import annotations
import argparse
from pathlib import Path
import geopandas as gpd
import pandas as pd
import shapely.geometry as sgeom
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor

LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "traj_srv.ddi_data_points"  # via FDW
BUFFER_M = 0.05

def fetch_meta(tokens):
    sql = ("SELECT id AS scene_token,name AS data_name,event_id,city_id,timestamp "
           "FROM transform.ods_t_data_fragment_datalake WHERE id IN %(tok)s")
    with hive_cursor('app_gy1') as cur:
        cur.execute(sql, {"tok": tuple(tokens)})
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

def fetch_bbox(names, engine):
    sql = text(f"""
        SELECT dataset_name,
               ST_XMin(ext) xmin, ST_YMin(ext) ymin,
               ST_XMax(ext) xmax, ST_YMax(ext) ymax,
               bool_and(workstage = 2) all_good
        FROM (
          SELECT dataset_name, ST_Extent(point_lla) ext, workstage
          FROM   {POINT_TABLE}
          WHERE  dataset_name IN :names
          GROUP  BY dataset_name, workstage) t
        GROUP BY dataset_name;
    """)
    return pd.read_sql(sql, engine, params={"names": tuple(names)})

def build_geom(r):
    xmin, ymin, xmax, ymax = r[['xmin','ymin','xmax','ymax']]
    if xmin==xmax or ymin==ymax:
        xmin-=BUFFER_M; ymin-=BUFFER_M; xmax+=BUFFER_M; ymax+=BUFFER_M
    return sgeom.box(xmin, ymin, xmax, ymax)

def run(list_file):
    tokens = [t.strip() for t in Path(list_file).read_text().splitlines() if t.strip()]
    engine = create_engine(LOCAL_DSN, future=True)
    for batch in (tokens[i:i+1000] for i in range(0,len(tokens),1000)):
        meta = fetch_meta(batch)
        if meta.empty: continue
        box = fetch_bbox(meta.data_name.tolist(), engine)
        merged = meta.merge(box, left_on='data_name', right_on='dataset_name')
        merged['geom'] = merged.apply(build_geom, axis=1)
        gpd.GeoDataFrame(merged, geometry='geom', crs=4326)[[
            'scene_token','data_name','event_id','city_id','timestamp','all_good','geom']].to_postgis('clips_bbox', engine, if_exists='append', index=False)

if __name__ == '__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--list',required=True); run(ap.parse_args().list)