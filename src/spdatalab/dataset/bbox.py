from __future__ import annotations
import argparse
from pathlib import Path
import geopandas as gpd, pandas as pd
import shapely.geometry as sgeom
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor

LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"
BUFFER_M = 0.05

def chunk(lst,n):
    for i in range(0,len(lst),n):
        yield lst[i:i+n]

def fetch_meta(tokens):
    sql = ("SELECT id AS scene_token,name AS data_name,event_id,city_id,timestamp "
           "FROM transform.ods_t_data_fragment_datalake WHERE id IN %(tok)s")
    with hive_cursor() as cur:
        cur.execute(sql, {"tok": tuple(tokens)})
        cols=[d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(),columns=cols)

def fetch_bbox(names, eng):
    sql_query = text(f"""
        SELECT 
            dataset_name,
            MIN(ST_XMin(t.ext)) AS xmin,
            MIN(ST_YMin(t.ext)) AS ymin,
            MAX(ST_XMax(t.ext)) AS xmax,
            MAX(ST_YMax(t.ext)) AS ymax,
            bool_and(t.workstage = 2) AS all_good
        FROM (
            SELECT dataset_name, ST_Extent(point_lla) AS ext, workstage
            FROM {POINT_TABLE}
            WHERE dataset_name = ANY(:names_param)
            GROUP BY dataset_name, workstage) AS t
        GROUP BY t.dataset_name;""")
    return pd.read_sql(sql_query, eng, params={"names_param":names})

def build_geom(row):
    xmin,ymin,xmax,ymax=row[['xmin','ymin','xmax','ymax']]
    if xmin==xmax or ymin==ymax:
        xmin-=BUFFER_M; ymin-=BUFFER_M; xmax+=BUFFER_M; ymax+=BUFFER_M
    return sgeom.box(xmin,ymin,xmax,ymax)

def run(list_path,batch=1000):
    tokens=[t.strip() for t in Path(list_path).read_text().splitlines() if t.strip()]
    eng=create_engine(LOCAL_DSN,future=True)
    total=0
    for b in chunk(tokens,batch):
        meta=fetch_meta(b)
        if meta.empty: continue
        box=fetch_bbox(meta.data_name.tolist(),eng)
        merged=meta.merge(box,left_on='data_name',right_on='dataset_name')
        if merged.empty: continue
        merged['geom']=merged.apply(build_geom,axis=1)
        gdf=gpd.GeoDataFrame(merged,geometry='geom',crs=4326)
        gdf[['scene_token','data_name','event_id','city_id','timestamp','all_good','geom']]\
            .to_postgis('clips_bbox',eng,if_exists='append',index=False)
        total+=len(gdf)
        print(f'[batch done] cumulative={total}')
    print('Done.')

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--list',required=True); ap.add_argument('--batch',type=int,default=1000)
    run(ap.parse_args().list, ap.parse_args().batch)