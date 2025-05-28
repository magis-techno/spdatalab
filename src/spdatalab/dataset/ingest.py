from pathlib import Path
from spdatalab.common.io_obs import init_moxing, download
from spdatalab.common.io_hive import hive_cursor

SQL = """SELECT id, origin_path, scene_obs_path
           FROM transform.ods_t_data_fragment_datalake
           WHERE name = %s
        """.strip()

def prepare_case(data_name: str, out_dir: Path) -> bool:
    with hive_cursor() as cur:
        cur.execute(SQL, (data_name,))
        row = cur.fetchone()
        if not row:
            print(f'not found {data_name}')
            return False
        obs_path = f"{row[2]}/maps/debug/rc_egopose.geojson"
    init_moxing()
    download(obs_path, out_dir / 'track_30s.geojson')
    return True

def ingest_list(list_file: Path, out_root: Path):
    names = list_file.read_text().splitlines()
    ok = 0
    for n in names:
        if n:
            if prepare_case(n.strip(), out_root / n.replace('/', '_')):
                ok += 1
    print(f'success {ok}/{len(names)}')