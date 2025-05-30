import click
from spdatalab.common.db import get_conn

@click.group()
def scene_set():
    """SceneSet collection management"""

@scene_set.command()
@click.option('--name', required=True)
@click.option('--desc', default='')
@click.option('--jsonl', type=click.Path(exists=True), required=True, help='Path to JSONL containing scene_token')
def create(name, desc, jsonl):
    """Create a new scene set from JSONL file"""
    import json
    conn = get_conn()
    with conn, conn.cursor() as cur:
        cur.execute(
            'INSERT INTO scene_sets(name, description) VALUES(%s,%s) RETURNING set_id',
            (name, desc)
        )
        set_id = cur.fetchone()[0]
        rows = []
        with open(jsonl, 'r', encoding='utf-8') as fh:
            for line in fh:
                try:
                    token = json.loads(line)['scene_token']
                    rows.append((set_id, token))
                except (KeyError, json.JSONDecodeError):
                    continue
        cur.executemany(
            'INSERT INTO scene_set_members(set_id, scene_token) VALUES(%s,%s) ON CONFLICT DO NOTHING',
            rows
        )
    click.echo(f'✅ Created set {set_id} with {len(rows)} scenes')