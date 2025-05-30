"""Utility to import scene_tokens from a JSONL file into scene_set tables."""
import argparse, json, sys
from spdatalab.common.db import get_conn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--jsonl', required=True)
    ap.add_argument('--set-id', type=int, required=True)
    args = ap.parse_args()

    conn = get_conn()
    with conn, conn.cursor() as cur, open(args.jsonl, 'r', encoding='utf-8') as fh:
        rows = []
        for ln in fh:
            try:
                token = json.loads(ln)['scene_token']
                rows.append((args.set_id, token))
            except (KeyError, json.JSONDecodeError):
                continue
        if not rows:
            sys.exit('No valid scene_token found')
        cur.executemany(
            'INSERT INTO scene_set_members(set_id, scene_token) VALUES(%s,%s) ON CONFLICT DO NOTHING',
            rows
        )
    print(f'Imported {len(rows)} tokens into set {args.set_id}')

if __name__ == '__main__':
    main()