import argparse, sys
from pathlib import Path
from spdatalab.dataset.ingest import ingest_list

def main():
    parser = argparse.ArgumentParser(prog='spdatalab')
    sub = parser.add_subparsers(dest='cmd')

    ing = sub.add_parser('ingest')
    ing.add_argument('--list', required=True)
    ing.add_argument('--out', required=True)

    args = parser.parse_args()
    if args.cmd == 'ingest':
        ingest_list(Path(args.list), Path(args.out))
    else:
        parser.print_help()

if __name__ == '__main__':
    main()