import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / '.env')

def getenv(key: str, default: str | None = None, required: bool = False):
    val = os.getenv(key, default)
    if required and val is None:
        raise RuntimeError(f'Missing env {key}')
    return val