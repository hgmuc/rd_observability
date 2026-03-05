from __future__ import annotations
from pathlib import Path
from typing import Union, Set

DB_PATH: Path = Path("observability.sqlite")
OBS_API_URL: Union[str, None] = None

REL_FILE_TYPES: Set[str] = set([
    'geojson', 'gpx', 'csv', 'txt', 'md', 'gzip', 'pkl', 'zip', 'py', 
    'html', 'js', 'jpg', 'webp', 'png', 'avif', 'jpeg', 'css'])

