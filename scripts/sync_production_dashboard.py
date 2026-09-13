#!/usr/bin/env python3
"""Plan/atomically export public-safe production data; never copy private runtime state."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from production_board import project_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, default=Path.home() / 'Desktop/CHARGED ALPHA EPISODES/_studio_motion_codex/production/dashboard/snapshot.json')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    try:
        data = project_snapshot(json.loads(args.snapshot.read_text()), json.loads((ROOT / 'data/shows_catalog.json').read_text()))
        payload = (json.dumps(data, indent=2, ensure_ascii=False) + '\n').encode()
        dest = ROOT / 'data/production_dashboard.json'
        changed = not dest.exists() or dest.read_bytes() != payload
        if args.execute and changed:
            fd, temporary = tempfile.mkstemp(dir=dest.parent, prefix='.production-')
            with os.fdopen(fd, 'wb') as stream:
                stream.write(payload)
            os.replace(temporary, dest)
        print(json.dumps({'mode': 'execute' if args.execute else 'plan', 'changed': changed,
                          'history': len(data['history']), 'upcoming': len(data['upcoming']),
                          'sha256': hashlib.sha256(payload).hexdigest()}))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({'status': 'blocked', 'error_type': type(error).__name__}))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
