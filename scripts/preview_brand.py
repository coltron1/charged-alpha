"""Run the isolated review build without production DB access or analytics."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["GOOGLE_ANALYTICS_ID"] = ""
os.environ["BRAND_PREVIEW"] = "1"
os.environ["FLASK_DEBUG"] = "0"

if __name__ == "__main__":
    if '--background' in sys.argv:
        import subprocess
        with open('/tmp/charged-alpha-brand-preview.log', 'a') as log:
            process = subprocess.Popen([sys.executable, str(Path(__file__).resolve())], cwd=ROOT, env=os.environ.copy(), stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        print('Preview process:', process.pid, 'Log: /tmp/charged-alpha-brand-preview.log')
        sys.exit(0)
    from app import app
    app.run(host="127.0.0.1", port=int(os.environ.get("PREVIEW_PORT", "5055")), debug=False, threaded=True)
