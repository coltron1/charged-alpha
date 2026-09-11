"""Loopback-only email UI fixture. In-memory DB, fake keys, no sending worker."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if '--background' in sys.argv:
    with open('/tmp/charged-alpha-alert-preview.log', 'a') as log:
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve())], cwd=ROOT,
                                 stdout=log, stderr=log, start_new_session=True)
    print('Email UI preview PID:', child.pid)
    raise SystemExit(0)
os.environ.update(DATABASE_URL='sqlite:///:memory:', GOOGLE_ANALYTICS_ID='', BRAND_PREVIEW='1',
                  STOCK_ALERTS_ENABLED='1', STOCK_ALERTS_PUBLIC='1', STOCK_ALERTS_WORKER='0',
                  RESEND_API_KEY='qa-never-sent', RESEND_WEBHOOK_SECRET='qa-never-sent',
                  STOCK_ALERTS_POSTAL_ADDRESS='Example Business Address')
sys.path.insert(0, str(ROOT))
from app import app
from email_alert_models import StockAlertMessage
app.config['TESTING'] = True


@app.get('/__qa/confirmation')
def qa_confirmation():
    message = StockAlertMessage.query.filter_by(state='queued').order_by(StockAlertMessage.created_at.desc()).first()
    if not message:
        return {'error': 'No pending QA message'}, 404
    body = json.loads(message.payload_json)
    token = re.search(r'/alerts/confirm/([^\s<"]+)', body['text']).group(1)
    return {'path': '/alerts/confirm/' + token}


app.run(host='127.0.0.1', port=5056, debug=False)
