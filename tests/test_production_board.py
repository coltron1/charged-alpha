import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from production_board import project_snapshot, load_board, public_url


def snapshot():
    row = {'ticker': 'XYZ', 'company': 'Example', 'report_date': '2026-09-14', 'report_time': 'amc',
           'report_at': '2026-09-14T16:30:00-04:00', 'screen_status': 'candidate',
           'planned_date': '2026-09-15', 'date_status': 'Issuer-announced date', 'source_url': 'https://example.com/earnings'}
    return {'schema': 'charged-alpha-production-dashboard/1', 'generated_at': '2026-09-13T13:00:00Z',
            'source_health': {'fetched_at': '2026-09-13T13:00:00Z', 'fetch_status': 'fresh'},
            'forecast': {'daily_target': 10, 'date_cards': [{'candidates': [row]}]},
            'completed': [{'ticker': 'XYZ', 'period': 'Q1 2026', 'evidence': 'actual_bundle_DONE',
                           'completed_at': '2026-09-12T20:00:00Z', 'video_url': 'https://youtu.be/TestVideo01'}],
            'schedule': {'status': 'PAUSED'}, 'claim': '/Users/private/claim', 'pid': 123, 'secret': 'never-public'}


class ProductionBoardTests(unittest.TestCase):
    def test_public_projection_removes_private_fields_and_keeps_verified_times(self):
        result = project_snapshot(snapshot(), {'episodes': [{'ticker': 'XYZ', 'company': 'Example Inc.'}]})
        raw = json.dumps(result)
        for private in ('/Users/', 'never-public', 'PAUSED', '"pid"', '"claim"'):
            self.assertNotIn(private, raw)
        self.assertEqual(result['upcoming'][0]['report_time_label'], '3:30 PM CDT')
        self.assertEqual(result['history'][0]['stock_url'], '/shows/xyz')
        self.assertEqual(result['upcoming'][0]['date_status'], 'Issuer-announced')

    def test_plain_upload_is_not_completion_and_future_quarter_survives(self):
        data = snapshot()
        data['completed'].append({**data['completed'][0], 'period': 'Q2 2026', 'evidence': 'uploaded_only'})
        result = project_snapshot(data, {})
        self.assertEqual(len(result['history']), 1)
        self.assertEqual(len(result['upcoming']), 1)

    def test_bad_links_cannot_expose_local_files_or_secrets(self):
        for url in ('file:///Users/colton/data', 'http://127.0.0.1:8766/', 'https://example.com/?apikey=secret', 'javascript:alert(1)'):
            self.assertIsNone(public_url(url))

    def test_old_calendar_is_stale_and_past_reports_are_removed(self):
        data = project_snapshot(snapshot(), {})
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'board.json'; path.write_text(json.dumps(data))
            current = load_board(path, datetime(2026, 9, 13, 14, tzinfo=timezone.utc))
            later = load_board(path, datetime(2026, 9, 16, 14, tzinfo=timezone.utc))
        self.assertFalse(current['stale']); self.assertTrue(later['stale'])
        self.assertEqual(later['upcoming'], [])
        self.assertEqual(len(later['history']), 1)

    def test_routes_show_three_preview_entries_and_expand(self):
        import app as site
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'board.json'
            data = project_snapshot(snapshot(), {})
            data['history'] = [{**data['history'][0], 'ticker': ticker} for ticker in ('AAA', 'BBB', 'CCC', 'DDD')]
            data['upcoming'] = [{**data['upcoming'][0], 'ticker': ticker} for ticker in ('EEE', 'FFF', 'GGG', 'HHH')]
            path.write_text(json.dumps(data))
            board = load_board(path, datetime(2026, 9, 13, 14, tzinfo=timezone.utc))
        with patch('app.load_board', return_value=board):
            home = site.app.test_client().get('/')
            full = site.app.test_client().get('/production')
        from lxml import html
        page = html.fromstring(home.data)
        self.assertEqual(home.status_code, 200); self.assertEqual(full.status_code, 200)
        columns = page.xpath('//*[contains(concat(" ", normalize-space(@class), " "), " production-preview__column ")]')
        self.assertEqual(len(columns), 2)
        self.assertEqual([len(c.xpath('.//li')) for c in columns], [3, 3])
        self.assertIn(b'/production', home.data)
        self.assertNotIn(b'pilot_state', full.data)


if __name__ == '__main__':
    unittest.main()
