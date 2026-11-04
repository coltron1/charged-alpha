#!/usr/bin/env python3
"""Plan/apply one review-bound ETN segment label SVG revision; never publish or relax imports."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path('/Users/colton/Projects/charged-alpha-production')
SITE = Path(__file__).resolve().parents[1]
EPISODE = 'ETN-Q3-2026'
SLUG = 'etn-q3-2026'
FILE = SLUG + '-research-packet.html'
OLD_HTML = '2d381c32aee6760e4bc69cc1b1d419dd0a616f8db4ff0afa1269698799fef58b'
REPORT = 'charged-alpha-published-etn-display-plan/1'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def plain(path):
    path = Path(path).absolute()
    require('..' not in path.parts, 'Parent traversal refused')
    require(not any(p.is_symlink() for p in [path, *path.parents]), 'Symlink refused')
    return path


def encoded(value):
    return (json.dumps(value, indent=2) + '\n').encode()


def corrected_html(raw):
    """Separate six exact paired-bar labels; preserve all research/chart data."""
    require(digest(raw) == OLD_HTML, 'Only the exact published ETN predecessor is admitted')
    result = raw
    for title, changes in [
        ('Segment sales', [('200.5', '184.5', '$1,079M'), ('212.7', '196.7', '$775M')]),
        ('Segment operating profit', [('198.8', '182.8', '$280M')]),
    ]:
        figure = re.search(rb'<figure><figcaption><b>' + re.escape(title.encode()) + rb'</b>.*?</figure>', result, re.S)
        require(figure is not None, 'Exact segment figure missing')
        svg = figure.group()
        for old_y, new_y, label in changes:
            pattern = rb'(<text\b[^>]*\by=")' + re.escape(old_y.encode()) + rb'("[^>]*>' + re.escape(label.encode()) + rb'</text>)'
            svg, count = re.subn(pattern, lambda m: m.group(1) + new_y.encode() + m.group(2), svg)
            require(count == 2, 'Exact desktop/mobile segment predecessor labels missing')
        result = result[:figure.start()] + svg + result[figure.end():]
    return result


def targets(root, site):
    queue = root / '_studio_queue' / EPISODE
    producer = root / '_pipeline_v2/episodes' / EPISODE / '_packet'
    return [producer / FILE, producer / 'packet_meta.json', queue / 'packet' / FILE,
            queue / 'packet/packet_meta.json', queue / 'handoff.json',
            queue / 'producer_staging_receipt.json', site / 'data/research_packets' / (SLUG + '.html'),
            site / 'data/research_packets.json']


def git_head(site):
    return subprocess.check_output(['git', '-C', str(site), 'rev-parse', 'HEAD'], text=True).strip()


def prepare(root, site, directory):
    root, site, directory = plain(root), plain(site), plain(directory)
    require(not directory.exists(), 'Use a new plan directory; retain prior evidence')
    queue = root / '_studio_queue' / EPISODE
    require(not (queue / 'DONE').exists() and not (root / '_studio_queue/_done' / EPISODE).exists(), 'Completed episode cannot be revised')
    paths = [plain(p) for p in targets(root, site)]
    before = [p.read_bytes() for p in paths]
    require(before[0] == before[2] == before[6], 'Published, producer and queue predecessor bytes differ')
    require(before[1] == before[3], 'Producer and queue packet metadata differ')
    after = list(before)
    after[0] = after[2] = after[6] = corrected_html(before[0])
    checksum = digest(after[0])
    meta = json.loads(before[1]); require(meta['sha256'] == OLD_HTML, 'Metadata predecessor differs')
    meta.update(sha256=checksum, bytes=len(after[0])); after[1] = after[3] = encoded(meta)
    handoff = json.loads(before[4]); require(handoff['episode'] == EPISODE and handoff['packet']['sha256'] == OLD_HTML, 'Handoff identity differs')
    handoff['packet']['sha256'] = checksum; after[4] = encoded(handoff)
    receipt = json.loads(before[5])
    require(receipt['episode'] == EPISODE, 'Staging receipt identity differs')
    for index in (0, 1):
        key = str(paths[index]); require(receipt['source_sha256'][key] == digest(before[index]), 'Staging source binding differs')
        receipt['source_sha256'][key] = digest(after[index])
    for index, key in [(2, 'packet/' + FILE), (3, 'packet/packet_meta.json'), (4, 'handoff.json')]:
        require(receipt['staged_file_sha256'][key] == digest(before[index]), 'Staged binding differs')
        receipt['staged_file_sha256'][key] = digest(after[index])
    if 'published_display_revision' in receipt:
        receipt.setdefault('published_display_revision_history', []).append(receipt['published_display_revision'])
    receipt['published_display_revision'] = {'previous_html_sha256': OLD_HTML, 'html_sha256': checksum,
                                           'reason': 'Desktop/mobile segment sales and operating-profit label separation only'}
    after[5] = encoded(receipt)
    registry = json.loads(before[7]); matches = [p for p in registry['packets'] if p['slug'] == SLUG]
    require(len(matches) == 1 and matches[0]['sha256'] == OLD_HTML, 'Exact published registry predecessor missing')
    record = matches[0]; require(record['source_episode'] == EPISODE, 'Registry episode differs')
    require((queue / 'packet/packet.json').read_bytes() == (paths[0].parent / 'packet.json').read_bytes(), 'Research data source differs')
    require(digest((queue / 'packet/packet.json').read_bytes()) == record['source_sha256']['packet/packet.json'], 'Published research data binding differs')
    for index, key in [(2, 'packet/' + FILE), (3, 'packet/packet_meta.json'), (4, 'handoff.json')]:
        require(record['source_sha256'][key] == digest(before[index]), 'Registry source predecessor differs')
        record['source_sha256'][key] = digest(after[index])
    record['sha256'] = checksum; after[7] = encoded(registry)
    protected = [queue / 'READY', queue / 'CLAIMED', queue / 'packet/packet.json', paths[0].parent / 'packet.json']
    job = root / '_studio_motion_codex/production/queue_jobs' / EPISODE / 'short_upload_job.json'
    if job.exists(): protected.append(job)
    protected_hashes = {str(plain(p)): digest(p.read_bytes()) for p in protected}
    directory.mkdir(parents=True, mode=0o700)
    updates = []
    for index, (path, old, new) in enumerate(zip(paths, before, after)):
        original, candidate = directory / f'{index}.before', directory / f'{index}.after'
        original.write_bytes(old); candidate.write_bytes(new)
        updates.append({'path': str(path), 'before': str(original), 'after': str(candidate),
                        'before_sha256': digest(old), 'after_sha256': digest(new)})
    marker = queue / 'PACKET_DONE'
    require(marker.is_file(), 'Existing exact publication proof is required')
    proof = json.loads(marker.read_bytes()); require(proof['source_sha256'] == OLD_HTML and proof['passed'] is True, 'Publication predecessor differs')
    (directory / 'original_PACKET_DONE.json').write_bytes(marker.read_bytes())
    plan = {'schema': REPORT, 'episode_id': EPISODE, 'root': str(root), 'site': str(site),
            'base_commit': git_head(site), 'old_html_sha256': OLD_HTML, 'new_html_sha256': checksum,
            'implementation_sha256': digest(Path(__file__).read_bytes()), 'updates': updates,
            'protected': protected_hashes, 'original_PACKET_DONE_sha256': digest(marker.read_bytes()),
            'p8_status': 'HOLD; fresh independent live P8 required after redeployment'}
    (directory / 'plan.json').write_bytes(encoded(plan))
    return plan


def apply(plan_path, review_path, root, site):
    require(not any(p.exists() for p in [root / '_HALT_AUTOPRODUCE', root / '_TTS_PAUSED.json',
                root / '_pipeline_v2/_TTS_PAUSED.json',
                root / '_pipeline_v2/episodes' / EPISODE / '.tts_pause.json']), 'HALT/TTS pause preserves the current artifact')
    plan_path, review_path = plain(plan_path), plain(review_path)
    plan = json.loads(plan_path.read_bytes()); review = json.loads(review_path.read_bytes())
    require(plan['schema'] == REPORT and plan['episode_id'] == EPISODE, 'Exact ETN plan required')
    require(plan['root'] == str(root) and plan['site'] == str(site), 'Plan roots differ')
    require(review.get('schema') == 'charged-alpha-published-etn-display-review/1'
            and review.get('episode_id') == EPISODE and review.get('approved') is True
            and review.get('model', review.get('reviewer_model')) == 'gpt-6.1-terra'
            and review.get('reasoning_effort') in {'high', 'xhigh', 'max', 'ultra'}
            and review.get('plan_sha256') == digest(plan_path.read_bytes())
            and review.get('implementation_sha256') == digest(Path(__file__).read_bytes())
            and review.get('display_only') is True and review.get('all_research_content_preserved') is True,
            'Exact actual Terra high display/source/implementation approval required')
    require(plan['implementation_sha256'] == digest(Path(__file__).read_bytes()), 'Implementation changed')
    expected = [str(plain(p)) for p in targets(root, site)]
    require([u['path'] for u in plan['updates']] == expected, 'Revision target allowlist differs')
    require(git_head(site) == plan['base_commit'], 'Git HEAD changed')
    remote = subprocess.check_output(['git', '-C', str(site), 'rev-parse', 'origin/main'], text=True).strip()
    require(remote == plan['base_commit'], 'Fetch/reconcile remote before revising')
    queue = root / '_studio_queue' / EPISODE
    require(not (queue / 'DONE').exists() and not (root / '_studio_queue/_done' / EPISODE).exists(), 'Completed episode cannot be revised')
    with plain(queue / '.packet-publication.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for path, checksum in plan['protected'].items():
            require(digest(plain(path).read_bytes()) == checksum, 'Protected source/claim/job changed')
        for update in plan['updates']:
            for key in ('before', 'after'):
                path = plain(update[key]); require(path.parent == plan_path.parent, 'Evidence outside plan directory')
                require(digest(path.read_bytes()) == update[key + '_sha256'], 'Revision evidence changed')
            require(digest(plain(update['path']).read_bytes()) in {update['before_sha256'], update['after_sha256']}, 'Unexpected partial revision state')
        require(Path(plan['updates'][0]['after']).read_bytes() == corrected_html(Path(plan['updates'][0]['before']).read_bytes()), 'Non-display delta refused')
        journal = {'plan_sha256': digest(plan_path.read_bytes()), 'review_sha256': digest(review_path.read_bytes()), 'completed_paths': []}
        for update in plan['updates']:
            path = plain(update['path'])
            if digest(path.read_bytes()) != update['after_sha256']:
                fd, name = tempfile.mkstemp(prefix='.etn-display-', dir=path.parent)
                try:
                    with os.fdopen(fd, 'wb') as stream:
                        stream.write(Path(update['after']).read_bytes()); stream.flush(); os.fsync(stream.fileno())
                    os.chmod(name, 0o644); os.replace(name, path)
                finally:
                    if os.path.exists(name): os.unlink(name)
            journal['completed_paths'].append(str(path))
            (plan_path.parent / 'journal.json').write_bytes(encoded(journal))
    journal['status'] = 'installed_pending_deployment_and_fresh_verifier_P8'
    (plan_path.parent / 'journal.json').write_bytes(encoded(journal))
    return journal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan-directory', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--review', type=Path)
    args = parser.parse_args()
    try:
        require(args.plan_directory.absolute().is_relative_to(ROOT / '_studio_motion_codex/production/scheduled_runs'), 'Plan must remain in private scheduled-run evidence')
        if args.execute:
            require(args.review is not None, 'Review required before execution')
            result = apply(args.plan_directory / 'plan.json', args.review, ROOT, SITE)
        else:
            result = prepare(ROOT, SITE, args.plan_directory)
        print(json.dumps(result, indent=2))
        return 0
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print(json.dumps({'status': 'blocked', 'reason': str(error)}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
