#!/usr/bin/env python3
"""Plan offline, collect public descriptions, then apply a bound local evidence proposal.

No credentials, YouTube API writes, git commits or pushes. Existing evidence is
never replaced. Unclear links stay in the general library for later investigation.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.earnings_shorts import (CHANNEL_ID, description_video_ids, is_studio_edition,
    load_episode_link_evidence, reporting_periods, validate_link_record, verified_description_target, youtube_id)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def snapshot(path):
    return sha(path.read_bytes()) if path.is_file() else None


def inputs(catalog, evidence=None, limit=40):
    catalog = Path(catalog).resolve()
    evidence = Path(evidence).resolve() if evidence else catalog.parent/'episode_link_evidence.json'
    require(catalog != evidence, 'Catalog and evidence paths must be distinct')
    require(limit > 0, 'Limit must be positive')
    catalog_raw = catalog.read_bytes()
    data = json.loads(catalog_raw)
    evidence_sha = snapshot(evidence)
    records = load_episode_link_evidence(evidence)
    require(snapshot(evidence) == evidence_sha, 'Evidence changed during input validation')
    rows = [(row, 'studio_primary') for row in data.get('episodes', []) if is_studio_edition(row)]
    for section in data.get('video_sections', []):
        if section.get('title') == 'Shorts and Clips':
            rows.extend((row, 'short_earnings') for row in section.get('videos', [])
                        if not row.get('earnings_youtube_url'))
    candidates, seen, stale = [], set(), []
    for row, relation in rows:
        video_id = youtube_id(row.get('youtube_url'))
        if not re.fullmatch(r'[A-Za-z0-9_-]{11}', video_id) or video_id in seen:
            continue
        seen.add(video_id)
        if video_id in records:
            if records[video_id]['title'] != row.get('title'):
                stale.append(video_id)
            continue
        candidates.append({'video_id':video_id, 'title':row.get('title'), 'relation':relation,
                           'ticker':row.get('ticker') if relation == 'studio_primary' else None})
    require(snapshot(catalog) == sha(catalog_raw), 'Catalog changed during input validation')
    return data, {'status':'plan', 'network_called':False, 'catalog':str(catalog), 'evidence':str(evidence),
        'catalog_sha256':sha(catalog_raw), 'evidence_sha256':evidence_sha,
        'candidates':candidates[:limit], 'candidate_count':len(candidates), 'existing_stale_evidence':stale,
        'next_step':'Collect a proposal, inspect accepted/skipped entries, then apply it locally and rerun sync.'}


def fetch_public_video(video_id):
    require(re.fullmatch(r'[A-Za-z0-9_-]{11}', video_id), 'Invalid video ID')
    url = 'https://www.youtube.com/watch?v='+video_id
    request = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
    with urllib.request.urlopen(request, timeout=45) as response:
        page = response.read().decode('utf-8')
    match = re.search(r'(?:var )?ytInitialPlayerResponse = ', page)
    require(match is not None, 'Public video metadata unavailable')
    player = json.JSONDecoder().raw_decode(page[match.end():])[0]
    return player.get('videoDetails') or {}


def proposal_record(candidate, details, originals):
    video_id = candidate['video_id']
    require(details.get('videoId') == video_id and details.get('channelId') == CHANNEL_ID,
            'Public video ID/channel mismatch')
    require(details.get('title') == candidate['title'], 'Public title differs from catalog; reconcile catalog first')
    description = details.get('shortDescription')
    require(isinstance(description,str), 'Public description unavailable')
    targets = description_video_ids(description) & originals.keys()
    require(len(targets) == 1, 'No unique existing original earnings video in description')
    target_id = next(iter(targets))
    record = {'source_video_id':video_id, 'source_url':'https://www.youtube.com/watch?v='+video_id,
        'channel_id':CHANNEL_ID, 'title':details['title'], 'description':description,
        'description_sha256':sha(description.encode('utf-8')), 'fetched_at':datetime.now(timezone.utc).isoformat(),
        'target_video_id':target_id, 'relation':candidate['relation']}
    validate_link_record(video_id, record)
    target = originals[target_id]
    require(verified_description_target({'youtube_url':record['source_url'],'title':record['title']}, originals,
            {video_id:record}, record['relation']) is not None, 'Ambiguous description target')
    if record['relation'] == 'studio_primary':
        # The catalog's source row identity is checked again in apply().
        require(not is_studio_edition(target) and target.get('ticker') == candidate.get('ticker')
                and reporting_periods(target.get('quarter')),
                'Studio target must be its same-ticker original earnings period')
    return record


def collect(catalog, evidence, output, limit=40):
    data, plan = inputs(catalog, evidence, limit)
    output = Path(output).resolve()
    require(output not in {Path(plan['catalog']),Path(plan['evidence'])} and not output.exists() and not output.is_symlink(),
            'Proposal output must be a new distinct file')
    originals = {youtube_id(row.get('youtube_url')):row for row in data.get('episodes', [])
                 if youtube_id(row.get('youtube_url')) and not is_studio_edition(row)}
    records, skipped = {}, []
    for candidate in plan['candidates']:
        try:
            record = proposal_record(candidate, fetch_public_video(candidate['video_id']), originals)
            records[candidate['video_id']] = record
        except (OSError, ValueError, KeyError, TypeError) as error:
            # Public pages contain no credential headers; keep stable error types
            # instead of echoing raw HTTP exceptions or server content.
            reason = str(error) if isinstance(error, ValueError) else type(error).__name__
            skipped.append({'video_id':candidate['video_id'],'reason':reason})
    require(snapshot(Path(plan['catalog'])) == plan['catalog_sha256'] and snapshot(Path(plan['evidence'])) == plan['evidence_sha256'],
            'Catalog/evidence changed while collecting; no proposal written')
    payload = dict(plan, schema_version=1, status='collected', network_called=bool(plan['candidates']),
        collector_sha256=snapshot(Path(__file__)), records=records, skipped=skipped)
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x',encoding='utf-8') as stream:
        json.dump(payload,stream,indent=2,ensure_ascii=False);stream.write('\n')
    return {'status':'collected','network_called':payload['network_called'],'proposal':str(output),
            'accepted':len(records),'skipped':skipped,'candidate_count':plan['candidate_count']}


def apply(proposal):
    proposal = Path(proposal).resolve()
    raw = proposal.read_bytes();plan = json.loads(raw)
    require(plan.get('schema_version') == 1 and plan.get('status') == 'collected', 'Invalid collected proposal')
    require(plan.get('collector_sha256') == snapshot(Path(__file__)), 'Collector changed; recollect proposal')
    require(isinstance(plan.get('records'),dict), 'Proposal records missing')
    catalog,evidence=Path(plan['catalog']),Path(plan['evidence'])
    require(catalog.is_absolute() and evidence.is_absolute() and len({catalog,evidence,proposal}) == 3,
            'Proposal paths must be distinct and absolute')
    lock_path=Path(tempfile.gettempdir())/('chargedalpha-episode-evidence-'+sha(str(evidence).encode())+'.lock')
    with lock_path.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        current=snapshot(evidence)
        if current != plan['evidence_sha256']:
            existing=load_episode_link_evidence(evidence)
            require(all(existing.get(key)==value for key,value in plan['records'].items()),
                    'Evidence changed; preserve it and recollect')
            return {'status':'already_applied','network_called':False,'evidence':str(evidence),'added':0}
        require(snapshot(catalog) == plan['catalog_sha256'], 'Catalog changed; recollect against current primary IDs')
        data=json.loads(catalog.read_bytes())
        originals={youtube_id(row.get('youtube_url')):row for row in data.get('episodes',[])
                   if youtube_id(row.get('youtube_url')) and not is_studio_edition(row)}
        all_rows={youtube_id(row.get('youtube_url')):row for row in data.get('episodes',[])}
        for section in data.get('video_sections',[]):
            if section.get('title') == 'Shorts and Clips':
                all_rows.update({youtube_id(row.get('youtube_url')):row for row in section.get('videos',[])})
        payload=json.loads(evidence.read_bytes()) if evidence.exists() else {'schema_version':1,'records':{}}
        records=load_episode_link_evidence(evidence)
        candidates={item['video_id']:item for item in plan.get('candidates',[])}
        for source_id,record in plan['records'].items():
            require(source_id in candidates and record.get('relation')==candidates[source_id].get('relation'),
                    'Proposal record is not an originally selected candidate')
            validate_link_record(source_id,record)
            require(source_id in all_rows and verified_description_target(all_rows[source_id],originals,
                    {source_id:record},record['relation']) is not None, 'Proposal no longer binds catalog source and target')
            require(source_id not in records or records[source_id] == record, 'Existing evidence differs; preserved')
            if record['relation']=='studio_primary':
                source=all_rows[source_id];target=originals[record['target_video_id']]
                require(is_studio_edition(source) and source.get('ticker')==target.get('ticker'), 'Studio source/target identity mismatch')
            records[source_id]=record
        if not plan['records']:
            return {'status':'unchanged','network_called':False,'evidence':str(evidence),'added':0}
        require(proposal.read_bytes()==raw and snapshot(catalog)==plan['catalog_sha256'] and snapshot(evidence)==current,
                'Proposal/catalog/evidence changed during apply; no ledger write')
        payload['records']=records
        with tempfile.NamedTemporaryFile('w',dir=evidence.parent,prefix='.'+evidence.name+'.',delete=False,encoding='utf-8') as stream:
            temporary=Path(stream.name);json.dump(payload,stream,indent=2,ensure_ascii=False);stream.write('\n');stream.flush();os.fsync(stream.fileno())
        try:
            require(snapshot(catalog)==plan['catalog_sha256'] and snapshot(evidence)==current and proposal.read_bytes()==raw,
                    'Source changed before ledger replace')
            os.replace(temporary,evidence)
        finally:
            if temporary.exists():temporary.unlink()
    return {'status':'applied','network_called':False,'evidence':str(evidence),'added':len(plan['records'])}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog',type=Path,default=Path('data/shows_catalog.json'))
    parser.add_argument('--evidence',type=Path)
    parser.add_argument('--limit',type=int,default=40)
    mode=parser.add_mutually_exclusive_group();mode.add_argument('--collect',action='store_true');mode.add_argument('--apply',type=Path)
    parser.add_argument('--output',type=Path,help='New proposal JSON required with --collect')
    args=parser.parse_args()
    try:
        if args.apply:result=apply(args.apply)
        elif args.collect:
            require(args.output is not None,'--collect requires --output');result=collect(args.catalog,args.evidence,args.output,args.limit)
        else:result=inputs(args.catalog,args.evidence,args.limit)[1]
        print(json.dumps(result,indent=2));return 0
    except (OSError,ValueError,KeyError,TypeError) as error:
        print(json.dumps({'status':'blocked','error':str(error),'network_called':bool(args.collect)}));return 2

if __name__=='__main__':raise SystemExit(main())
