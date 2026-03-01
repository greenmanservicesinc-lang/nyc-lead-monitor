#!/usr/bin/env python3
"""
NYC Pest Control Lead Monitor - UPGRADED VERSION
Green Man Services Inc.
-------------------------------------------------
WHAT'S NEW vs OLD VERSION:
 - DOB violations: FIXED (was broken/skipped)
 - ECB violations: ADDED (was missing entirely)
 - DOHMH: now catches ALL 4 pest codes (04L, 04M, 04N, 08A)
 - Owner Lookup: AUTOMATIC - every HPD/DOB/ECB lead now shows
   property owner name + mailing address (no more manual ACRIS!)
 - seen_leads.json: fixed to track all sources
 - Debug mode: OFF (no more hourly emails when 0 leads)
"""

import requests
import json
import os
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import re

# ── Configuration ────────────────────────────────────────────
EMAIL_TO        = "greenmanservicesinc@gmail.com"
EMAIL_FROM      = os.environ.get('SENDGRID_EMAIL', 'leads@yourleadmonitor.com')
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
SEEN_FILE       = 'seen_leads.json'

KEYWORDS = [
    'pest control', 'exterminator', 'mice', 'rats', 'rodent', 'roaches',
    'ants', 'bed bug', 'bedbug', 'termites', 'violation', 'bees', 'wasps',
    'cockroach', 'infestation', 'vermin', 'mold', 'water damage'
]

# ── Seen Leads Tracking ──────────────────────────────────────
def load_seen_leads():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, 'r') as f:
                data = json.load(f)
                # Make sure all keys exist (backward compat)
                for key in ['hpd', 'dohmh', 'reddit', '311', 'dob', 'ecb', 'craigslist']:
                    if key not in data:
                        data[key] = []
                return data
        except:
            pass
    return {'hpd': [], 'dohmh': [], 'reddit': [], '311': [], 'dob': [], 'ecb': [], 'craigslist': []}

def save_seen_leads(seen):
    with open(SEEN_FILE, 'w') as f:
        json.dump(seen, f)

# ── Owner Lookup (BBL-based, no external API needed) ─────────
BOROUGH_TO_CODE = {
    'MANHATTAN': '1', 'MN': '1',
    'BRONX':     '2', 'BX': '2',
    'BROOKLYN':  '3', 'BK': '3',
    'QUEENS':    '4', 'QN': '4', 'QU': '4',
    'STATEN ISLAND': '5', 'SI': '5'
}

def bbl_to_acris_url(bbl_str):
    """Build a direct ACRIS link for a specific property BBL."""
    if not bbl_str:
        return None
    bbl_clean = str(bbl_str).zfill(10)
    return f"https://a836-acris.nyc.gov/DS/DocumentSearch/BBL?BBL={bbl_clean}"

def lookup_owner_from_bbl(bbl_str):
    """
    Given a BBL (from HPD/DOB data directly), query NYC MapPLUTO
    for owner name + mailing address. No GeoSearch needed.
    """
    if not bbl_str:
        return None, None
    try:
        bbl_clean = str(bbl_str).zfill(10)
        boro  = bbl_clean[0]
        block = str(int(bbl_clean[1:6]))
        lot   = str(int(bbl_clean[6:10]))

        # MapPLUTO has owner name (ownername field)
        url = "https://data.cityofnewyork.us/resource/64uk-42ks.json"
        params = {'borocode': boro, 'block': block, 'lot': lot, '$limit': 1}
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200 and r.json():
            rec = r.json()[0]
            owner = rec.get('ownername', '').strip()
            addr  = f"{rec.get('address','').strip()}, {rec.get('city','').strip()}, {rec.get('state','').strip()} {rec.get('zipcode','').strip()}".strip(', ')
            if owner:
                return owner, addr

        # Fallback: try property assessment dataset
        url2 = "https://data.cityofnewyork.us/resource/yjxr-fw8i.json"
        r2 = requests.get(url2, params=params, timeout=15)
        if r2.status_code == 200 and r2.json():
            rec2 = r2.json()[0]
            owner = rec2.get('ownername', '').strip()
            addr  = f"{rec2.get('address','').strip()}, {rec2.get('city','').strip()}, {rec2.get('state','').strip()} {rec2.get('zipcode','').strip()}".strip(', ')
            if owner:
                return owner, addr

        return None, None
    except Exception as e:
        print(f"  Owner lookup error: {e}")
        return None, None

# ── NY DOS Business Entity Lookup ─────────────────────────────
BUSINESS_SUFFIXES = [
    'LLC', 'L.L.C', 'INC', 'CORP', 'CO.', ' CO ',
    'REALTY', 'PROPERTIES', 'PROPERTY', 'HOLDINGS',
    'ASSOCIATES', 'PARTNERS', 'LP', 'L.P', 'LLP',
    'REAL ESTATE', 'MGMT', 'MANAGEMENT', 'GROUP',
    'ENTERPRISES', 'VENTURES', 'TRUST', 'FUND'
]

def is_business_entity(name):
    """Check if owner name looks like a business (not a private person)."""
    if not name:
        return False
    name_upper = name.upper()
    return any(suffix in name_upper for suffix in BUSINESS_SUFFIXES)

def lookup_ny_dos(entity_name):
    """
    Search NY Department of State for business entity info.
    Returns agent name, office address, entity type, status.
    Uses the public NY Open Data API - no key needed.
    """
    if not entity_name or not is_business_entity(entity_name):
        print(f"  DOS: skipping '{entity_name}' (not a business entity)")
        return None
    try:
        print(f"  DOS: searching for '{entity_name}'")
        url = "https://data.ny.gov/resource/ej5i-dqe7.json"

        # Clean name - remove legal suffix for broader search
        search_name = entity_name.upper().strip()
        # Use first meaningful words for search (max 20 chars for safety)
        words = [w for w in search_name.split() if w not in ('LLC','INC','CORP','LLP','LP','THE','OF','AND')]
        short = ' '.join(words[:2]) if words else search_name[:20]

        params = {
            '$q': short,        # full-text search - more reliable than $where LIKE
            '$limit': 5,
            '$order': 'dos_id DESC'
        }
        r = requests.get(url, params=params, timeout=15)
        print(f"  DOS: status={r.status_code}, results={len(r.json()) if r.status_code==200 else 'N/A'}")

        if r.status_code != 200 or not r.json():
            print(f"  DOS: no results for '{short}'")
            return None

        results = r.json()

        # Find best match - entity name should contain our search words
        rec = None
        for result in results:
            result_name = result.get('current_entity_name', '').upper()
            if words and words[0] in result_name:
                rec = result
                print(f"  DOS: matched '{result_name}'")
                break
        if not rec:
            rec = results[0]
            print(f"  DOS: using first result '{rec.get('current_entity_name','')}'")

        entity_type = rec.get('entity_type', '').strip()
        status      = rec.get('entity_status', '').strip()
        dos_id      = rec.get('dos_id', '').strip()

        agent_name  = rec.get('registered_agent_name', '').strip()
        agent_addr1 = rec.get('registered_agent_address_1', '').strip()
        agent_city  = rec.get('registered_agent_city', '').strip()
        agent_state = rec.get('registered_agent_state', '').strip()
        agent_zip   = rec.get('registered_agent_zip', '').strip()

        office_addr = rec.get('principal_executive_office_address_1', '').strip()
        office_city = rec.get('principal_executive_office_city', '').strip()
        office_state= rec.get('principal_executive_office_state', '').strip()
        office_zip  = rec.get('principal_executive_office_zip', '').strip()

        agent_full  = ', '.join(filter(None, [agent_name, agent_addr1, agent_city, agent_state, agent_zip]))
        office_full = ', '.join(filter(None, [office_addr, office_city, office_state, office_zip]))
        dos_url     = f"https://apps.dos.ny.gov/publicInquiry/EntitySearch?SEARCH_TYPE=1&DOS_ID={dos_id}" if dos_id else None

        if not entity_type and not agent_full and not office_full:
            print(f"  DOS: found record but all fields empty")
            return None

        print(f"  DOS: SUCCESS - {entity_type} | {status} | agent: {agent_full[:40] if agent_full else 'none'}")
        return {
            'entity_type': entity_type,
            'status':      status,
            'agent':       agent_full or None,
            'office':      office_full or None,
            'dos_url':     dos_url
        }

    except Exception as e:
        print(f"  DOS lookup error: {e}")
        return None

# ── HPD Violations ────────────────────────────────────────────
def check_hpd_violations():
    print("Checking HPD violations...")
    base_url  = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
    week_ago  = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    boroughs  = ['BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN']
    pest_kw   = ['pest', 'roach', 'rodent', 'mice', 'rat', 'bedbug', 'bed bug', 'vermin', 'infestation']
    all_violations = []
    seen = load_seen_leads()

    for borough in boroughs:
        try:
            params = {
                '$where': f"inspectiondate > '{week_ago}T00:00:00'",
                '$limit': 50,
                'boro': borough
            }
            r = requests.get(base_url, params=params, timeout=30)
            if r.status_code != 200:
                print(f"  HPD ({borough}): status {r.status_code}")
                continue

            for v in r.json():
                if not isinstance(v, dict):
                    continue
                desc = str(v.get('novdescription', '')).lower()
                if not any(k in desc for k in pest_kw):
                    continue
                vid = str(v.get('violationid', ''))
                if vid and vid not in seen['hpd']:
                    addr_str = f"{v.get('housenumber', '')} {v.get('streetname', '')}".strip()
                    bbl = str(v.get('bbl', '')).strip()
                    owner_name, owner_addr = lookup_owner_from_bbl(bbl)
                    acris_url = bbl_to_acris_url(bbl) or "https://a836-acris.nyc.gov/DS/DocumentSearch/Index"
                    dos_info  = lookup_ny_dos(owner_name) if owner_name else None
                    all_violations.append({
                        'id':            vid,
                        'address':       f"{addr_str}, {borough}",
                        'apartment':     v.get('apartment', 'N/A'),
                        'zip':           v.get('zip', ''),
                        'class':         v.get('class', ''),
                        'description':   v.get('novdescription', ''),
                        'inspection_date': (v.get('inspectiondate', '')[:10]),
                        'owner_name':    owner_name,
                        'owner_addr':    owner_addr,
                        'acris_url':     acris_url,
                        'dos_info':      dos_info
                    })
                    seen['hpd'].append(vid)
        except Exception as e:
            print(f"  HPD ({borough}) error: {e}")

    seen['hpd'] = seen['hpd'][-2000:]
    save_seen_leads(seen)
    print(f"  Found {len(all_violations)} new HPD violations")
    return all_violations

# ── DOHMH Restaurant Violations (ALL 4 pest codes) ───────────
def check_dohmh_violations():
    print("Checking DOHMH violations...")
    base_url  = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    week_ago  = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    boroughs  = ['Brooklyn', 'Queens', 'Bronx', 'Manhattan']
    # 04L=mice, 04M=rats, 04N=roaches, 08A=not vermin-proof
    pest_codes = ['04L', '04M', '04N', '08A']
    all_violations = []
    seen = load_seen_leads()

    for borough in boroughs:
        for code in pest_codes:
            try:
                params = {
                    '$where': f"inspection_date > '{week_ago}T00:00:00'",
                    'violation_code': code,
                    'boro': borough,
                    '$limit': 25
                }
                r = requests.get(base_url, params=params, timeout=30)
                if r.status_code != 200:
                    continue
                for v in r.json():
                    if not isinstance(v, dict):
                        continue
                    uid = f"{v.get('camis', '')}_{v.get('inspection_date', '')}_{code}"
                    if uid not in seen['dohmh']:
                        all_violations.append({
                            'id':         uid,
                            'restaurant': v.get('dba', 'Unknown'),
                            'address':    f"{v.get('building', '')} {v.get('street', '')}, {borough}".strip(),
                            'zip':        v.get('zipcode', ''),
                            'phone':      v.get('phone', 'N/A'),
                            'violation_code': code,
                            'violation':  v.get('violation_description', ''),
                            'inspection_date': (v.get('inspection_date', '')[:10])
                        })
                        seen['dohmh'].append(uid)
            except Exception as e:
                print(f"  DOHMH ({borough}/{code}) error: {e}")

    seen['dohmh'] = seen['dohmh'][-2000:]
    save_seen_leads(seen)
    print(f"  Found {len(all_violations)} new DOHMH violations")
    return all_violations

# ── 311 Complaints ────────────────────────────────────────────
def check_311_complaints():
    print("Checking 311 complaints...")
    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    boroughs = ['BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN']
    all_complaints = []
    seen = load_seen_leads()

    for borough in boroughs:
        try:
            params = {
                '$where': f"created_date > '{week_ago}T00:00:00'",
                'complaint_type': 'Rodent',
                'borough': borough,
                '$limit': 25
            }
            r = requests.get(base_url, params=params, timeout=30)
            if r.status_code != 200:
                continue
            for c in r.json():
                if not isinstance(c, dict):
                    continue
                uid = str(c.get('unique_key', ''))
                if uid and uid not in seen['311']:
                    addr = c.get('incident_address', 'Address not provided')
                    all_complaints.append({
                        'id':         uid,
                        'type':       c.get('complaint_type', 'Unknown'),
                        'descriptor': c.get('descriptor', ''),
                        'address':    f"{addr}, {borough}",
                        'zip':        c.get('incident_zip', 'N/A'),
                        'created_date': (c.get('created_date', '')[:10]),
                        'status':     c.get('status', 'Unknown')
                    })
                    seen['311'].append(uid)
        except Exception as e:
            print(f"  311 ({borough}) error: {e}")

    seen['311'] = seen['311'][-2000:]
    save_seen_leads(seen)
    print(f"  Found {len(all_complaints)} new 311 complaints")
    return all_complaints

# ── DOB Violations (FIXED) ────────────────────────────────────
def check_dob_violations():
    print("Checking DOB violations...")
    # Correct endpoint: DOB violations dataset
    base_url = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    pest_kw  = ['pest', 'vermin', 'rodent', 'rat', 'mice', 'roach', 'infestation',
                'unsanitary', 'filth', 'garbage']
    all_violations = []
    seen = load_seen_leads()

    try:
        params = {
            '$where': f"issue_date > '{week_ago}T00:00:00'",
            '$limit': 100
        }
        r = requests.get(base_url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"  DOB: status {r.status_code}")
            return []

        for v in r.json():
            if not isinstance(v, dict):
                continue
            desc = str(v.get('description', '') or v.get('violation_type', '')).lower()
            if not any(k in desc for k in pest_kw):
                continue
            vid = str(v.get('isn_dob_bis_viol', v.get('number', '')))
            if vid and vid not in seen['dob']:
                addr_str = f"{v.get('house_number', '')} {v.get('street', '')}".strip()
                borough  = str(v.get('borough', '')).upper()
                bbl = str(v.get('bbl', '')).strip()
                owner_name, owner_addr = lookup_owner_from_bbl(bbl)
                acris_url = bbl_to_acris_url(bbl) or "https://a836-acris.nyc.gov/DS/DocumentSearch/Index"
                dos_info  = lookup_ny_dos(owner_name) if owner_name else None
                all_violations.append({
                    'id':          vid,
                    'address':     f"{addr_str}, {borough}",
                    'description': v.get('description', v.get('violation_type', 'N/A')),
                    'issue_date':  str(v.get('issue_date', ''))[:10],
                    'disposition': v.get('disposition_date', 'Open'),
                    'owner_name':  owner_name,
                    'owner_addr':  owner_addr,
                    'acris_url':   acris_url,
                    'dos_info':    dos_info
                })
                seen['dob'].append(vid)
    except Exception as e:
        print(f"  DOB error: {e}")

    seen['dob'] = seen['dob'][-2000:]
    save_seen_leads(seen)
    print(f"  Found {len(all_violations)} new DOB violations")
    return all_violations

# ── ECB Violations (NEW) ──────────────────────────────────────
def check_ecb_violations():
    """
    ECB = Environmental Control Board violations.
    These are fines issued for building code violations including
    pest/sanitary conditions. Hot leads — owner already got fined.
    """
    print("Checking ECB violations...")
    base_url = "https://data.cityofnewyork.us/resource/6bgk-3dad.json"
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    pest_kw  = ['pest', 'vermin', 'rodent', 'rat', 'mice', 'roach',
                'infestation', 'unsanitary', 'filth', 'extermination']
    all_violations = []
    seen = load_seen_leads()

    try:
        params = {
            '$where': f"issue_date > '{week_ago}T00:00:00'",
            '$limit': 100
        }
        r = requests.get(base_url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"  ECB: status {r.status_code}")
            return []

        for v in r.json():
            if not isinstance(v, dict):
                continue
            desc = str(v.get('violation_description', '') or v.get('section_law_description', '')).lower()
            if not any(k in desc for k in pest_kw):
                continue
            vid = str(v.get('ecb_violation_number', v.get('isn_dob_bis_viol', '')))
            if vid and vid not in seen['ecb']:
                addr_str = f"{v.get('house_number', '')} {v.get('street_name', '')}".strip()
                borough  = str(v.get('borough', '')).upper()
                bbl = str(v.get('bbl', '')).strip()
                owner_name, owner_addr = lookup_owner_from_bbl(bbl)
                acris_url = bbl_to_acris_url(bbl) or "https://a836-acris.nyc.gov/DS/DocumentSearch/Index"
                dos_info  = lookup_ny_dos(owner_name) if owner_name else None
                all_violations.append({
                    'id':          vid,
                    'address':     f"{addr_str}, {borough}",
                    'description': v.get('violation_description', v.get('section_law_description', 'N/A')),
                    'issue_date':  str(v.get('issue_date', ''))[:10],
                    'fine':        v.get('penalty_imposed', 'N/A'),
                    'status':      v.get('ecb_violation_status', 'N/A'),
                    'owner_name':  owner_name,
                    'owner_addr':  owner_addr,
                    'acris_url':   acris_url,
                    'dos_info':    dos_info
                })
                seen['ecb'].append(vid)
    except Exception as e:
        print(f"  ECB error: {e}")

    seen['ecb'] = seen['ecb'][-2000:]
    save_seen_leads(seen)
    print(f"  Found {len(all_violations)} new ECB violations")
    return all_violations

# ── Craigslist ────────────────────────────────────────────────
def check_craigslist():
    print("Checking Craigslist...")
    feeds = [
        'https://newyork.craigslist.org/search/bks?format=rss&query=pest+exterminator',
        'https://newyork.craigslist.org/search/que?format=rss&query=pest+exterminator',
        'https://newyork.craigslist.org/search/brx?format=rss&query=pest+exterminator',
        'https://newyork.craigslist.org/search/mnh?format=rss&query=pest+exterminator',
    ]
    new_posts = []
    seen = load_seen_leads()

    for feed_url in feeds:
        try:
            r = requests.get(feed_url, timeout=10)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            for item in root.findall('.//{http://purl.org/rss/1.0/}item')[:5]:
                title_elem = item.find('{http://purl.org/rss/1.0/}title')
                link_elem  = item.find('{http://purl.org/rss/1.0/}link')
                if title_elem is None or link_elem is None:
                    continue
                link = link_elem.text or ''
                m = re.search(r'/(\d+)\.html', link)
                if not m:
                    continue
                pid = m.group(1)
                if pid not in seen['craigslist']:
                    new_posts.append({'id': pid, 'title': title_elem.text or '', 'link': link})
                    seen['craigslist'].append(pid)
        except Exception as e:
            print(f"  Craigslist error: {e}")

    seen['craigslist'] = seen['craigslist'][-1000:]
    save_seen_leads(seen)
    print(f"  Found {len(new_posts)} new Craigslist posts")
    return new_posts

# ── Reddit ────────────────────────────────────────────────────
def check_reddit():
    print("Checking Reddit...")
    subreddits = [
        'AskNYC', 'nyc', 'Brooklyn', 'Queens', 'Bronx',
        'Bedbugs', 'Landlord', 'Bushwick', 'williamsburg', 'astoria'
    ]
    new_posts = []
    seen = load_seen_leads()
    headers = {'User-Agent': 'LeadMonitor/2.0'}
    nyc_kw  = ['nyc', 'new york', 'brooklyn', 'queens', 'bronx', 'manhattan']

    for sub in subreddits:
        try:
            r = requests.get(f"https://www.reddit.com/r/{sub}/new.json?limit=10",
                             headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            for post in r.json().get('data', {}).get('children', []):
                d    = post.get('data', {})
                pid  = d.get('id')
                text = (d.get('title', '') + ' ' + d.get('selftext', '')).lower()
                if not any(k.lower() in text for k in KEYWORDS):
                    continue
                if pid and pid not in seen['reddit']:
                    if sub.lower() in ['asknyc', 'nyc', 'brooklyn', 'queens', 'bronx'] or \
                       any(k in text for k in nyc_kw):
                        new_posts.append({
                            'id':        pid,
                            'subreddit': sub,
                            'title':     d.get('title', ''),
                            'text':      d.get('selftext', '')[:200],
                            'url':       f"https://reddit.com{d.get('permalink', '')}",
                            'created':   datetime.fromtimestamp(
                                            d.get('created_utc', 0)
                                         ).strftime('%Y-%m-%d %H:%M')
                        })
                        seen['reddit'].append(pid)
        except Exception as e:
            print(f"  Reddit r/{sub} error: {e}")

    seen['reddit'] = seen['reddit'][-1000:]
    save_seen_leads(seen)
    print(f"  Found {len(new_posts)} new Reddit posts")
    return new_posts

# ── Owner HTML Block ──────────────────────────────────────────
def owner_html(owner_name, owner_addr, acris_url, dos_info=None):
    html = ''

    if owner_name:
        html += f"""
        <div style="margin-top:8px; padding:10px; background:#e8f4e8; border-radius:4px; border-left:3px solid #28a745;">
            <div style="font-weight:bold; color:#28a745;">👤 Owner: <span style="color:#000;">{owner_name}</span></div>
            {'<div>📬 ' + owner_addr + '</div>' if owner_addr else ''}
        </div>"""
    
    if dos_info:
        status_color = '#28a745' if 'ACTIVE' in dos_info.get('status','').upper() else '#dc3545'
        html += f"""
        <div style="margin-top:6px; padding:10px; background:#e8f0fe; border-radius:4px; border-left:3px solid #4a6cf7;">
            <div style="font-weight:bold; color:#4a6cf7;">🏢 NY DOS Business Info:</div>
            <div>Type: <strong>{dos_info.get('entity_type','N/A')}</strong> &nbsp;|&nbsp; 
                 Status: <strong style="color:{status_color};">{dos_info.get('status','N/A')}</strong></div>
            {'<div>🧑‍💼 Agent: ' + dos_info['agent'] + '</div>' if dos_info.get('agent') else ''}
            {'<div>🏠 Office: ' + dos_info['office'] + '</div>' if dos_info.get('office') else ''}
            {'<a href="' + dos_info['dos_url'] + '" style="display:inline-block;margin-top:5px;background:#4a6cf7;color:white;padding:4px 10px;text-decoration:none;border-radius:4px;font-size:.85em;">NY DOS Record →</a>' if dos_info.get('dos_url') else ''}
        </div>"""

    html += f"""
        <div style="margin-top:6px;">
            <a href="{acris_url}"
               style="background:#0066ff;color:white;padding:5px 12px;text-decoration:none;border-radius:4px;font-size:.85em;">
               🔍 ACRIS Property Record →</a>
        </div>"""

    return html

# ── Send Email ────────────────────────────────────────────────
def send_email_alert(hpd, dohmh, c311, dob, ecb, craigslist, reddit):
    total = len(hpd) + len(dohmh) + len(c311) + len(dob) + len(ecb) + len(craigslist) + len(reddit)

    if total == 0:
        print("No new leads found — skipping email.")
        return

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; max-width: 800px; margin: auto; }}
        .section {{ margin:20px 0; padding:15px; border-left:4px solid #0066ff; background:#f5f5f5; }}
        .lead {{ margin:10px 0; padding:12px; background:white; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,.1); }}
        .emergency {{ border-left:4px solid #ff0000; }}
        h2 {{ color:#0066ff; margin:0 0 10px; }}
        .label {{ font-weight:bold; color:#333; }}
        .address {{ font-size:1.1em; color:#0066ff; font-weight:bold; }}
    </style></head><body>
    <h1>🎯 {total} New Leads — NYC Pest Control</h1>
    <p style="color:#666;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Green Man Services Inc.</p>
    """

    # HPD
    if hpd:
        html += f'<div class="section"><h2>🏛️ HPD Housing Violations ({len(hpd)} new)</h2>'
        for v in hpd[:15]:
            ec = 'emergency' if v.get('class') == 'C' else ''
            html += f"""
            <div class="lead {ec}">
                <div class="address">📍 {v['address']}</div>
                <div><span class="label">Apartment:</span> {v.get('apartment','N/A')}</div>
                <div><span class="label">Class:</span> {v.get('class','N/A')} {'⚠️ EMERGENCY' if v.get('class')=='C' else ''}</div>
                <div><span class="label">Issue:</span> {v.get('description','')[:180]}</div>
                <div><span class="label">Inspected:</span> {v.get('inspection_date','N/A')}</div>
                {owner_html(v.get('owner_name'), v.get('owner_addr'), v.get('acris_url','https://a836-acris.nyc.gov/DS/DocumentSearch/Index'), v.get('dos_info'))}
            </div>"""
        html += '</div>'

    # DOB
    if dob:
        html += f'<div class="section"><h2>🏗️ DOB Building Violations ({len(dob)} new)</h2>'
        for v in dob[:15]:
            html += f"""
            <div class="lead">
                <div class="address">📍 {v['address']}</div>
                <div><span class="label">Issue:</span> {v.get('description','')[:180]}</div>
                <div><span class="label">Date:</span> {v.get('issue_date','N/A')}</div>
                {owner_html(v.get('owner_name'), v.get('owner_addr'), v.get('acris_url','https://a836-acris.nyc.gov/DS/DocumentSearch/Index'), v.get('dos_info'))}
            </div>"""
        html += '</div>'

    # ECB
    if ecb:
        html += f'<div class="section"><h2>⚖️ ECB Violations ({len(ecb)} new)</h2>'
        for v in ecb[:15]:
            html += f"""
            <div class="lead">
                <div class="address">📍 {v['address']}</div>
                <div><span class="label">Violation:</span> {v.get('description','')[:180]}</div>
                <div><span class="label">Fine:</span> ${v.get('fine','N/A')} &nbsp;|&nbsp; <span class="label">Status:</span> {v.get('status','N/A')}</div>
                <div><span class="label">Date:</span> {v.get('issue_date','N/A')}</div>
                {owner_html(v.get('owner_name'), v.get('owner_addr'), v.get('acris_url','https://a836-acris.nyc.gov/DS/DocumentSearch/Index'), v.get('dos_info'))}
            </div>"""
        html += '</div>'

    # DOHMH
    if dohmh:
        code_labels = {'04L':'🐭 Mice','04M':'🐀 Rats','04N':'🪳 Roaches','08A':'🚪 Not Vermin-Proof'}
        html += f'<div class="section"><h2>🍽️ DOHMH Restaurant Violations ({len(dohmh)} new)</h2>'
        for v in dohmh[:15]:
            label = code_labels.get(v.get('violation_code',''), v.get('violation_code',''))
            html += f"""
            <div class="lead">
                <div class="address">📍 {v.get('restaurant','N/A')}</div>
                <div><span class="label">Address:</span> {v['address']}</div>
                <div><span class="label">Phone:</span> {v.get('phone','N/A')}</div>
                <div><span class="label">Type:</span> {label} — {v.get('violation','')[:150]}</div>
                <div><span class="label">Inspected:</span> {v.get('inspection_date','N/A')}</div>
            </div>"""
        html += '</div>'

    # 311
    if c311:
        html += f'<div class="section"><h2>📞 311 Complaints ({len(c311)} new)</h2>'
        for c in c311[:15]:
            html += f"""
            <div class="lead">
                <div class="address">📍 {c['address']}</div>
                <div><span class="label">Type:</span> {c.get('type','N/A')} — {c.get('descriptor','')}</div>
                <div><span class="label">Status:</span> {c.get('status','N/A')} &nbsp;|&nbsp; <span class="label">Date:</span> {c.get('created_date','N/A')}</div>
            </div>"""
        html += '</div>'

    # Craigslist
    if craigslist:
        html += f'<div class="section"><h2>📋 Craigslist ({len(craigslist)} new)</h2>'
        for p in craigslist[:10]:
            html += f"""
            <div class="lead">
                <div class="address">{p.get('title','N/A')}</div>
                <a href="{p.get('link','#')}" style="background:#6633cc;color:white;padding:6px 12px;text-decoration:none;border-radius:4px;">View Post →</a>
            </div>"""
        html += '</div>'

    # Reddit
    if reddit:
        html += f'<div class="section"><h2>💬 Reddit ({len(reddit)} new)</h2>'
        for p in reddit[:10]:
            html += f"""
            <div class="lead">
                <div class="address">r/{p.get('subreddit','')}: {p.get('title','N/A')}</div>
                <div style="color:#666;font-size:.9em;">{p.get('text','')[:150]}</div>
                <a href="{p.get('url','#')}" style="background:#ff4500;color:white;padding:6px 12px;text-decoration:none;border-radius:4px;">View Post →</a>
            </div>"""
        html += '</div>'

    html += """
    <hr>
    <p style="color:#666;font-size:.85em;">Green Man Services Inc. | Automated Lead Monitor v2.0<br>
    Respond fast — speed wins jobs!</p>
    </body></html>"""

    # Send via SendGrid
    try:
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            json={
                "personalizations": [{"to": [{"email": EMAIL_TO}],
                                       "subject": f"🎯 {total} New Leads — NYC Pest Control"}],
                "from":    {"email": EMAIL_FROM},
                "content": [{"type": "text/html", "value": html}]
            },
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}",
                     "Content-Type": "application/json"},
            timeout=10
        )
        if r.status_code == 202:
            print("✅ Email sent!")
        else:
            print(f"❌ Email failed: {r.status_code} — {r.text}")
    except Exception as e:
        print(f"Email error: {e}")

# ── Main ──────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"NYC Lead Monitor v2.0 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    hpd       = check_hpd_violations()
    dohmh     = check_dohmh_violations()
    c311      = check_311_complaints()
    dob       = check_dob_violations()
    ecb       = check_ecb_violations()
    craigslist = check_craigslist()
    reddit    = check_reddit()

    total = len(hpd)+len(dohmh)+len(c311)+len(dob)+len(ecb)+len(craigslist)+len(reddit)
    print(f"\n📊 Summary: {total} total new leads")
    print(f"   HPD:        {len(hpd)}")
    print(f"   DOB:        {len(dob)}  ← was broken, now fixed")
    print(f"   ECB:        {len(ecb)}  ← new source")
    print(f"   DOHMH:      {len(dohmh)}  ← now all 4 pest codes")
    print(f"   311:        {len(c311)}")
    print(f"   Craigslist: {len(craigslist)}")
    print(f"   Reddit:     {len(reddit)}")

    send_email_alert(hpd, dohmh, c311, dob, ecb, craigslist, reddit)
    print(f"\n✅ Done!\n")

if __name__ == "__main__":
    main()
