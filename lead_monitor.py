#!/usr/bin/env python3
"""
NYC Pest Control Lead Monitor
Monitors HPD violations, DOHMH violations, 311 complaints, DOB violations,
Craigslist posts, Twitter/X posts, and Reddit posts
Sends email alerts for new leads
"""

import requests
import json
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import xml.etree.ElementTree as ET
import re

# Configuration
EMAIL_TO = "greenmanservicesinc@gmail.com"
EMAIL_FROM = os.environ.get('SENDGRID_EMAIL', 'leads@yourleadmonitor.com')
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')

# Keywords to monitor
KEYWORDS = [
    'pest control', 'exterminator', 'mice', 'rats', 'rodent', 'roaches',
    'ants', 'bed bug', 'bedbug', 'termites', 'violation', 'bees', 'wasps',
    'cockroach', 'infestation', 'vermin', 'mold', 'water damage'
]

# File to track what we've already seen
SEEN_FILE = 'seen_leads.json'

def load_seen_leads():
    """Load previously seen leads from file"""
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, 'r') as f:
            return json.load(f)
    return {'hpd': [], 'dohmh': [], 'reddit': [], '311': [], 'dob': [], 'craigslist': [], 'twitter': []}

def save_seen_leads(seen):
    """Save seen leads to file"""
    with open(SEEN_FILE, 'w') as f:
        json.dump(seen, f)

def check_hpd_violations():
    """Check NYC HPD violations for pest-related issues"""
    print("Checking HPD violations...")
    
    # NYC Open Data API endpoint
    base_url = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
    
    # Get violations from last 24 hours
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
    
    # Query for pest-related violations in Brooklyn, Queens, Bronx
    query = f"""
    $where=inspectiondate > '{yesterday}' AND 
    (boro = 'BROOKLYN' OR boro = 'QUEENS' OR boro = 'BRONX') AND
    (UPPER(novdescription) LIKE '%PEST%' OR 
     UPPER(novdescription) LIKE '%ROACH%' OR 
     UPPER(novdescription) LIKE '%RODENT%' OR 
     UPPER(novdescription) LIKE '%MICE%' OR 
     UPPER(novdescription) LIKE '%RAT%' OR
     UPPER(novdescription) LIKE '%BED BUG%' OR
     UPPER(novdescription) LIKE '%BEDBUG%' OR
     UPPER(novdescription) LIKE '%VERMIN%' OR
     UPPER(novdescription) LIKE '%INFESTATION%')
    &$limit=50
    """
    
    try:
        response = requests.get(base_url, params={'$where': query.strip()})
        violations = response.json()
        
        new_violations = []
        seen = load_seen_leads()
        
        for v in violations:
            violation_id = v.get('violationid')
            if violation_id and violation_id not in seen['hpd']:
                new_violations.append({
                    'id': violation_id,
                    'address': f"{v.get('housenumber', '')} {v.get('streetname', '')}, {v.get('boro', '')}",
                    'apartment': v.get('apartment', 'N/A'),
                    'zip': v.get('zip', ''),
                    'class': v.get('class', ''),
                    'description': v.get('novdescription', ''),
                    'inspection_date': v.get('inspectiondate', ''),
                    'status': v.get('currentstatus', '')
                })
                seen['hpd'].append(violation_id)
        
        # Keep only last 1000 IDs to prevent file from growing too large
        seen['hpd'] = seen['hpd'][-1000:]
        save_seen_leads(seen)
        
        print(f"Found {len(new_violations)} new HPD violations")
        return new_violations
        
    except Exception as e:
        print(f"Error checking HPD: {e}")
        return []

def check_dohmh_violations():
    """Check DOHMH restaurant violations"""
    print("Checking DOHMH restaurant violations...")
    
    base_url = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    
    # Get inspections from last 24 hours
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
    
    # Violation codes for pests: 04L (mice), 04M (rats), 04N (roaches), 08A (not vermin proof)
    query = f"""
    $where=inspection_date > '{yesterday}' AND
    (boro = 'Brooklyn' OR boro = 'Queens' OR boro = 'Bronx') AND
    violation_code IN ('04L', '04M', '04N', '08A')
    &$limit=50
    """
    
    try:
        response = requests.get(base_url, params={'$where': query.strip()})
        violations = response.json()
        
        new_violations = []
        seen = load_seen_leads()
        
        for v in violations:
            # Create unique ID from restaurant + inspection date
            unique_id = f"{v.get('camis', '')}_{v.get('inspection_date', '')}"
            
            if unique_id not in seen['dohmh']:
                new_violations.append({
                    'id': unique_id,
                    'restaurant': v.get('dba', 'Unknown'),
                    'address': f"{v.get('building', '')} {v.get('street', '')}, {v.get('boro', '')}",
                    'zip': v.get('zipcode', ''),
                    'phone': v.get('phone', 'N/A'),
                    'violation_code': v.get('violation_code', ''),
                    'violation': v.get('violation_description', ''),
                    'inspection_date': v.get('inspection_date', ''),
                    'grade': v.get('grade', 'N/A')
                })
                seen['dohmh'].append(unique_id)
        
        seen['dohmh'] = seen['dohmh'][-1000:]
        save_seen_leads(seen)
        
        print(f"Found {len(new_violations)} new DOHMH violations")
        return new_violations
        
    except Exception as e:
        print(f"Error checking DOHMH: {e}")
        return []

def check_311_complaints():
    """Check NYC 311 service requests for pest-related complaints"""
    print("Checking NYC 311 complaints...")
    
    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    
    # Get complaints from last 24 hours
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
    
    # Query for pest-related complaints in Brooklyn, Queens, Bronx
    query = f"""
    $where=created_date > '{yesterday}' AND
    (borough = 'BROOKLYN' OR borough = 'QUEENS' OR borough = 'BRONX') AND
    (UPPER(complaint_type) LIKE '%RODENT%' OR
     UPPER(complaint_type) LIKE '%PEST%' OR
     UPPER(descriptor) LIKE '%RODENT%' OR
     UPPER(descriptor) LIKE '%MICE%' OR
     UPPER(descriptor) LIKE '%RAT%' OR
     UPPER(descriptor) LIKE '%ROACH%' OR
     UPPER(descriptor) LIKE '%BED BUG%' OR
     UPPER(descriptor) LIKE '%BEDBUG%' OR
     UPPER(descriptor) LIKE '%UNSANITARY%')
    &$limit=50
    """
    
    try:
        response = requests.get(base_url, params={'$where': query.strip()})
        complaints = response.json()
        
        new_complaints = []
        seen = load_seen_leads()
        
        for c in complaints:
            unique_number = c.get('unique_key')
            
            if unique_number and unique_number not in seen['311']:
                # Build address
                address_parts = []
                if c.get('incident_address'):
                    address_parts.append(c.get('incident_address'))
                if c.get('borough'):
                    address_parts.append(c.get('borough'))
                
                new_complaints.append({
                    'id': unique_number,
                    'type': c.get('complaint_type', 'Unknown'),
                    'descriptor': c.get('descriptor', ''),
                    'address': ', '.join(address_parts) if address_parts else 'Address not provided',
                    'zip': c.get('incident_zip', 'N/A'),
                    'created_date': c.get('created_date', ''),
                    'status': c.get('status', 'Unknown'),
                    'agency': c.get('agency', 'N/A')
                })
                seen['311'].append(unique_number)
        
        seen['311'] = seen['311'][-1000:]
        save_seen_leads(seen)
        
        print(f"Found {len(new_complaints)} new 311 complaints")
        return new_complaints
        
    except Exception as e:
        print(f"Error checking 311: {e}")
        return []

def check_dob_violations():
    """Check NYC Department of Buildings violations"""
    print("Checking DOB violations...")
    
    base_url = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"
    
    # Get violations from last 7 days (DOB updates slower)
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
    
    query = f"""
    $where=issue_date > '{week_ago}' AND
    (boro = 'BROOKLYN' OR boro = 'QUEENS' OR boro = 'BRONX') AND
    (UPPER(violation_type_code) LIKE '%HAZARD%' OR
     UPPER(violation_category) LIKE '%HAZARD%' OR
     UPPER(violation_type) LIKE '%UNSAFE%' OR
     UPPER(violation_type) LIKE '%UNSANITARY%')
    &$limit=30
    """
    
    try:
        response = requests.get(base_url, params={'$where': query.strip()})
        violations = response.json()
        
        new_violations = []
        seen = load_seen_leads()
        
        for v in violations:
            isn_dob_bis = v.get('isn_dob_bis_extract')
            
            if isn_dob_bis and isn_dob_bis not in seen['dob']:
                new_violations.append({
                    'id': isn_dob_bis,
                    'number': v.get('number', 'N/A'),
                    'address': f"{v.get('house_number', '')} {v.get('street', '')}, {v.get('boro', '')}",
                    'zip': v.get('zip', 'N/A'),
                    'violation_type': v.get('violation_type', 'Unknown'),
                    'category': v.get('violation_category', 'N/A'),
                    'issue_date': v.get('issue_date', ''),
                    'disposition': v.get('disposition_comments', 'Pending')
                })
                seen['dob'].append(isn_dob_bis)
        
        seen['dob'] = seen['dob'][-1000:]
        save_seen_leads(seen)
        
        print(f"Found {len(new_violations)} new DOB violations")
        return new_violations
        
    except Exception as e:
        print(f"Error checking DOB: {e}")
        return []

def check_reddit():
    """Check Reddit for pest control posts"""
    print("Checking Reddit...")
    
    subreddits = [
        # Main NYC subreddits
        'AskNYC', 'nyc', 'Brooklyn', 'Queens', 'Bronx',
        
        # Pest-specific
        'Bedbugs', 'Landlord',
        
        # Brooklyn neighborhoods
        'Bushwick', 'williamsburg', 'Ridgewood', 'FortGreene', 'ParkSlope',
        'BayRidge', 'greenpoint', 'BedStuy', 'crownheights',
        
        # Queens neighborhoods
        'astoria', 'flushing', 'corona', 'woodside', 'JacksonHeights',
        'ForestHills', 'Sunnyside', 'LIC', 'Elmhurst',
        
        # Manhattan neighborhoods
        'StuyTown', 'upperwestside', 'harlem', 'EastVillage',
        
        # Nassau County
        'nassaucounty', 'longisland', 'Hempstead', 'longbeach', 'Freeport',
        'ValleyStream', 'Levittown', 'Massapequa', 'Hicksville', 'Plainview',
        'Syosset', 'GardenCity', 'rockvillecentre', 'Oceanside'
    ]
    
    new_posts = []
    seen = load_seen_leads()
    
    headers = {'User-Agent': 'LeadMonitor/1.0'}
    
    for subreddit in subreddits:
        try:
            url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=25"
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                continue
                
            data = response.json()
            posts = data.get('data', {}).get('children', [])
            
            for post in posts:
                post_data = post.get('data', {})
                post_id = post_data.get('id')
                title = post_data.get('title', '').lower()
                selftext = post_data.get('selftext', '').lower()
                
                # Check if post contains keywords
                text_to_check = title + ' ' + selftext
                if not any(keyword.lower() in text_to_check for keyword in KEYWORDS):
                    continue
                
                # Check if we've seen this post
                if post_id and post_id not in seen['reddit']:
                    # Check if post is from NYC area
                    nyc_keywords = ['nyc', 'new york', 'brooklyn', 'queens', 'bronx', 'manhattan']
                    if subreddit.lower() in ['asknyc', 'nyc', 'brooklyn', 'queens', 'bronx'] or \
                       any(kw in text_to_check for kw in nyc_keywords):
                        
                        new_posts.append({
                            'id': post_id,
                            'subreddit': subreddit,
                            'title': post_data.get('title', ''),
                            'text': post_data.get('selftext', '')[:200],  # First 200 chars
                            'url': f"https://reddit.com{post_data.get('permalink', '')}",
                            'created': datetime.fromtimestamp(post_data.get('created_utc', 0)).strftime('%Y-%m-%d %H:%M:%S')
                        })
                        seen['reddit'].append(post_id)
            
        except Exception as e:
            print(f"Error checking r/{subreddit}: {e}")
            continue
    
    seen['reddit'] = seen['reddit'][-1000:]
    save_seen_leads(seen)
    
    print(f"Found {len(new_posts)} new Reddit posts")
    return new_posts

def check_craigslist():
    """Check Craigslist RSS feeds for pest-related posts"""
    print("Checking Craigslist...")
    
    # Craigslist RSS feeds for NYC
    feeds = [
        # Services wanted
        'https://newyork.craigslist.org/search/bks?format=rss&query=pest+exterminator+mice+rats+roaches',
        'https://newyork.craigslist.org/search/que?format=rss&query=pest+exterminator+mice+rats+roaches',
        'https://newyork.craigslist.org/search/brx?format=rss&query=pest+exterminator+mice+rats+roaches',
        # Housing posts mentioning pests
        'https://newyork.craigslist.org/search/apa?format=rss&query=mice+rats+roaches+bedbugs',
    ]
    
    new_posts = []
    seen = load_seen_leads()
    
    for feed_url in feeds:
        try:
            response = requests.get(feed_url, timeout=10)
            if response.status_code != 200:
                continue
            
            # Parse RSS/XML
            root = ET.fromstring(response.content)
            
            # Find all items
            for item in root.findall('.//item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                desc_elem = item.find('description')
                date_elem = item.find('pubDate')
                
                if title_elem is None or link_elem is None:
                    continue
                
                title = title_elem.text or ''
                link = link_elem.text or ''
                description = desc_elem.text if desc_elem is not None else ''
                
                # Extract post ID from link
                post_id_match = re.search(r'/(\d+)\.html', link)
                if not post_id_match:
                    continue
                    
                post_id = post_id_match.group(1)
                
                # Check if we've seen it
                if post_id in seen['craigslist']:
                    continue
                
                # Check if relevant to NYC pest control
                text_to_check = (title + ' ' + description).lower()
                if not any(keyword.lower() in text_to_check for keyword in KEYWORDS):
                    continue
                
                new_posts.append({
                    'id': post_id,
                    'title': title,
                    'description': description[:200],
                    'link': link,
                    'posted': date_elem.text if date_elem is not None else 'Unknown'
                })
                seen['craigslist'].append(post_id)
        
        except Exception as e:
            print(f"Error checking Craigslist feed {feed_url}: {e}")
            continue
    
    seen['craigslist'] = seen['craigslist'][-1000:]
    save_seen_leads(seen)
    
    print(f"Found {len(new_posts)} new Craigslist posts")
    return new_posts

def check_twitter():
    """Check Twitter/X via Nitter RSS for NYC pest-related posts"""
    print("Checking Twitter/X...")
    
    # Twitter accounts to monitor via Nitter (free RSS alternative)
    # These are example accounts - NYC landlords, property managers, neighborhood accounts
    accounts = [
        'NYCHousing',
        'NYCHA',
        'NYCHealthy',
        'nycgov',
    ]
    
    new_posts = []
    seen = load_seen_leads()
    
    # Use nitter.net for free RSS (Twitter alternative)
    for account in accounts:
        try:
            # Nitter RSS feed
            feed_url = f'https://nitter.net/{account}/rss'
            response = requests.get(feed_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            
            if response.status_code != 200:
                continue
            
            # Parse RSS
            root = ET.fromstring(response.content)
            
            for item in root.findall('.//item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                desc_elem = item.find('description')
                date_elem = item.find('pubDate')
                
                if title_elem is None or link_elem is None:
                    continue
                
                title = title_elem.text or ''
                link = link_elem.text or ''
                description = desc_elem.text if desc_elem is not None else ''
                
                # Extract tweet ID from link
                tweet_id_match = re.search(r'/status/(\d+)', link)
                if not tweet_id_match:
                    continue
                
                tweet_id = tweet_id_match.group(1)
                
                if tweet_id in seen['twitter']:
                    continue
                
                # Check if relevant
                text_to_check = (title + ' ' + description).lower()
                if not any(keyword.lower() in text_to_check for keyword in KEYWORDS):
                    continue
                
                # Check if NYC-related
                nyc_keywords = ['nyc', 'new york', 'brooklyn', 'queens', 'bronx', 'manhattan']
                if not any(kw in text_to_check for kw in nyc_keywords):
                    continue
                
                new_posts.append({
                    'id': tweet_id,
                    'account': account,
                    'tweet': title,
                    'link': link,
                    'posted': date_elem.text if date_elem is not None else 'Unknown'
                })
                seen['twitter'].append(tweet_id)
        
        except Exception as e:
            print(f"Error checking Twitter @{account}: {e}")
            continue
    
    seen['twitter'] = seen['twitter'][-1000:]
    save_seen_leads(seen)
    
    print(f"Found {len(new_posts)} new Twitter posts")
    return new_posts

def send_email_alert(hpd_violations, dohmh_violations, complaints_311, dob_violations, craigslist_posts, twitter_posts, reddit_posts):
    """Send email alert with new leads"""
    
    if not (hpd_violations or dohmh_violations or complaints_311 or dob_violations or craigslist_posts or twitter_posts or reddit_posts):
        print("No new leads to report")
        return
    
    # Build email content
    html_content = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            .section { margin: 20px 0; padding: 15px; border-left: 4px solid #0066ff; background: #f5f5f5; }
            .lead { margin: 10px 0; padding: 10px; background: white; border-radius: 5px; }
            .emergency { border-left: 4px solid #ff0000; }
            h2 { color: #0066ff; }
            .label { font-weight: bold; color: #333; }
            .address { font-size: 1.1em; color: #0066ff; }
        </style>
    </head>
    <body>
        <h1>🎯 New Pest Control Leads - {datetime.now().strftime('%Y-%m-%d %H:%M')}</h1>
    """.format(datetime=datetime)
    
    # HPD Violations
    if hpd_violations:
        html_content += f"""
        <div class="section">
            <h2>🏛️ HPD Housing Violations ({len(hpd_violations)} new)</h2>
        """
        for v in hpd_violations:
            emergency_class = 'emergency' if v['class'] == 'C' else ''
            html_content += f"""
            <div class="lead {emergency_class}">
                <div class="address">📍 {v['address']}</div>
                <div><span class="label">Apartment:</span> {v['apartment']}</div>
                <div><span class="label">ZIP:</span> {v['zip']}</div>
                <div><span class="label">Class:</span> {v['class']} {'⚠️ EMERGENCY' if v['class'] == 'C' else ''}</div>
                <div><span class="label">Description:</span> {v['description']}</div>
                <div><span class="label">Inspected:</span> {v['inspection_date'][:10]}</div>
                <div><span class="label">Status:</span> {v['status']}</div>
                <div style="margin-top: 10px;">
                    <a href="https://a836-acris.nyc.gov/DS/DocumentSearch/Index" style="background: #0066ff; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">Find Owner in ACRIS →</a>
                </div>
            </div>
            """
        html_content += "</div>"
    
    # DOHMH Violations
    if dohmh_violations:
        html_content += f"""
        <div class="section">
            <h2>🍽️ DOHMH Restaurant Violations ({len(dohmh_violations)} new)</h2>
        """
        for v in dohmh_violations:
            html_content += f"""
            <div class="lead">
                <div class="address">📍 {v['restaurant']}</div>
                <div><span class="label">Address:</span> {v['address']}</div>
                <div><span class="label">ZIP:</span> {v['zip']}</div>
                <div><span class="label">Phone:</span> {v['phone']}</div>
                <div><span class="label">Violation:</span> [{v['violation_code']}] {v['violation']}</div>
                <div><span class="label">Inspected:</span> {v['inspection_date'][:10]}</div>
                <div><span class="label">Grade:</span> {v['grade']}</div>
            </div>
            """
        html_content += "</div>"
    
    # 311 Complaints
    if complaints_311:
        html_content += f"""
        <div class="section">
            <h2>📞 NYC 311 Complaints ({len(complaints_311)} new)</h2>
        """
        for c in complaints_311:
            html_content += f"""
            <div class="lead">
                <div class="address">📍 {c['address']}</div>
                <div><span class="label">Type:</span> {c['type']}</div>
                <div><span class="label">Descriptor:</span> {c['descriptor']}</div>
                <div><span class="label">ZIP:</span> {c['zip']}</div>
                <div><span class="label">Created:</span> {c['created_date'][:10]}</div>
                <div><span class="label">Status:</span> {c['status']}</div>
                <div><span class="label">Agency:</span> {c['agency']}</div>
                <div style="margin-top: 10px;">
                    <a href="https://a836-acris.nyc.gov/DS/DocumentSearch/Index" style="background: #0066ff; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">Find Owner in ACRIS →</a>
                </div>
            </div>
            """
        html_content += "</div>"
    
    # DOB Violations
    if dob_violations:
        html_content += f"""
        <div class="section">
            <h2>🏗️ DOB Building Violations ({len(dob_violations)} new)</h2>
        """
        for v in dob_violations:
            html_content += f"""
            <div class="lead">
                <div class="address">📍 {v['address']}</div>
                <div><span class="label">Number:</span> {v['number']}</div>
                <div><span class="label">ZIP:</span> {v['zip']}</div>
                <div><span class="label">Type:</span> {v['violation_type']}</div>
                <div><span class="label">Category:</span> {v['category']}</div>
                <div><span class="label">Issued:</span> {v['issue_date'][:10]}</div>
                <div><span class="label">Disposition:</span> {v['disposition']}</div>
            </div>
            """
        html_content += "</div>"
    
    # Craigslist Posts
    if craigslist_posts:
        html_content += f"""
        <div class="section">
            <h2>📋 Craigslist Posts ({len(craigslist_posts)} new)</h2>
        """
        for p in craigslist_posts:
            html_content += f"""
            <div class="lead">
                <div class="address">{p['title']}</div>
                <div style="margin: 10px 0;">{p['description']}...</div>
                <div><span class="label">Posted:</span> {p['posted']}</div>
                <div>
                    <a href="{p['link']}" style="background: #6633cc; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">View Post →</a>
                </div>
            </div>
            """
        html_content += "</div>"
    
    # Twitter Posts
    if twitter_posts:
        html_content += f"""
        <div class="section">
            <h2>🐦 Twitter/X Posts ({len(twitter_posts)} new)</h2>
        """
        for p in twitter_posts:
            html_content += f"""
            <div class="lead">
                <div class="address">@{p['account']}</div>
                <div style="margin: 10px 0;">{p['tweet']}</div>
                <div><span class="label">Posted:</span> {p['posted']}</div>
                <div>
                    <a href="{p['link']}" style="background: #1DA1F2; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">View Tweet →</a>
                </div>
            </div>
            """
        html_content += "</div>"
    
    # Reddit Posts
    if reddit_posts:
        html_content += f"""
        <div class="section">
            <h2>💬 Reddit Posts ({len(reddit_posts)} new)</h2>
        """
        for p in reddit_posts:
            html_content += f"""
            <div class="lead">
                <div class="address">r/{p['subreddit']}: {p['title']}</div>
                <div><span class="label">Posted:</span> {p['created']}</div>
                <div style="margin: 10px 0;">{p['text']}...</div>
                <div>
                    <a href="{p['url']}" style="background: #ff4500; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">View Post →</a>
                </div>
            </div>
            """
        html_content += "</div>"
    
    html_content += """
    <hr>
    <p style="color: #666; font-size: 0.9em;">
        This is an automated alert from your NYC Pest Control Lead Monitor.<br>
        Respond to leads within 5 minutes for best conversion rates!
    </p>
    </body>
    </html>
    """
    
    # Send via SendGrid API
    try:
        url = "https://api.sendgrid.com/v3/mail/send"
        
        payload = {
            "personalizations": [{
                "to": [{"email": EMAIL_TO}],
                "subject": f"🎯 {len(hpd_violations) + len(dohmh_violations) + len(complaints_311) + len(dob_violations) + len(craigslist_posts) + len(twitter_posts) + len(reddit_posts)} New Leads - NYC Pest Control"
            }],
            "from": {"email": EMAIL_FROM},
            "content": [{"type": "text/html", "value": html_content}]
        }
        
        headers = {
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 202:
            print("✅ Email sent successfully!")
        else:
            print(f"❌ Email failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error sending email: {e}")

def main():
    """Main monitoring function"""
    print(f"\n{'='*60}")
    print(f"NYC Pest Control Lead Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Check all sources
    hpd_violations = check_hpd_violations()
    dohmh_violations = check_dohmh_violations()
    complaints_311 = check_311_complaints()
    dob_violations = check_dob_violations()
    craigslist_posts = check_craigslist()
    twitter_posts = check_twitter()
    reddit_posts = check_reddit()
    
    # Send alert if we found anything
    total_leads = (len(hpd_violations) + len(dohmh_violations) + len(complaints_311) + 
                   len(dob_violations) + len(craigslist_posts) + len(twitter_posts) + len(reddit_posts))
    
    print(f"\n📊 Summary: {total_leads} total new leads")
    print(f"   - HPD: {len(hpd_violations)}")
    print(f"   - DOHMH: {len(dohmh_violations)}")
    print(f"   - 311: {len(complaints_311)}")
    print(f"   - DOB: {len(dob_violations)}")
    print(f"   - Craigslist: {len(craigslist_posts)}")
    print(f"   - Twitter: {len(twitter_posts)}")
    print(f"   - Reddit: {len(reddit_posts)}")
    
    if total_leads > 0:
        send_email_alert(hpd_violations, dohmh_violations, complaints_311, dob_violations, 
                        craigslist_posts, twitter_posts, reddit_posts)
    
    print(f"\n✅ Monitoring complete!\n")

if __name__ == "__main__":
    main()
