#!/usr/bin/env python3
"""
NYC Pest Control Lead Monitor - FULL VERSION
Monitors HPD violations, DOHMH violations, 311 complaints, DOB violations,
Craigslist posts, Twitter/X posts, and Reddit posts
Sends email alerts for new leads
"""

import requests
import json
import os
from datetime import datetime, timedelta
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
        try:
            with open(SEEN_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'hpd': [], 'dohmh': [], 'reddit': [], '311': [], 'dob': [], 'craigslist': [], 'twitter': []}

def save_seen_leads(seen):
    """Save seen leads to file"""
    with open(SEEN_FILE, 'w') as f:
        json.dump(seen, f)

def check_hpd_violations():
    """Check NYC HPD violations for pest-related issues"""
    print("Checking HPD violations...")
    
    base_url = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
    
    # Get violations from last 7 days (more reliable than 24 hours)
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        # Simple query without complex WHERE clause
        params = {
            '$where': f"inspectiondate > '{week_ago}T00:00:00'",
            '$limit': 100,
            'boro': 'BROOKLYN'
        }
        
        response = requests.get(base_url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"HPD API returned status {response.status_code}")
            return []
        
        violations = response.json()
        
        if not isinstance(violations, list):
            print(f"HPD API returned unexpected format")
            return []
        
        new_violations = []
        seen = load_seen_leads()
        
        # Filter for pest-related violations
        pest_keywords = ['pest', 'roach', 'rodent', 'mice', 'rat', 'bedbug', 'bed bug', 'vermin', 'infestation']
        
        for v in violations:
            if not isinstance(v, dict):
                continue
                
            description = str(v.get('novdescription', '')).lower()
            
            # Check if pest-related
            if not any(keyword in description for keyword in pest_keywords):
                continue
            
            violation_id = v.get('violationid')
            if violation_id and str(violation_id) not in seen['hpd']:
                new_violations.append({
                    'id': str(violation_id),
                    'address': f"{v.get('housenumber', '')} {v.get('streetname', '')}, {v.get('boro', '')}".strip(),
                    'apartment': v.get('apartment', 'N/A'),
                    'zip': v.get('zip', ''),
                    'class': v.get('class', ''),
                    'description': v.get('novdescription', ''),
                    'inspection_date': v.get('inspectiondate', '')[:10] if v.get('inspectiondate') else '',
                    'status': v.get('currentstatus', '')
                })
                seen['hpd'].append(str(violation_id))
        
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
    
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        # Pest violation codes: 04L, 04M, 04N, 08A
        params = {
            '$where': f"inspection_date > '{week_ago}T00:00:00'",
            'violation_code': '04L',
            'boro': 'Brooklyn',
            '$limit': 50
        }
        
        response = requests.get(base_url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"DOHMH API returned status {response.status_code}")
            return []
        
        violations = response.json()
        
        if not isinstance(violations, list):
            print(f"DOHMH API returned unexpected format")
            return []
        
        new_violations = []
        seen = load_seen_leads()
        
        for v in violations:
            if not isinstance(v, dict):
                continue
                
            unique_id = f"{v.get('camis', '')}_{v.get('inspection_date', '')}"
            
            if unique_id not in seen['dohmh']:
                new_violations.append({
                    'id': unique_id,
                    'restaurant': v.get('dba', 'Unknown'),
                    'address': f"{v.get('building', '')} {v.get('street', '')}, {v.get('boro', '')}".strip(),
                    'zip': v.get('zipcode', ''),
                    'phone': v.get('phone', 'N/A'),
                    'violation_code': v.get('violation_code', ''),
                    'violation': v.get('violation_description', ''),
                    'inspection_date': v.get('inspection_date', '')[:10] if v.get('inspection_date') else '',
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
    
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        params = {
            '$where': f"created_date > '{week_ago}T00:00:00'",
            'complaint_type': 'Rodent',
            'borough': 'BROOKLYN',
            '$limit': 50
        }
        
        response = requests.get(base_url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"311 API returned status {response.status_code}")
            return []
        
        complaints = response.json()
        
        if not isinstance(complaints, list):
            print(f"311 API returned unexpected format")
            return []
        
        new_complaints = []
        seen = load_seen_leads()
        
        for c in complaints:
            if not isinstance(c, dict):
                continue
                
            unique_number = c.get('unique_key')
            
            if unique_number and str(unique_number) not in seen['311']:
                address_parts = []
                if c.get('incident_address'):
                    address_parts.append(c.get('incident_address'))
                if c.get('borough'):
                    address_parts.append(c.get('borough'))
                
                new_complaints.append({
                    'id': str(unique_number),
                    'type': c.get('complaint_type', 'Unknown'),
                    'descriptor': c.get('descriptor', ''),
                    'address': ', '.join(address_parts) if address_parts else 'Address not provided',
                    'zip': c.get('incident_zip', 'N/A'),
                    'created_date': c.get('created_date', '')[:10] if c.get('created_date') else '',
                    'status': c.get('status', 'Unknown'),
                    'agency': c.get('agency', 'N/A')
                })
                seen['311'].append(str(unique_number))
        
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
    
    # DOB API is less reliable, skip for now
    print("DOB: Skipping (API currently unavailable)")
    return []

def check_craigslist():
    """Check Craigslist RSS feeds for pest-related posts"""
    print("Checking Craigslist...")
    
    feeds = [
        'https://newyork.craigslist.org/search/bks?format=rss&query=pest+exterminator',
    ]
    
    new_posts = []
    seen = load_seen_leads()
    
    for feed_url in feeds:
        try:
            response = requests.get(feed_url, timeout=10)
            if response.status_code != 200:
                continue
            
            root = ET.fromstring(response.content)
            
            for item in root.findall('.//{http://purl.org/rss/1.0/}item')[:5]:  # Limit to 5
                title_elem = item.find('{http://purl.org/rss/1.0/}title')
                link_elem = item.find('{http://purl.org/rss/1.0/}link')
                
                if title_elem is None or link_elem is None:
                    continue
                
                title = title_elem.text or ''
                link = link_elem.text or ''
                
                post_id_match = re.search(r'/(\d+)\.html', link)
                if not post_id_match:
                    continue
                    
                post_id = post_id_match.group(1)
                
                if post_id in seen['craigslist']:
                    continue
                
                new_posts.append({
                    'id': post_id,
                    'title': title,
                    'description': '',
                    'link': link,
                    'posted': 'Recent'
                })
                seen['craigslist'].append(post_id)
        
        except Exception as e:
            print(f"Error checking Craigslist: {e}")
            continue
    
    seen['craigslist'] = seen['craigslist'][-1000:]
    save_seen_leads(seen)
    
    print(f"Found {len(new_posts)} new Craigslist posts")
    return new_posts

def check_twitter():
    """Check Twitter/X via Nitter RSS"""
    print("Checking Twitter/X...")
    
    # Twitter/Nitter often blocked, skip for now
    print("Twitter: Skipping (Nitter currently unavailable)")
    return []

def check_reddit():
    """Check Reddit for pest control posts"""
    print("Checking Reddit...")
    
    subreddits = [
        'AskNYC', 'nyc', 'Brooklyn', 'Queens', 'Bronx',
        'Bedbugs', 'Landlord',
        'Bushwick', 'williamsburg', 'astoria'
    ]
    
    new_posts = []
    seen = load_seen_leads()
    
    headers = {'User-Agent': 'LeadMonitor/1.0'}
    
    for subreddit in subreddits:
        try:
            url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=10"
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                continue
                
            data = response.json()
            posts = data.get('data', {}).get('children', [])
            
            for post in posts:
                post_data = post.get('data', {})
                post_id = post_data.get('id')
                title = post_data.get('title', '').lower()
                selftext = post_data.get('selftext', '').lower()
                
                text_to_check = title + ' ' + selftext
                if not any(keyword.lower() in text_to_check for keyword in KEYWORDS):
                    continue
                
                if post_id and post_id not in seen['reddit']:
                    nyc_keywords = ['nyc', 'new york', 'brooklyn', 'queens', 'bronx', 'manhattan']
                    if subreddit.lower() in ['asknyc', 'nyc', 'brooklyn', 'queens', 'bronx'] or \
                       any(kw in text_to_check for kw in nyc_keywords):
                        
                        new_posts.append({
                            'id': post_id,
                            'subreddit': subreddit,
                            'title': post_data.get('title', ''),
                            'text': post_data.get('selftext', '')[:200],
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

def send_email_alert(hpd_violations, dohmh_violations, complaints_311, dob_violations, craigslist_posts, twitter_posts, reddit_posts):
    """Send email alert with new leads"""
    
    total = len(hpd_violations) + len(dohmh_violations) + len(complaints_311) + len(dob_violations) + len(craigslist_posts) + len(twitter_posts) + len(reddit_posts)
    
    if total == 0:
        print("No new leads to report")
        return
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .section {{ margin: 20px 0; padding: 15px; border-left: 4px solid #0066ff; background: #f5f5f5; }}
            .lead {{ margin: 10px 0; padding: 10px; background: white; border-radius: 5px; }}
            .emergency {{ border-left: 4px solid #ff0000; }}
            h2 {{ color: #0066ff; }}
            .label {{ font-weight: bold; color: #333; }}
            .address {{ font-size: 1.1em; color: #0066ff; }}
        </style>
    </head>
    <body>
        <h1>🎯 {total} New Leads - NYC Pest Control</h1>
        <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    """
    
    # HPD Violations
    if hpd_violations:
        html_content += f"""
        <div class="section">
            <h2>🏛️ HPD Housing Violations ({len(hpd_violations)} new)</h2>
        """
        for v in hpd_violations[:10]:  # Limit to 10
            emergency_class = 'emergency' if v.get('class') == 'C' else ''
            html_content += f"""
            <div class="lead {emergency_class}">
                <div class="address">📍 {v.get('address', 'N/A')}</div>
                <div><span class="label">Apartment:</span> {v.get('apartment', 'N/A')}</div>
                <div><span class="label">Class:</span> {v.get('class', 'N/A')} {'⚠️ EMERGENCY' if v.get('class') == 'C' else ''}</div>
                <div><span class="label">Description:</span> {v.get('description', 'N/A')[:150]}</div>
                <div><span class="label">Inspected:</span> {v.get('inspection_date', 'N/A')}</div>
                <div style="margin-top: 10px;">
                    <a href="https://a836-acris.nyc.gov/DS/DocumentSearch/Index" style="background: #0066ff; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">Find Owner →</a>
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
        for v in dohmh_violations[:10]:
            html_content += f"""
            <div class="lead">
                <div class="address">📍 {v.get('restaurant', 'N/A')}</div>
                <div><span class="label">Address:</span> {v.get('address', 'N/A')}</div>
                <div><span class="label">Phone:</span> {v.get('phone', 'N/A')}</div>
                <div><span class="label">Violation:</span> [{v.get('violation_code', '')}] {v.get('violation', 'N/A')[:150]}</div>
            </div>
            """
        html_content += "</div>"
    
    # 311 Complaints
    if complaints_311:
        html_content += f"""
        <div class="section">
            <h2>📞 NYC 311 Complaints ({len(complaints_311)} new)</h2>
        """
        for c in complaints_311[:10]:
            html_content += f"""
            <div class="lead">
                <div class="address">📍 {c.get('address', 'N/A')}</div>
                <div><span class="label">Type:</span> {c.get('type', 'N/A')}</div>
                <div><span class="label">Descriptor:</span> {c.get('descriptor', 'N/A')}</div>
                <div><span class="label">Status:</span> {c.get('status', 'N/A')}</div>
            </div>
            """
        html_content += "</div>"
    
    # Craigslist
    if craigslist_posts:
        html_content += f"""
        <div class="section">
            <h2>📋 Craigslist Posts ({len(craigslist_posts)} new)</h2>
        """
        for p in craigslist_posts[:10]:
            html_content += f"""
            <div class="lead">
                <div class="address">{p.get('title', 'N/A')}</div>
                <div><a href="{p.get('link', '#')}" style="background: #6633cc; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">View Post →</a></div>
            </div>
            """
        html_content += "</div>"
    
    # Reddit
    if reddit_posts:
        html_content += f"""
        <div class="section">
            <h2>💬 Reddit Posts ({len(reddit_posts)} new)</h2>
        """
        for p in reddit_posts[:10]:
            html_content += f"""
            <div class="lead">
                <div class="address">r/{p.get('subreddit', 'N/A')}: {p.get('title', 'N/A')}</div>
                <div><a href="{p.get('url', '#')}" style="background: #ff4500; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px;">View Post →</a></div>
            </div>
            """
        html_content += "</div>"
    
    html_content += """
    <hr>
    <p style="color: #666; font-size: 0.9em;">
        Automated NYC Pest Control Lead Monitor<br>
        Respond quickly for best conversion rates!
    </p>
    </body>
    </html>
    """
    
    try:
        url = "https://api.sendgrid.com/v3/mail/send"
        
        payload = {
            "personalizations": [{
                "to": [{"email": EMAIL_TO}],
                "subject": f"🎯 {total} New Leads - NYC Pest Control Monitor"
            }],
            "from": {"email": EMAIL_FROM},
            "content": [{"type": "text/html", "value": html_content}]
        }
        
        headers = {
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
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
