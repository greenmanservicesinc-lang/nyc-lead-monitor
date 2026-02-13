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
    
    # Get violations from last 7 days
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Check all boroughs
    boroughs = ['BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN']
    all_violations = []
    
    for borough in boroughs:
        try:
            params = {
                '$where': f"inspectiondate > '{week_ago}T00:00:00'",
                '$limit': 50,
                'boro': borough
            }
            
            response = requests.get(base_url, params=params, timeout=30)
