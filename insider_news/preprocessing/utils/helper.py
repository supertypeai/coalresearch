from datetime import datetime, timedelta

from scrapper.esdm_minerba import COMMODITY_MAP

import re 


def extract_commodities(title: str, body: str) -> list[str]:
        text_to_search = f"{title} {body}".lower()
        found_commodities = []

        for commodity_key, commodity_value in COMMODITY_MAP.items():
            if re.search(rf'\b{re.escape(commodity_key.lower())}\b', text_to_search):
                found_commodities.append(commodity_value)

        return found_commodities


def run_extract_commodities(title: str, body: str, full_body: str = None) -> list[str]:
    text = f"{title} {body}".lower()
    result = any(key.lower() in text or value.lower() in text for key, value in COMMODITY_MAP.items())
    
    quick_matches = extract_commodities(title, body)

    if not quick_matches and full_body and result:
        return extract_commodities(title, full_body)
    
    return quick_matches


def manual_scoring_time(date: str): 
    if isinstance(date, str):
        publication_timestamp = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

    current_time = datetime.now() 

    # scoring manual for timestamp 
    time_difference = current_time - publication_timestamp 

    # Score 5: Very recent (published within the last 48 hours)
    if time_difference <= timedelta(hours=48):
        return 5

    # Score 3: Recent (published within the last week)
    elif time_difference <= timedelta(days=7):
        return 3 

    # Score 2: Somewhat recent (published within the last 2 weeks)
    elif time_difference <= timedelta(days=14):
        return 2 

    # Score 1: Outdated 
    else:
        return 1
        
def dedup_comodities(commodities: list) -> list:
    seen = set()
    update = []

    for commodity in commodities:
        if commodity not in seen:
            update.append(commodity)
            seen.add(commodity)
    
    return update 