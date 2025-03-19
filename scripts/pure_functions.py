#!/usr/bin/env python3
"""Utilities for interacting with the Pure API and resolving press clipping data."""
from rapidfuzz import fuzz
from datetime import datetime
from typing import List, Tuple, Dict, Any
import configparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import logging
import requests
from urllib.parse import quote_plus, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Load configuration
CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
API_KEY = CONFIG["CREDENTIALS"]["APIKEY_CRUD"]
GOOGLE_API = CONFIG["CREDENTIALS"]["GOOGLE_API"]
GOOGLE_CX = CONFIG["CREDENTIALS"]["GOOGLE_CX"]
BASEURL = CONFIG["CREDENTIALS"]["BASEURL"]
# Session setup with retries
SESSION = requests.Session()
retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
SESSION.mount("https://", adapter)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}


import logging
import requests
from urllib.parse import quote_plus, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor

# HTTP session and headers
SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
FALLBACK_ORG_UUID = 'cdd6493c-70ab-40f8-8246-b8be95f27e71'
# Known source mappings
import requests
import logging
from urllib.parse import urlparse, parse_qs, quote_plus, unquote
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

SOURCE_MAP = {
    "fd.nl": "https://fd.nl",
    "volkskrant.nl": "https://www.volkskrant.nl",
    "ad.nl": "https://www.ad.nl",
    "tech daily news": "https://www.techdailynews.com",
    "nature geoscience": "https://www.nature.com/ngeo",
    "ncbi": "https://www.ncbi.nlm.nih.gov",
    "open universiteit": "https://www.ou.nl",
    "nscr": "https://nscr.nl",
    "movisie.nl": "https://www.movisie.nl",
    "university world news": "https://www.universityworldnews.com",
    "annekeschrijft.com": "https://annekeschrijft.com",
    "po-raad.nl": "https://www.poraad.nl",
    # Add other known mappings as needed
}

def resolve_lexisnexis_url(url, title, session, headers):
    """Resolve a LexisNexis URL to its final destination, rejecting LexisNexis results."""

    try:
        response = session.get(url, headers=headers, allow_redirects=True, timeout=10)
        final_url = response.url
        if response.ok and not ("lexisnexis.com" in final_url.lower() or "#content" in final_url):
            logging.info(f"Direct resolution succeeded for '{title}': {final_url}")
            return final_url
        else:
            logging.warning(f"Direct resolution invalid or LexisNexis domain for '{title}': {final_url}")
    except requests.RequestException as e:
        logging.warning(f"Direct resolution failed for '{title}': {e}")

    parsed_url = urlparse(url)
    params = parse_qs(parsed_url.query)
    source = params.get("e", [""])[0].lower().replace("+", " ")

    for key, base_url in SOURCE_MAP.items():
        if key in source:
            guessed_slug = quote_plus(title.lower().replace(" ", "-"))
            guessed_url = f"{base_url}/{guessed_slug}"
            try:
                head_resp = session.head(guessed_url, headers=headers, allow_redirects=True, timeout=5)
                if head_resp.ok:
                    logging.info(f"Successfully guessed URL for '{title}': {guessed_url}")
                    return guessed_url
            except requests.RequestException:
                continue

    return None

def search_duckduckgo(title, session):
    query = quote_plus(title)
    search_url = f"https://duckduckgo.com/html/?q={query}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"}

    try:
        response = session.get(search_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.find_all('a', class_='result__a', href=True):
            raw_url = link['href']
            parsed_link = urlparse(raw_url)
            qs = parse_qs(parsed_link.query)
            if 'uddg' in qs:
                url = unquote(qs['uddg'][0])
                if "lexisnexis.com" not in url.lower():
                    logging.info(f"DuckDuckGo found URL for '{title}': {url}")
                    return url
    except requests.RequestException as e:
        logging.warning(f"DuckDuckGo search failed for '{title}': {e}")

    return None

def multi_engine_resolve(url, title, session, headers):
    resolved_url = resolve_lexisnexis_url(url, title, session, headers)
    if resolved_url:
        return resolved_url

    ddg_result = search_duckduckgo(title, session)
    if ddg_result:
        return ddg_result

    fallback_url = f"https://www.google.com/search?q={quote_plus(title)}"
    logging.info(f"Fallback Google search for '{title}': {fallback_url}")
    return fallback_url

def batch_resolve_urls(articles, session, headers, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_article = {
            executor.submit(
                multi_engine_resolve,
                article.get("URL", ""),
                article["Media item title"],
                session,
                headers
            ): article for article in articles if article.get("URL", "").strip()
        }

        for future in future_to_article:
            article = future_to_article[future]
            try:
                resolved_url = future.result()
                article["URL"] = resolved_url
            except Exception as e:
                article["URL"] = f"https://www.google.com/search?q={quote_plus(article['Media item title'])}"
                logging.error(f"Error resolving '{article['Media item title']}': {e}")

    logging.info("Batch URL resolution complete.")



def find_person(name: str, date: datetime, threshold: int = 95) -> Tuple[str | None, List[Dict[str, str]], str]:
    url = "https://staging.research-portal.uu.nl/ws/api/persons/search/"
    payload = {"searchString": name}
    headers = {"Content-Type": "application/json", "accept": "application/json", "api-key": API_KEY}

    response = SESSION.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        logging.warning(f"API request failed for '{name}': {response.status_code}")
        return None, [], ""

    data = response.json()
    if not data.get("items"):
        logging.info(f"No persons found for '{name}'")
        return None, [], ""

    best_match = None
    best_score = 0
    best_name = ""

    for item in data["items"]:
        # Extract all possible names
        possible_names = set()

        # Primary name
        full_name = f"{item['name'].get('firstName', '')} {item['name'].get('lastName', '')}".strip()
        if full_name:
            possible_names.add(full_name)

        # Alternative names (e.g., "Known as name")
        for name_entry in item.get("names", []):
            alt_name = f"{name_entry['name'].get('firstName', '')} {name_entry['name'].get('lastName', '')}".strip()
            if alt_name:
                possible_names.add(alt_name)

        # Compare each possible name
        for candidate_name in possible_names:
            similarity = fuzz.ratio(name.lower(), candidate_name.lower())
            logging.debug(f"Comparing '{name}' with '{candidate_name}' - Similarity: {similarity}%")

            if similarity > best_score:
                best_match = item
                best_score = similarity
                best_name = candidate_name

    if best_match is None or best_score < threshold:
        logging.info(f"Best match for '{name}' is '{best_name}' with {best_score}% similarity (below threshold).")
        return None, [], ""

    if best_score == 100:
        logging.info(f"Exact match found for '{name}' -> '{best_name}'")
    elif best_score >= threshold:
        logging.info(f"Match above threshold ({threshold}%) for '{name}' -> '{best_name}' ({best_score}%)")

    employee_id = None
    for id_entry in best_match.get("identifiers", []):
        id_type = id_entry.get("type", {}).get("term", {}).get("en_GB", "")
        if id_type == "Employee ID":
            employee_id = id_entry.get("id")
            break
    else:
        logging.info(f"No Employee ID found for '{best_name}'")

    affiliations, org_name = filter_affiliations(best_match, date) if employee_id else ([], "")
    return employee_id, affiliations, org_name


def filter_affiliations(data: Dict[str, Any], date: datetime) -> Tuple[List[Dict[str, str]], str]:
    """Filter a person's affiliations by date."""
    affiliations = []
    org_name = ""
    for org in data.get("staffOrganizationAssociations", []):
        start_date = datetime.strptime(org["period"]["startDate"], "%Y-%m-%d")
        end_date = datetime.strptime(org["period"].get("endDate", "9999-12-31"), "%Y-%m-%d")
        if start_date <= date <= end_date:
            uuid = org["organization"]["uuid"]
            org_type, org_name = get_org_type(uuid)

            affiliations.append({"organization-uuid": uuid, "orgtype": org_type, "orgname": org_name})
    return affiliations, org_name


def get_org_type(uuid: str) -> Tuple[str, str]:
    """Determine the type and name of an organization by UUID."""
    url = f"https://staging.research-portal.uu.nl/ws/api/organizations/{uuid}"
    headers = {"accept": "application/json", "api-key": API_KEY}
    response = SESSION.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        org_type = "Research organization" if "r" in data["type"]["uri"].split("/")[-1] else "Organization"
        return org_type, data["name"]["en_GB"]
    return "Unknown", "Unknown"


def resolve_persons(names: List[str], date: datetime) -> Tuple[List[Tuple[str, List[Dict[str, str]]]], List[str]]:
    """Resolve multiple names to Pure person IDs and affiliations."""
    resolved = []
    seen = set()  # To track unique (person_id, tuple(affiliations)) pairs
    errors = []

    for name in names:
        person_id, affiliations, org_name = find_person(name.strip(), date)
        if person_id and affiliations:
            # Convert affiliations to a hashable type for deduplication
            aff_tuple = tuple(sorted(frozenset(d.items()) for d in affiliations))
            if (person_id, aff_tuple) not in seen:
                seen.add((person_id, aff_tuple))
                resolved.append((person_id, name, affiliations))
                logging.info(f"Resolved '{name}' to ID {person_id}")
        else:
            errors.append(name)
            logging.warning(f"Failed to resolve '{name}'")

    return resolved, errors


def check_duplicates(title: str, persons: List[Tuple[str, Any]], date: datetime) -> bool:
    """Check if a press clipping already exists in Pure."""
    url = BASEURL + 'press-media'
    # url = "https://research-portal.uu.nl/ws/api/524/press-media"
    params = {"q": title, "apiKey": API_KEY}
    headers = {"accept": "application/json", "api-key": API_KEY}
    response = SESSION.get(url, params=params, headers=headers)

    if response.status_code != 200:
        return False

    data = response.json()
    for item in data.get("items", []):
        item_title = item["title"]["text"][0]["value"].lower()
        item_date = item["period"]["startDate"]
        person_ids = {assoc["person"]["externalId"] for assoc in item.get("personAssociations", [])}
        if (item_title == title.lower() and item_date == date.strftime("%Y-%m-%d") and
                any(p[0] in person_ids for p in persons)):
            return True
    return False


