
"""Utilities for interacting with the Pure API and resolving press clipping data."""

import logging
import configparser
import unicodedata
import json
import requests
import html

from urllib.parse import urlparse, parse_qs, quote_plus, unquote
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Tuple, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path


# Load configuration
CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
API_KEY = CONFIG["CREDENTIALS"]["APIKEY_CRUD"]
API_KEY_OLD = CONFIG["CREDENTIALS"]["APIKEY"]
GOOGLE_API = CONFIG["CREDENTIALS"]["GOOGLE_API"]
GOOGLE_CX = CONFIG["CREDENTIALS"]["GOOGLE_CX"]
BASEURL = CONFIG["CREDENTIALS"]["BASEURL"]
BASEURL_CRUD = CONFIG['CREDENTIALS']['BASEURL_CRUD']
APIKEY_CRUD = CONFIG['CREDENTIALS']['APIKEY_CRUD']
WORKFLOW_STATUS = dict(CONFIG["WORKFLOW STATUS"])
WORKFLOW_STATUS = {k.upper(): v for k, v in CONFIG["WORKFLOW STATUS"].items()}

# Session setup with retries
SESSION = requests.Session()
retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
SESSION.mount("https://", adapter)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}
SESSION = requests.Session()


SOURCE_MAP = dict(CONFIG.items("SOURCE_MAP"))


def make_free_keywords_group(keywords: list[str]) -> dict:
    return {
        "typeDiscriminator": "FreeKeywordsKeywordGroup",
        "logicalName": "keywordContainers",
        "name": {"en_GB": "Keywords"},
        "keywords": [{
            "locale": "en_GB",
            "freeKeywords": keywords
        }]
    }

def escape_pure_text(text, wrap_paragraph=False):
    """Escapes &, <, > for XHTML and normalizes for consistent comparison of titles."""
    if not isinstance(text, str):
        return text

    # Unescape to prevent double encoding
    text = html.unescape(text.strip())

    # Normalize unicode and make comparison case-insensitive
    text = unicodedata.normalize("NFKC", text).casefold()
    escaped = html.escape(text)

    return f"<p>{escaped}</p>" if wrap_paragraph else escaped





def resolve_lexisnexis_url(url, title, session, headers):
    """Resolve a LexisNexis URL to its final destination, rejecting LexisNexis results."""

    try:
        response = session.get(url, headers=headers, allow_redirects=True, timeout=10)
        final_url = response.url
        if response.ok and not ("lexisnexis.com" in final_url.lower() or "#content" in final_url):
            logging.debug(f"Direct resolution succeeded for '{title}': {final_url}")
            return final_url
        else:
            logging.debug(f"Direct resolution invalid or LexisNexis domain for '{title}': {final_url}")
    except requests.RequestException as e:
        logging.debug(f"Direct resolution failed for '{title}': {e}")

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
                    logging.debug(f"Successfully guessed URL for '{title}': {guessed_url}")
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
                    logging.debug(f"DuckDuckGo found URL for '{title}': {url}")
                    return url
    except requests.RequestException as e:
        logging.debug(f"DuckDuckGo search failed for '{title}': {e}")

    return None

def multi_engine_resolve(url, title, session, headers):
    resolved_url = resolve_lexisnexis_url(url, title, session, headers)
    if resolved_url:
        return resolved_url

    ddg_result = search_duckduckgo(title, session)
    if ddg_result:
        return ddg_result


    return None


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
                # article["URL"] = f""
                logging.debug(f"Error resolving '{article['Media item title']}': {e}")

    logging.info("Batch URL resolution complete.")



from typing import Optional, Tuple, List, Dict
from rapidfuzz import fuzz
import logging

def find_person(name: str, date: datetime, threshold: int = 95) -> Tuple[Optional[str], Optional[str], List[Dict[str, str]], str]:
    """Zoekt een persoon in Pure en geeft (employee_id, uuid, affiliaties, org_name) terug."""

    url = f"{BASEURL_CRUD}persons/search"
    payload = {"searchString": name}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api-key": API_KEY
    }

    try:
        response = SESSION.post(url, json=payload, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.debug(f"API request failed for '{name}': {e}")
        return None, None, [], ""

    data = response.json()
    items = data.get("items", [])
    if not items:
        logging.debug(f"No persons found for '{name}'")
        return None, None, [], ""

    best_match, best_score, best_name = None, 0, ""

    for item in items:
        possible_names = set()

        full_name = f"{item['name'].get('firstName', '')} {item['name'].get('lastName', '')}".strip()
        if full_name:
            possible_names.add(full_name)

        for alt in item.get("names", []):
            alt_name = f"{alt['name'].get('firstName', '')} {alt['name'].get('lastName', '')}".strip()
            if alt_name:
                possible_names.add(alt_name)

        for candidate in possible_names:
            score = fuzz.ratio(name.lower(), candidate.lower())
            logging.debug(f"Comparing '{name}' ↔ '{candidate}' = {score}%")

            if score > best_score:
                best_match, best_score, best_name = item, score, candidate

    if not best_match or best_score < threshold:
        logging.debug(f"No good match for '{name}' (best: '{best_name}' at {best_score}%)")
        return None, None, [], ""

    employee_id = None
    uuid = None
    for id_entry in best_match.get("identifiers", []):
        id_type = id_entry.get("type", {}).get("term", {}).get("en_GB", "")
        if id_type == "Employee ID":
            employee_id = id_entry.get("id")
            uuid = best_match.get("uuid")
            break

    if not employee_id:
        logging.debug(f"No Employee ID found for match '{best_name}'")
        return None, None, [], ""

    affiliations, org_name = filter_affiliations(best_match, date)
    return employee_id, uuid, affiliations, org_name




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


    url = f"{BASEURL_CRUD}organizations/{uuid}"
    headers = {"accept": "application/json", "api-key": APIKEY_CRUD}
    response = SESSION.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        type_segment = data["type"]["uri"].split("/")[-1]
        if "r" in type_segment:
            org_type = "Research organization"
        elif type_segment.startswith("003") or type_segment.startswith("004"):
            org_type = "dep/fac"
        else:
            org_type = "Organization"

        return org_type, data["name"]["en_GB"]

    return "Unknown", "Unknown"



def resolve_persons(names: List[str], date: datetime) -> Tuple[List[Tuple[str, str, str, List[Dict[str, str]]]], List[str]]:
    """Zoekt alle personen op in Pure en geeft lijst terug van (person_id, uuid, oorspronkelijke naam, affiliaties)."""

    resolved = []
    seen = set()
    errors = []

    for raw_name in names:
        name = raw_name.strip()

        person_id, uuid, affiliations, org_name = find_person(name, date)

        if person_id and affiliations:
            aff_key = tuple(sorted(frozenset(a.items()) for a in affiliations))
            dedup_key = (person_id, aff_key)

            if dedup_key not in seen:
                seen.add(dedup_key)
                resolved.append((person_id, uuid, name, affiliations))
                logging.debug(f"Resolved '{name}' → ID {person_id} ({len(affiliations)} affiliations)")
            else:
                logging.debug(f"Duplicate person+affiliation found for '{name}', skipping")
        else:
            errors.append(name)
            logging.debug(f"Failed to resolve '{name}'")

    return resolved, errors



def check_duplicates(title: str, persons: List[Tuple[str, Any]], date: datetime) -> bool:
    """Check if a press clipping already exists in Pure."""
    url = f"{BASEURL}press-media"

    searchtitle = title.replace("°", "")
    params = {"q": searchtitle, "apiKey": API_KEY_OLD}
    headers = {"accept": "application/json", "api-key": API_KEY_OLD}
    response = SESSION.get(url, params=params, headers=headers)

    if response.status_code != 200:

        return False

    data = response.json()

    for item in data.get("items", []):

        item_title = item["title"]["text"][0]["value"].lower()
        item_date = item["period"]["startDate"]
        item_date_str = item_date.split("T")[0]
        person_ids = set()
        for assoc in item.get("personAssociations", []):
            if "person" not in assoc:
                print("⚠️ 'person' ontbreekt in assoc:", assoc)
                continue
            for _id in (assoc["person"].get("externalId"), assoc["person"].get("internalId")):
                if _id:
                    person_ids.add(_id)

        escaped_input = escape_pure_text(title)
        escaped_existing = escape_pure_text(item_title)


        if (escaped_input == escaped_existing and item_date_str == date.strftime("%Y-%m-%d")) and  any(p[0] in person_ids for p in persons):

            return True

    return False
def build_payload_from_row(row):
    title = html.escape(row['Media item title'])
    url = row['URL']
    date = row['Datum'].strftime("%Y-%m-%d") if isinstance(row['Datum'], datetime) else str(row['Datum'])
    medium = row['Media name']
    persons = row['Person_resolved']
    medium_type = row.get('Medium_type', 'Web')
    media_type = row['media_type'].upper()
    typerole = row.get('typerole', 'exportcomment')
    degree = row.get('article_degree', 'national')
    role_term = row.get('researcher_role', 'interviewee')
    role_uri = f"/dk/atira/pure/clipping/roles/clipping/{role_term.lower()}"

    if row['Language'] == 'nl':
        country = 'nl'
    else:
        country = 'unknown'

    # Use first organization of first person as managing organization

    # Standaard als niemand gekoppeld is
    managing_org = {'organization-uuid': 'UNKNOWN', 'systemName': 'Organization'}

    if persons:
        orgs = persons[0][3]  # lijst van dicts met 'organization-uuid' en 'orgtype'
        org_with_orgtype = []

        for org in orgs:
            uuid = org.get("organization-uuid")
            org_type, _ = get_org_type(uuid)
            org_with_orgtype.append((org_type, org))

        # Zoek naar prioriteit: Organization > dep/fac > andere
        org = next((o for t, o in org_with_orgtype if t == "Organization"), None)
        if not org:
            org = next((o for t, o in org_with_orgtype if t == "dep/fac"), None)
        if not org:
            org = org_with_orgtype[0][1] if org_with_orgtype else None

        if org:
            managing_org = org

    coverage_persons = []
    org_set = set()
    # Voeg vrije trefwoorden toe aan payload
    keywords = [kw.strip() for kw in row.get('keywords', []) if kw.strip()]


    for pure_id, person_uuid, full_name, orgs in persons:
        first_name, *last_name_parts = full_name.split()
        last_name = ' '.join(last_name_parts) if last_name_parts else ''

        seen_orgs = set()
        org_list = []
        for o in orgs:
            key = o['organization-uuid']
            if key not in seen_orgs:
                seen_orgs.add(key)
                org_list.append({'uuid': key, 'systemName': 'Organization'})

        for o in org_list:
            org_set.add((o['uuid'], o['systemName']))
        coverage_persons.append({
            "typeDiscriminator": "InternalPressMediaPersonAssociation",
            "name": {
                "firstName": first_name,
                "lastName": last_name
            },
            "role": {
                "uri": role_uri,
                "term": {
                    "en_GB": role_term.capitalize()
                }
            },
            "person": {
                "systemName": "Person",
                "uuid": person_uuid
            },
            "organizations": org_list
        })

    payload = {
        "version": "v1",
        "title": {"en_GB": title},
        "type": {
            "uri": f"/dk/atira/pure/clipping/clippingtypes/clipping/{typerole.lower()}",
            # "term": {"en_GB": "Expert Comment"}
        },
        "visibility": {
            "key": "FREE",
            "description": {"en_GB": "Public - No restriction"}
        },
        # "workflow": {
        #     "step": "approved",
        #     "description": {"en_GB": "Approved"}
        # },
        "descriptions": [{
            "value": {"en_GB": f" "},
            "type": {
                "uri": "/dk/atira/pure/clipping/descriptions/clippingdescription",
                "term": {"en_GB": "Description"}
            }
        }],
        "managingOrganization": {
            "uuid": managing_org['organization-uuid'],
            "systemName": "Organization"
        },
        "mediaCoverages": [{
            "coverageType": media_type,
            "title": {"en_GB": title},
            "description": {"en_GB": f" "},
            "url": url,
            "medium": medium,
            "mediaType": {
                "uri": f"/dk/atira/pure/clipping/mediatype/{medium_type.lower()}",
                "term": {"en_GB": medium_type}
            },
            "degreeOfRecognition": {
                "uri": f"/dk/atira/pure/clipping/degreeofrecognition/{degree.lower()}",
                "term": {"en_GB": degree.capitalize()}
            },
            "authorProducer": "",
            "durationLengthSize": "",
            "date": date,
            "country": {
                "uri": f"/dk/atira/pure/core/countries/{country}",
                # "term": {"en_GB": "Netherlands"}
            },
            "persons": coverage_persons,
            "organizations": [{"uuid": uuid, "systemName": system_name} for uuid, system_name in org_set]
        }]
    }

    payload["workflow"] = build_workflow(row['Faculty'])
    # standaard classification keywordgroup
    CLASSIFICATION_GROUP = {
        "typeDiscriminator": "ClassificationsKeywordGroup",

        "logicalName": "/dk/atira/pure/clippings/keywords/imported",
        "name": {"en_GB": "Imported by media import tool"},
        "classifications": [{
            "uri": "/dk/atira/pure/clippings/keywords/imported/true",
            "term": {"en_GB": "true"}
        }]
    }
    payload["keywordGroups"] = [CLASSIFICATION_GROUP]

    if keywords:
        payload["keywordGroups"].append(make_free_keywords_group(keywords))

    return payload

def build_workflow(faculty: str) -> dict:
    """
    Bouwt workflow sectie op basis van faculteit uit config.ini
    """
    # lookup uit config (default = "approved" als faculteit niet gevonden)
    print(faculty)

    status = WORKFLOW_STATUS.get(faculty, "approved")

    return {
        "step": status,
        "description": {"en_GB": status}
    }


def upload_processed_articles(processed_articles, api_key, api_url_base):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api-key": api_key
    }
    succes = 0
    fail = 0
    for row in processed_articles:
        payload = build_payload_from_row(row)
        response = requests.put(f"{api_url_base}/pressmedia", headers=headers, data=json.dumps(payload))

        if response.status_code in (200, 201):
            location = response.headers.get("Location", "N/A")
            logging.info(f"✓ Created at: {location}")
            succes += 1
        else:
            logging.warning(f"✗ Error: {response.text}")
            fail += 1
    logging.info(f"Uploaded articles: {succes}")

def get_media_item(uuid):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api-key": APIKEY_CRUD
    }
    response = requests.get(f"{BASEURL_CRUD}/pressmedia/{uuid}", headers=headers)
