"""Pure API interaction: person lookup, duplicate checking, payload building, and upload."""

import logging
import configparser
import unicodedata
import json
import requests
import html
from functools import lru_cache
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any
from rapidfuzz import fuzz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

logger = logging.getLogger(__name__)

# --- Configuration -----------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

API_KEY = CONFIG["CREDENTIALS"]["APIKEY_CRUD"]
API_KEY_OLD = CONFIG["CREDENTIALS"]["APIKEY"]
BASEURL = CONFIG["CREDENTIALS"]["BASEURL"]
BASEURL_CRUD = CONFIG["CREDENTIALS"]["BASEURL_CRUD"]
APIKEY_CRUD = CONFIG["CREDENTIALS"]["APIKEY_CRUD"]
WORKFLOW_STATUS = {k.upper(): v for k, v in CONFIG["WORKFLOW STATUS"].items()}
UPLOAD_TIMEOUT = (5, 30)

LANGUAGE_TO_COUNTRY = {
    "nl": "nl",
}

# --- HTTP session for Pure API calls -----------------------------------------

SESSION = requests.Session()
_retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
_adapter = HTTPAdapter(max_retries=_retry)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# --- Utilities ---------------------------------------------------------------

def escape_pure_text(text: str, wrap_paragraph: bool = False) -> str:
    """Normalise and HTML-escape text for consistent Pure comparison."""
    if not isinstance(text, str):
        return text
    text = html.unescape(text.strip())
    text = unicodedata.normalize("NFKC", text).casefold()
    escaped = html.escape(text)
    return f"<p>{escaped}</p>" if wrap_paragraph else escaped


def make_free_keywords_group(keywords: list[str]) -> dict:
    return {
        "typeDiscriminator": "FreeKeywordsKeywordGroup",
        "logicalName": "keywordContainers",
        "name": {"en_GB": "Keywords"},
        "keywords": [{"locale": "en_GB", "freeKeywords": keywords}],
    }

# --- Person lookup -----------------------------------------------------------

@lru_cache(maxsize=512)
def get_org_type(uuid: str) -> Tuple[str, str]:
    """Return (org_type, org_name) for a Pure organisation UUID. Cached per UUID."""
    url = f"{BASEURL_CRUD}organizations/{uuid}"
    headers = {"accept": "application/json", "api-key": APIKEY_CRUD}
    response = SESSION.get(url, headers=headers)

    if response.status_code != 200:
        return "Unknown", "Unknown"

    data = response.json()
    type_segment = data["type"]["uri"].split("/")[-1]
    if "r" in type_segment:
        org_type = "Research organization"
    elif type_segment.startswith("003") or type_segment.startswith("004"):
        org_type = "dep/fac"
    else:
        org_type = "Organization"
    return org_type, data["name"]["en_GB"]


def filter_affiliations(data: Dict[str, Any], date: datetime) -> Tuple[List[Dict[str, str]], str]:
    """Return affiliations active on the article date, plus the last matched org name."""
    affiliations = []
    org_name = ""
    for org in data.get("staffOrganizationAssociations", []):
        start = datetime.strptime(org["period"]["startDate"], "%Y-%m-%d")
        end = datetime.strptime(org["period"].get("endDate", "9999-12-31"), "%Y-%m-%d")
        if start <= date <= end:
            uuid = org["organization"]["uuid"]
            org_type, org_name = get_org_type(uuid)
            affiliations.append({"organization-uuid": uuid, "orgtype": org_type, "orgname": org_name})
    return affiliations, org_name


def find_person(
    name: str, date: datetime, threshold: int = 95
) -> Tuple[Optional[str], Optional[str], List[Dict[str, str]], str]:
    """Search Pure for a person by name and return (employee_id, uuid, affiliations, org_name)."""
    url = f"{BASEURL_CRUD}persons/search"
    headers = {"Content-Type": "application/json", "Accept": "application/json", "api-key": API_KEY}

    try:
        response = SESSION.post(url, json={"searchString": name}, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.debug(f"Person API request failed for '{name}': {e}")
        return None, None, [], ""

    items = response.json().get("items", [])
    if not items:
        logger.debug(f"No persons found for '{name}'")
        return None, None, [], ""

    best_match, best_score, best_name = None, 0, ""
    for item in items:
        candidates = {
            f"{item['name'].get('firstName', '')} {item['name'].get('lastName', '')}".strip()
        }
        for alt in item.get("names", []):
            candidates.add(f"{alt['name'].get('firstName', '')} {alt['name'].get('lastName', '')}".strip())

        for candidate in filter(None, candidates):
            score = fuzz.ratio(name.lower(), candidate.lower())
            logger.debug(f"Comparing '{name}' ↔ '{candidate}' = {score}%")
            if score > best_score:
                best_match, best_score, best_name = item, score, candidate

    if not best_match or best_score < threshold:
        logger.debug(f"No match for '{name}' (best: '{best_name}' at {best_score}%)")
        return None, None, [], ""

    employee_id = uuid = None
    for id_entry in best_match.get("identifiers", []):
        if id_entry.get("type", {}).get("term", {}).get("en_GB") == "Employee ID":
            employee_id = id_entry.get("id")
            uuid = best_match.get("uuid")
            break

    if not employee_id:
        logger.debug(f"No Employee ID for matched person '{best_name}'")
        return None, None, [], ""

    affiliations, org_name = filter_affiliations(best_match, date)
    return employee_id, uuid, affiliations, org_name


def resolve_persons(
    names: List[str], date: datetime
) -> Tuple[List[Tuple[str, str, str, List[Dict[str, str]]]], List[str]]:
    """Resolve all names against Pure and return (resolved, errors).

    resolved: list of (employee_id, uuid, original_name, affiliations)
    errors:   names that could not be matched
    """
    resolved, errors, seen = [], [], set()

    for raw_name in names:
        name = raw_name.strip()
        person_id, uuid, affiliations, _ = find_person(name, date)

        if person_id and affiliations:
            aff_key = tuple(sorted(frozenset(a.items()) for a in affiliations))
            dedup_key = (person_id, aff_key)
            if dedup_key not in seen:
                seen.add(dedup_key)
                resolved.append((person_id, uuid, name, affiliations))
                logger.debug(f"Resolved '{name}' → {person_id} ({len(affiliations)} affiliations)")
            else:
                logger.debug(f"Duplicate person+affiliation for '{name}', skipping")
        else:
            errors.append(name)
            logger.debug(f"Could not resolve '{name}'")

    return resolved, errors

# --- Duplicate detection -----------------------------------------------------

def check_duplicates(title: str, persons: List[Tuple[str, Any]], date: datetime) -> bool:
    """Return True if a matching press clipping already exists in Pure."""
    params = {"q": title.replace("°", ""), "apiKey": API_KEY_OLD}
    headers = {"accept": "application/json", "api-key": API_KEY_OLD}
    try:
        response = SESSION.get(
            f"{BASEURL}press-media",
            params=params,
            headers=headers,
            timeout=UPLOAD_TIMEOUT,
        )
    except requests.RequestException as e:
        logger.warning(f"Duplicate check request failed for '{title}': {e}")
        return False

    if response.status_code != 200:
        return False

    escaped_input = escape_pure_text(title)
    for item in response.json().get("items", []):
        item_title = (
            item.get("title", {})
            .get("text", [{}])[0]
            .get("value", "")
        )
        item_period_start = item.get("period", {}).get("startDate", "")
        if not item_title or not item_period_start:
            logger.debug(f"Skipping duplicate candidate with incomplete fields for '{title}'")
            continue
        item_date_str = item_period_start.split("T")[0]

        person_ids = set()
        for assoc in item.get("personAssociations", []):
            if "person" not in assoc:
                logger.warning(f"Missing 'person' key in association for '{title}': {assoc}")
                continue
            for _id in (assoc["person"].get("externalId"), assoc["person"].get("internalId")):
                if _id:
                    person_ids.add(_id)

        if (
            escape_pure_text(item_title) == escaped_input
            and item_date_str == date.strftime("%Y-%m-%d")
            and any(p[0] in person_ids for p in persons)
        ):
            return True

    return False

# --- Payload building --------------------------------------------------------

CLASSIFICATION_GROUP = {
    "typeDiscriminator": "ClassificationsKeywordGroup",
    "logicalName": "/dk/atira/pure/clippings/keywords/imported",
    "name": {"en_GB": "Imported by media import tool"},
    "classifications": [{"uri": "/dk/atira/pure/clippings/keywords/imported/true", "term": {"en_GB": "true"}}],
}


def build_workflow(faculty: str) -> dict:
    """Return a Pure workflow dict based on faculty config."""
    status = WORKFLOW_STATUS.get(faculty, "approved")
    logger.debug(f"Workflow for faculty '{faculty}': {status}")
    return {"step": status, "description": {"en_GB": status}}


def build_payload_from_row(row: dict) -> dict:
    """Construct the Pure press media JSON payload from a processed article row."""
    title = html.escape(row["Media item title"])
    url = row["URL"]
    date = row["Datum"].strftime("%Y-%m-%d") if isinstance(row["Datum"], datetime) else str(row["Datum"])
    persons = row["Person_resolved"]
    medium_type = row.get("Medium_type", "Web")
    media_type = row["media_type"].upper()
    typerole = row.get("typerole", "exportcomment")
    degree = row.get("article_degree", "national")
    role_term = row.get("researcher_role", "interviewee")
    role_uri = f"/dk/atira/pure/clipping/roles/clipping/{role_term.lower()}"
    country = LANGUAGE_TO_COUNTRY.get(row.get("Language"))
    keywords = [kw.strip() for kw in row.get("keywords", []) if kw.strip()]

    # Determine managing organisation: prefer top-level Organisation > dep/fac > fallback
    managing_org = {"organization-uuid": "UNKNOWN", "systemName": "Organization"}
    if persons:
        candidates = [(get_org_type(o.get("organization-uuid"))[0], o) for o in persons[0][3]]
        org = (
            next((o for t, o in candidates if t == "Organization"), None)
            or next((o for t, o in candidates if t == "dep/fac"), None)
            or (candidates[0][1] if candidates else None)
        )
        if org:
            managing_org = org

    # Build per-person coverage entries
    coverage_persons = []
    org_set = set()
    for pure_id, person_uuid, full_name, orgs in persons:
        first_name, *rest = full_name.split()
        last_name = " ".join(rest)
        seen_orgs: set = set()
        org_list = []
        for o in orgs:
            key = o["organization-uuid"]
            if key not in seen_orgs:
                seen_orgs.add(key)
                org_list.append({"uuid": key, "systemName": "Organization"})
        for o in org_list:
            org_set.add((o["uuid"], o["systemName"]))

        coverage_persons.append({
            "typeDiscriminator": "InternalPressMediaPersonAssociation",
            "name": {"firstName": first_name, "lastName": last_name},
            "role": {"uri": role_uri, "term": {"en_GB": role_term.capitalize()}},
            "person": {"systemName": "Person", "uuid": person_uuid},
            "organizations": org_list,
        })

    payload = {
        "version": "v1",
        "title": {"en_GB": title},
        "type": {"uri": f"/dk/atira/pure/clipping/clippingtypes/clipping/{typerole.lower()}"},
        "visibility": {"key": "FREE", "description": {"en_GB": "Public - No restriction"}},
        "descriptions": [{
            "value": {"en_GB": " "},
            "type": {
                "uri": "/dk/atira/pure/clipping/descriptions/clippingdescription",
                "term": {"en_GB": "Description"},
            },
        }],
        "managingOrganization": {"uuid": managing_org["organization-uuid"], "systemName": "Organization"},
        "mediaCoverages": [{
            "coverageType": media_type,
            "title": {"en_GB": title},
            "description": {"en_GB": " "},
            "url": url,
            "medium": row["Media name"],
            "mediaType": {
                "uri": f"/dk/atira/pure/clipping/mediatype/{medium_type.lower()}",
                "term": {"en_GB": medium_type},
            },
            "degreeOfRecognition": {
                "uri": f"/dk/atira/pure/clipping/degreeofrecognition/{degree.lower()}",
                "term": {"en_GB": degree.capitalize()},
            },
            "authorProducer": "",
            "durationLengthSize": "",
            "date": date,
            "persons": coverage_persons,
            "organizations": [{"uuid": u, "systemName": s} for u, s in org_set],
        }],
        "workflow": build_workflow(row["Faculty"]),
        "keywordGroups": [CLASSIFICATION_GROUP] + ([make_free_keywords_group(keywords)] if keywords else []),
    }
    if country:
        payload["mediaCoverages"][0]["country"] = {"uri": f"/dk/atira/pure/core/countries/{country}"}

    # NOTE: PDF attachment via 'images' is not supported by Pure (images only accepts
    # .jpg/.jpeg/.png/.bmp/.gif). PDFs are archived locally in output/pdf/ instead.
    # Revisit if Pure exposes a documents endpoint for press media in a future version.

    return payload

# --- Upload ------------------------------------------------------------------

def upload_processed_articles(processed_articles: list, api_key: str, api_url_base: str) -> None:
    """PUT each processed article to the Pure press media endpoint."""
    json_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api-key": api_key,
    }
    success, fail = 0, 0
    for row in processed_articles:
        payload = build_payload_from_row(row)
        try:
            response = SESSION.put(
                f"{api_url_base}/pressmedia",
                headers=json_headers,
                data=json.dumps(payload),
                timeout=UPLOAD_TIMEOUT,
            )
        except requests.RequestException as e:
            logger.warning(f"Upload failed for '{row['Media item title']}': {e}")
            fail += 1
            continue
        if response.status_code in (200, 201):
            location = response.headers.get("Location", "N/A")
            pdf_note = " + PDF" if row.get("pdf_path") else ""
            logger.info(f"Uploaded '{row['Media item title']}'{pdf_note} (local) → {location}")
            success += 1
        else:
            logger.warning(f"Upload failed for '{row['Media item title']}': {response.text}")
            fail += 1

    logger.info(f"Upload complete: {success} succeeded, {fail} failed")


def get_media_item(uuid: str) -> dict | None:
    """Fetch a single press media item from Pure by UUID."""
    headers = {"Content-Type": "application/json", "Accept": "application/json", "api-key": APIKEY_CRUD}
    response = requests.get(f"{BASEURL_CRUD}/pressmedia/{uuid}", headers=headers)
    if response.status_code == 200:
        return response.json()
    return None
