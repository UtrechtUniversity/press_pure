import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)
import configparser
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
from bs4 import BeautifulSoup

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
CONFIG.read('config.cfg')
OPENAI_API = CONFIG['CREDENTIALS']['OPENAI_API']

MODEL = "gpt-4o-mini"
MAX_ARTICLE_CHARS = 2000  # Limit article body before building the prompt

# Client once at module level — not per article
_client = OpenAI(api_key=OPENAI_API)


def fetch_article_text(url, fallback_title):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    session = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            return soup.get_text(separator="\n", strip=True)
        else:
            logger.warning(f"Failed to fetch page {url}, status code: {response.status_code}")
            return fallback_title
    except requests.exceptions.RequestException as e:
        logger.warning(f"Exception fetching URL {url}: {e}")
        return fallback_title


def rename_typerole(typerole):
    if typerole == "public engagement activity":
        typerole = "publicengagement"
    elif typerole == "expert comment":
        typerole = "exportcomment"
    elif typerole == "unknown":
        typerole = "other"
    return typerole


def ai_getinfo(row):
    url = row['URL']
    if url:
        article_text = fetch_article_text(url, row['Media item title'])
    else:
        article_text = row["Media item title"]

    # Truncate article body before building the prompt so the rest is always intact
    article_text = article_text[:MAX_ARTICLE_CHARS]

    title = row["Media item title"]
    source = row["Media name"]
    # person tuple: (employee_id, uuid, original_name, affiliations)
    names = ", ".join(person[2] for person in row['Person_resolved'])
    organizations = ", ".join(org['orgname'] for person in row['Person_resolved'] for org in person[3])

    prompt = f"""I have an article with the following details:
- Title: {title}
- Source: {source}
- Researcher: {names}
- Organisation: {organizations}
- Content (excerpt): {article_text}

Return a JSON object with exactly these fields:

1. "keywords": list of 4 relevant keywords (max 2 words each)
2. "degree": one of "local", "national", "international" (English article → international)
3. "researcher_role": one of "research cited", "interviewee", "participant", "author"
   - interviewee: quoted as only person
   - author: ONLY if clearly stated
   - participant: default when in doubt
4. "typerole": one of "expert comment", "research", "public engagement activity"
5. "Medium_type": one of "Radio", "TV", "Web" (use Web if unclear)
"""

    try:
        response = _client.chat.completions.create(
            model=MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a metadata classifier. Return only valid JSON."},
                {"role": "user", "content": prompt},
            ],
        )
        data = json.loads(response.choices[0].message.content)
    except Exception as e:
        logger.warning(f"AI call failed for '{title}': {e}")
        data = {
            "keywords": [],
            "degree": "national",
            "researcher_role": "participant",
            "typerole": "unknown",
            "Medium_type": "Web",
        }

    row['article_degree'] = data.get("degree", "national")
    row['researcher_role'] = data.get("researcher_role", "participant")

    if row['researcher_role'] in ("research cited", "researchcited"):
        row['researcher_role'] = 'researchcited'
        row['media_type'] = "Coverage"
    else:
        if row['researcher_role'] not in ("author", "interviewee"):
            row['researcher_role'] = 'participant'
        row['media_type'] = "Contribution"

    row['typerole'] = rename_typerole(data.get("typerole", "unknown"))
    row['goodfit'] = "yes"
    row['keywords'] = data.get("keywords", [])
    row['Medium_type'] = data.get("Medium_type", "Web")

    logger.info(f"AI classified '{title}': role={row['researcher_role']}, type={row['typerole']}")
    return row
