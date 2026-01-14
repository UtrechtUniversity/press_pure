import json
from openai import OpenAI
import configparser
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
from bs4 import BeautifulSoup
import re
CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
CONFIG.read('config.cfg')  # Adjust path if needed
GOOGLE_API = CONFIG['CREDENTIALS']['GOOGLE_API']
GOOGLE_CX = CONFIG['CREDENTIALS']['GOOGLE_CX']
OPENAI_API = CONFIG['CREDENTIALS']['OPENAI_API']



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
            article_text = soup.get_text(separator="\n", strip=True)
            return article_text
        else:
            print(f"[WARN] Failed to fetch page {url}, status code: {response.status_code}")
            return fallback_title

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Exception fetching URL {url}: {e}")
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
    client = OpenAI(api_key=OPENAI_API)
    url = row['URL']
    article = fetch_article_text(url, row['Media item title'])
    title = row["Media item title"]
    source = row["Media name"]
    # Extract names
    names = ", ".join(person[1] for person in row['Person_resolved'])

    # Extract organizations
    organizations = ", ".join(org['orgname'] for person in row['Person_resolved'] for org in person[3])

    # Construct the prompt using an f-string to include variable values
    prompt = f"""
    I have an article with the following details:
    - Article Title: {title}
    - Source: {source}
    - University Researcher: {names}
    - Article Content: {article}
    - Organisation: {organizations}

    Please perform the following tasks:

    1. Extract the four most relevant keywords from the article.
       - Return them as a list of words or short phrases of maximum two words.

    2. Determine the degree of recognition.
       - If the language is English → "international"
       - Otherwise → "national"
       - If very local → "local"

    3. Researcher’s Role (choose only from):
       - "research cited"
       - "interviewee" (quoted as only person)
       - "participant" (quoted with others – default if doubt)
       - "author" (ONLY if clearly stated)

    4. Typerole (one of):
       - "expert comment"
       - "research"
       - "public engagement activity"

    5. Medium_type:
       - "Radio" (only if explicitly clear)
       - "TV" (only if explicitly clear)
       - "Web" (if unclear)

    Return ONLY valid JSON, nothing else:
    {{
        "keywords": ["keyword1", "keyword2", "keyword3", "keyword4"],
        "degree": "value",
        "researcher_role": "value",
        "typerole": "value",
        "Medium_type": "value"
    }}
    """

    # Create a chat completion
    response = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "Return only valid JSON without explanation."},
            {"role": "user", "content": prompt}
        ]
    )

    ai_output = response.choices[0].message.content

    # Extract possible JSON object
    match = re.search(r'\{[\s\S]*\}', ai_output)

    if not match:
        print(f"⚠️ Geen JSON gevonden voor artikel '{title}', AI-output:")
        print(ai_output)
        # fallback, zodat script niet crasht
        data = {
            "keywords": [],
            "degree": "unknown",
            "researcher_role": "unknown",
            "typerole": "unknown",
            "Medium_type": "unknown",
        }
    else:
        clean_json = match.group(0)
        clean_json = re.sub(r',\s*([\]}])', r'\1', clean_json)

        try:
            data = json.loads(clean_json)
        except Exception:
            print(f"⚠️ JSON probleem voor artikel '{title}', AI-output:")
            print(ai_output)
            # fallback
            data = {
                "keywords": [],
                "degree": "unknown",
                "researcher_role": "unknown",
                "typerole": "unknown",
                "Medium_type": "unknown",
            }

    # Assign values to variables
    row['article_degree'] = data.get("degree", "degree not found")
    row['researcher_role'] = data.get("researcher_role", "participant")

    if row['researcher_role'] == "research cited" or row['researcher_role'] == 'researchcited':
        row['researcher_role'] = 'researchcited'
        row['media_type'] =  "Coverage"

    else:
        if row['researcher_role'] != "author" and row['researcher_role'] != "interviewee":
            row['researcher_role'] = 'participant'
        row['media_type'] = "Contribution"

    row['typerole'] = data.get("typerole", "Unknown typerole")
    row['typerole'] = rename_typerole(row['typerole'])
    print(title, row['typerole'])
    row['goodfit'] = data.get("goodfit", "Unknown goodfit")
    row['keywords'] = data.get("keywords", "")
    row['Medium_type'] = data.get("Medium_type", "web")


    return row