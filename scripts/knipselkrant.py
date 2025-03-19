#!/usr/bin/env python3
"""Main script to process LexisNexis press clippings and generate Pure-compatible XML."""
import yake

from datetime import datetime
import logging
import locale
import pandas as pd
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
import pure_functions
import xml_builder
import configparser
import re
import json
from openai import OpenAI
import requests
# Configuration
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT_DIR / "knipsel"
OUTPUT_DIR = ROOT_DIR / "output"
LOG_DIR = ROOT_DIR / "logs"

VALID_FACULTIES = [
    "Faculteit Bètawetenschappen", "Faculteit Betawetenschappen", "Faculteit Diergeneeskunde",
    "Faculteit Geesteswetenschappen", "Faculteit Geowetenschappen", "Faculteit REBO",
    "Faculteit Sociale Wetenschappen", "UMC Utrecht"
]
STOP_WORDS = set(stopwords.words("dutch"))

# Set locale to Dutch for month abbreviations
locale.setlocale(locale.LC_TIME, "nl_NL.UTF-8")

# List of unwanted terms
unwanted_terms = [
    r'\bUtrecht University\b',
    r'\bUniversiteit Utrecht\b',
    r'\bUniversiteit\b',
    r'\bUtrecht\b'
]

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
CONFIG.read('config.cfg')  # Adjust path if needed
GOOGLE_API = CONFIG['CREDENTIALS']['GOOGLE_API']
GOOGLE_CX = CONFIG['CREDENTIALS']['GOOGLE_CX']
OPENAI_API = CONFIG['CREDENTIALS']['OPENAI_API']

def setup_logging() -> None:
    """Configure logging to file and console with a timestamped log file."""
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"press_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Press clipping processing started.")


def clean_text(text: str) -> str:
    """Remove newlines, pipes, and extra spaces from text."""
    return " ".join(text.replace("\n", " ").replace("|", "").split()).strip()


def parse_date(date_str: str) -> datetime | None:
    """Parse a date string, handling variations and returning a datetime object."""
    try:
        cleaned_date = clean_text(date_str)
        return datetime.strptime(cleaned_date, "%d %b %Y %H:%M")
    except ValueError as e:
        logging.warning(f"Failed to parse date '{date_str}': {e}")
        return None


def extract_keywords(title: str) -> str:
    """Extract keywords from a title by tokenizing and removing stopwords/punctuation."""
    if not title:
        return ""
    # Set YAKE parameters (adjust as needed)
    kw_extractor = yake.KeywordExtractor(
        lan='en',
        n=3,  # max keyword phrase length (1 to 3 recommended)
        dedupLim=0.5,  # deduplication limit (0 to 1)
        top=5,  # number of keywords returned
        features=None
    )
    # Extract keywords
    keywords = [keyword for keyword, score in kw_extractor.extract_keywords(title)]

    return keywords


def extract_faculties(soup: BeautifulSoup) -> list[str]:
    """Identify faculty names from an article block."""
    faculties = set()
    for tag in soup.find_all(["strong", "td", "span", "div", "table"]):
        text = tag.get_text(strip=True)
        for faculty in VALID_FACULTIES:
            if faculty in text:
                faculties.add(faculty)
    return list(faculties) or ["not found"]


# Function to clean names
def clean_names(name_set):
    cleaned_set = set()
    for name in name_set:
        # Remove unwanted terms
        cleaned_name = name
        for term in unwanted_terms:
            cleaned_name = re.sub(term, '', cleaned_name).strip()

        # Remove extra spaces and keep only non-empty names
        cleaned_name = ' '.join(cleaned_name.split())
        if cleaned_name:
            cleaned_set.add(cleaned_name)

    return cleaned_set

def extract_persons(block) -> list[str]:
    persons = set()

    # Step 1: Extract names from the "Personen" sections
    personen_sections = block.find_all('strong', string='Personen')
    for section in personen_sections:
        names_block = section.find_parent().text
        names_block = re.sub(r'Personen:\s*', '', names_block)  # Remove "Personen:" prefix

        # Split names by commas
        split_names = re.split(r',\s*', names_block)
        for name in split_names:
            name_clean = name.strip()
            # Remove trailing ellipses if present
            name_clean = re.sub(r'\s*\.\.\.$', '', name_clean)
            if name_clean:
                persons.add(name_clean)

    # Step 2: Extract names from "Faculty / Name" patterns
    faculty_pattern = re.compile(r'Faculteit [^/]+ / ([^/]+)')
    faculty_matches = faculty_pattern.findall(block.text)
    for match in faculty_matches:
        name_clean = match.strip()
        # Remove trailing faculty information if it exists
        name_clean = re.sub(r'\nFaculteit [^/]+', '', name_clean)
        # Remove trailing ellipses if present
        name_clean = re.sub(r'\s*\.\.\.$', '', name_clean)
        if name_clean:
            persons.add(name_clean)

    # Step 3: Extract names from green-highlighted text (keep full names)
    green_highlighted = block.find_all('span', style=re.compile(r'background:#88C53E;'))
    highlighted_names = []

    for tag in green_highlighted:
        name_clean = tag.get_text(strip=True)
        if name_clean:
            highlighted_names.append(name_clean)

    # Combine consecutive names into full names
    full_highlighted_names = []
    temp_name = []

    for name in highlighted_names:
        temp_name.append(name)
        # Check if next name exists or this is the last name in sequence
        if len(temp_name) > 1:
            full_highlighted_names.append(" ".join(temp_name))
        else:
            full_highlighted_names.append(name)

    # Add the full highlighted names to the persons set
    persons.update(full_highlighted_names)

    # Step 4: Deduplication logic to remove partial names if a full name exists
    persons_list = list(persons)
    final_persons = set()

    # Build a set of full names
    full_names = {name for name in persons_list if ' ' in name}

    # Only add names that are not substrings of any longer full name
    for name in persons_list:
        if any(full_name != name and name in full_name.split() for full_name in full_names):
            continue
        final_persons.add(name)

    # Log for verification
    # Remove unwanted entries and artifacts
    final_persons = clean_names(final_persons)

    logging.debug(f"Extracted persons: {list(final_persons)}")

    return list(final_persons)


def ai_getinfo(row):
    client = OpenAI(api_key=OPENAI_API)

    url = row['URL']

    # Set a user-agent to avoid getting blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:

        soup = BeautifulSoup(response.text, "html.parser")
        article_text = soup.get_text(separator="\n", strip=True)  # Extract visible text

        article = article_text
    else:
        print(f"Failed to fetch page, status code: {response.status_code}")
        article = row['Media item title']



    title = row["Media item title"]
    source = row["Media name"]
    # Extract names
    names = ", ".join(person[1] for person in row['Person_resolved'])

    # Extract organizations
    organizations = ", ".join(org['orgname'] for person in row['Person_resolved'] for org in person[2])

    print('ai row: ', row)
    # Construct the prompt using an f-string to include variable values
    prompt = f"""
        I have an article with the following details:
        - Article Title: {title}
        - Source: {source}
        - University Researcher: {names}
        - Article Content: {article}
        - organisation: {organizations}

        Please perform the following tasks:

        1. Extract the most relevant keywords from the article.
           - Return them as a list of words or short phrases.

        2. Determine the degree of recognition.
           - local
           - national
           - international

        3. Determine the Researcher’s Role:  
           Identify the researcher’s role from the following options:
           - research cited
           - interviewee
           - participant
           - author

        4. Determine the Media Item Type:  
           Decide whether the media item is a:
           - Media Contribution  
           or  
           - Media Coverage

        5. Determine the Typerole:  
           Classify the typerole as one of the following:
           - expert comment
           - research
           - public engagement activity
           - other

        6. determine Goodfit:
            if the subject and keywords of the article make sense to belong to the organisation {organizations}, answer with yes or no or maybe. for example 'the role of gods in Egypt' does noet belong to 'faculteit diergeneeskunde'

        Return the result in **JSON format** like this:
        {{
            "keywords": ["keyword1", "keyword2", "keyword3", ...],
            "degree": "determined_degree_of_recognition_here",
            "researcher_role": "determined_role_here",
            "media_type": "determined_type_here",
            "typerole": "determined_typerole_here"
            "goodfit": "determined_goodfit_here"
        }}
        only return this, and nothing else.
        """

    # Create a chat completion
    # Use the client’s chat completion method.
    response = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    ai_output = response.choices[0].message.content

    clean_json = re.sub(r'^```json\n|\n```$', '', ai_output, flags=re.MULTILINE)

    data = json.loads(clean_json)  # Convert JSON string to Python dictionary

    # Assign values to variables
    row['article_degree'] = data.get("degree", "degree not found")
    row['researcher_role'] = data.get("researcher_role", "Unknown role")
    row['media_type'] = data.get("media_type", "media ontribution")
    row['media_type'] = row['media_type'].replace("Media ", "")
    row['typerole'] = data.get("typerole", "Unknown typerole")
    row['goodfit'] = data.get("goodfit", "Unknown goodfit")
    row['keywords'] = data.get("keywords", "")

    return row


def process_html_file(file_path: Path) -> list[dict]:
    """Parse an HTML file and extract article metadata."""
    with file_path.open("r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    articles = []
    for block in soup.find_all("tr", class_="article_container"):
        title_tag = block.find("a", class_="email-article-headline")
        if not title_tag:
            continue

        title = clean_text(title_tag.get_text(strip=False))
        title = title.lstrip('-').lstrip()
        url = title_tag.get("href", "")[:1024]
        date_tag = block.find("span", class_="article-email-harvest-date")
        date = parse_date(date_tag.get_text(strip=True)) if date_tag else None
        source_tag = block.find("a", class_="email-article-source-name")
        source = clean_text(source_tag.get_text(strip=True)) if source_tag else "Unknown"

        if date:  # Only include articles with a valid date
            articles.append({
                "Media item title": title,
                "URL": url,
                "Datum": date,
                "Media name": source,
                "Faculty": extract_faculties(block),
                "Person": extract_persons(block),
                "Keywords": extract_keywords(title)
            })
    return articles

# In main(), load config
def main():
    setup_logging()
    OUTPUT_DIR.mkdir(exist_ok=True)

    all_articles = []
    for html_file in INPUT_DIR.glob("*.html"):
        logging.info(f"Processing file: {html_file.name}")
        articles = process_html_file(html_file)
        all_articles.extend(articles)

    logging.info(f"Starting batch URL resolution for {len(all_articles)} articles")
    pure_functions.batch_resolve_urls(all_articles, pure_functions.SESSION, pure_functions.HEADERS)

    processed_articles = []
    counter = 0
    AI = False
    for article in all_articles:
        counter += 1
        persons, errors = pure_functions.resolve_persons(article["Person"], article["Datum"])
        # Log resolution details for debugging

        logging.info(f"Article '{article['Media item title']}': Resolved persons={persons}, Errors={errors}")

        # Only skip if absolutely necessary (e.g., no persons AND critical errors)
        if not persons and errors and not article.get("Person"):  # Skip only if no persons provided AND all fail
            logging.warning(f"Skipping '{article['Media item title']}': No valid persons and errors={errors}")
            continue

        # Proceed even with partial success
        if not pure_functions.check_duplicates(article["Media item title"], persons, article["Datum"]):

            if persons:
               article["Person_resolved"] = persons if persons else []  # Use empty list if no persons resolve
               if AI == True:
                   article = ai_getinfo(article)
               else:
                   article['article_degree'] = "national"
                   article['researcher_role'] = "interviewee"
                   article['media_type'] = "Contribution"
                   article['typerole'] = "exportcomment"
                   article['goodfit'] = "yes"
                   article['keywords'] = article["Keywords"]
               processed_articles.append(article)
        else:
            logging.info(f"Duplicate found: {article['Media item title']}")
        # if counter ==2:
        #     break
    # Log final count
    logging.info(f"Processed {len(processed_articles)} articles into XML")

    xml_content = xml_builder.build_xml(processed_articles)
    output_file = OUTPUT_DIR / f"press_clippings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
    output_file.write_text(xml_content, encoding="utf-8")
    logging.info(f"Generated XML file: {output_file}")

    pd.DataFrame(all_articles).to_excel(LOG_DIR / "processed_articles.xlsx", index=False)


if __name__ == "__main__":
    nltk.download("punkt")
    nltk.download("stopwords")
    main()