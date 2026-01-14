#!/usr/bin/env python3
"""Main script to process LexisNexis press clippings and generate Pure-compatible XML."""
import yake
from datetime import datetime
import logging
import pandas as pd
from bs4 import BeautifulSoup
import re
from pathlib import Path
from email import policy
from email.parser import BytesParser
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
import configparser

blacklist_names = ['Anton Pijpers', 'David Beverborg']
# Determine project root and filter file path
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
FILTER_FILE = PROJECT_ROOT / "files" / "Filter_media.xlsx"

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
CONFIG.read('config.cfg')

# List of unwanted terms
unwanted_terms_raw = CONFIG.get("FILTERS", "UNWANTED_TERMS", fallback="")
unwanted_terms = [re.compile(term.strip()) for term in unwanted_terms_raw.split(",") if term.strip()]

ALLOWED_LANGUAGES = {
    lang.strip() for lang in CONFIG.get("DEFAULTS", "ALLOWED_LANGUAGES", fallback="").split(",") if lang.strip()
}
VALID_FACULTIES = [fac.strip() for fac in CONFIG.get("DEFAULTS", "VALID_FACULTIES", fallback="").split(",") if fac.strip()]

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

def extract_html_from_eml(eml_path):
    with open(eml_path, 'rb') as f:
        msg = BytesParser(policy=policy.default).parse(f)

    html_content = None
    # Walk through the email parts
    for part in msg.walk():
        content_type = part.get_content_type()
        if content_type == 'text/html':
            html_content = part.get_content()
            break  # Stop after finding the first HTML part

    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup
    else:
        return None

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

    # Step 3: Combine all green-highlighted tokens as one name (handles 'van der' etc.)
    green_highlighted = block.find_all('span', style=re.compile(r'background:#88C53E;', re.IGNORECASE))
    tokens = [tag.get_text(strip=True) for tag in green_highlighted if tag.get_text(strip=True)]

    # New: combine *all* tokens in one full name and also try pairs (fallback)
    if tokens:
        full_token_name = " ".join(tokens)
        if 1 <= len(full_token_name.split()) <= 6:
            persons.add(full_token_name)

        # Extra fallback: add 2-token combos if relevant (e.g., for rare edge cases)
        for i in range(len(tokens) - 1):
            combo = f"{tokens[i]} {tokens[i+1]}"
            if 1 <= len(combo.split()) <= 4:
                persons.add(combo)

        # Optional: add final token if odd count
        if len(tokens) % 2 != 0:
            persons.add(tokens[-1])

    # Step 4: Deduplication logic to remove partial names if a full name exists
    persons_list = list(persons)

    final_persons = set()

    # Precompute blacklist in lowercase
    lower_blacklist = {b.lower() for b in blacklist_names}

    # Cache splits en maak set van volledige namen (>= 2 tokens)
    split_cache = {name: name.split() for name in persons_list}
    full_names = {name for name in persons_list if len(split_cache[name]) > 1}

    for name in persons_list:
        # Sla naam over als die in de blacklist staat (case-insensitive)
        if name.lower() in lower_blacklist:
            continue

        parts = split_cache[name]
        if not parts:
            continue

        # Enkel woord (zoals "Siegel") -> skippen als het onderdeel is van een langere naam
        if len(parts) == 1:
            token = parts[0].lower()

            # Staat dit ene woord als losse token in een langere naam?
            in_longer_name = any(
                token == p.lower()
                for full_name in full_names
                for p in split_cache[full_name]
            )
            if in_longer_name:
                # bv. "Siegel" en "Dina Siegel" -> "Siegel" wordt geskipt
                continue

        final_persons.add(name)

    # Log voor verificatie + opschonen
    final_persons = clean_names(final_persons)
    # =============================================
    # FINAL STEP: remove single-word names that are
    # contained in any multi-word name
    # =============================================
    final_list = list(final_persons)
    split_map = {n: n.split() for n in final_list}
    full_names = {n for n in final_list if len(split_map[n]) > 1}

    really_final = set()
    for n in final_list:
        parts = split_map[n]
        if len(parts) == 1:
            # skip single tokens if they appear inside any full name
            token = parts[0].lower()
            if any(token == p.lower() for fn in full_names for p in split_map[fn]):
                continue
        really_final.add(n)

    return list(really_final)






def rename_mediatype(mediatype):

    if mediatype == "Media Coverage":
        mediatype = "Coverage"
    elif mediatype == "Media Contribution":
        mediatype = "Contribution"
    return mediatype

def process_html_file(file_path: Path, faculty) -> list[dict]:
    """Parse an HTML file and extract article metadata."""

    soup = extract_html_from_eml(file_path)
    # with file_path.open("r", encoding="utf-8") as f:
    #     soup = BeautifulSoup(f.read(), "html.parser")

    if FILTER_FILE.exists():
        filtered_sources = set(pd.read_excel(FILTER_FILE, sheet_name="Media name").iloc[:, 0].dropna().str.strip())
        filtered_titles = set(pd.read_excel(FILTER_FILE, sheet_name="Media title").iloc[:, 0].dropna().str.strip())
    else:
        filtered_sources = set()
        filtered_titles = set()
        logging.warning(f"Filter file not found: {FILTER_FILE}. No sources will be filtered.")

    articles = []
    for block in soup.find_all("tr", class_="article_container"):
        title_tag = block.find("a", class_="email-article-headline")
        if not title_tag:
            continue

        title = clean_text(title_tag.get_text(strip=False))
        title = title.lstrip('-').lstrip()

        try:
            lang = detect(title)

            if lang not in ALLOWED_LANGUAGES:
                logging.debug(f"Skipped not allowed language article: '{title}' (lang: {lang})")
                continue

        except LangDetectException:
            logging.debug(f"Language detection failed for: '{title}'")
            continue
        url = title_tag.get("href", "")[:1024]
        date_tag = block.find("span", class_="article-email-harvest-date")
        date = parse_date(date_tag.get_text(strip=True)) if date_tag else None
        source_tag = block.find("a", class_="email-article-source-name")
        source = clean_text(source_tag.get_text(strip=True)) if source_tag else "Unknown"

        if source in filtered_sources:
            logging.info(f"Skipped article from filtered source: '{source}' - '{title}'")
            continue

        if any(word in title for word in filtered_titles):
            logging.info(f"Skipped article with filtered word in title: '{title}'")
            continue

        if date:  # Only include articles with a valid date
            articles.append({
                "Media item title": title,
                "URL": url,
                "Datum": date,
                "Media name": source,
                "Faculty": faculty,
                "Person": extract_persons(block),
                "Keywords": extract_keywords(title),
                "Language": lang
            })
    return articles
