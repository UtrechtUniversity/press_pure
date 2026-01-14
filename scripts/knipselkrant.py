#!/usr/bin/env python3
"""Main script to process LexisNexis press clippings and generate Pure-compatible XMLor json for ."""

from datetime import datetime
import logging
import locale
import pandas as pd
import nltk
import pure_functions
import xml_builder
import configparser
from pathlib import Path
import ai_functions
import parsing_functions
import re

ROOT_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT_DIR / "knipsel"
OUTPUT_DIR = ROOT_DIR / "output"
LOG_DIR = ROOT_DIR / "logs"

# Set locale to Dutch for month abbreviations
locale.setlocale(locale.LC_TIME, "nl_NL.UTF-8")


CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
CONFIG.read('config.cfg')  # Abatch_resolve_urlsdjust path if needed
GOOGLE_API = CONFIG['CREDENTIALS']['GOOGLE_API']
GOOGLE_CX = CONFIG['CREDENTIALS']['GOOGLE_CX']
OPENAI_API = CONFIG['CREDENTIALS']['OPENAI_API']
APIKEY_CRUD = CONFIG['CREDENTIALS']['APIKEY_CRUD']
BASEURL_CURD = CONFIG['CREDENTIALS']['BASEURL_CRUD']
WORKFLOW_STATUS = dict(CONFIG["WORKFLOW STATUS"])


FACULTY_MAP = {
    "Beta 1": "BETA",
    "Beta 2": "BETA",
    "Beta 3": "BETA",
    "DGK": "DGK",
    "FSW": "FSW",
    "GEO": "GEO",
    "GW": "GW",
    "REBO": "REBO"
}

def extract_faculty(filename: str) -> str | None:
    """
    Haalt de faculteitsnaam uit een bestandsnaam.
    Normaliseert naar vaste afkorting indien mogelijk.
    """
    match = re.search(r"faculteit (.*?) -", filename)
    if not match:
        return None
    raw_faculty = match.group(1).strip()
    return FACULTY_MAP.get(raw_faculty, raw_faculty)

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

def deduplicate_articles_by_fields(articles, fields):
    seen = set()
    unique_articles = []

    for article in articles:
        identifier = tuple(article[field] for field in fields)
        if identifier not in seen:
            seen.add(identifier)
            unique_articles.append(article)

    return unique_articles



def main():
    setup_logging()
    OUTPUT_DIR.mkdir(exist_ok=True)

    all_articles = []
    for html_file in INPUT_DIR.rglob("*.eml"):
        logging.info(f"Processing file: {html_file.name}")
        faculty = extract_faculty(html_file.name)
        articles = parsing_functions.process_html_file(html_file, faculty)

        all_articles.extend(articles)

    logging.info(f"Starting batch URL resolution for {len(all_articles)} articles")
    pure_functions.batch_resolve_urls(all_articles, pure_functions.SESSION, pure_functions.HEADERS)

    processed_articles = []
    counter = 0
    duplicates = 0
    no_valid_names = 0
    AI = True
    for article in all_articles:
        counter += 1
        print (article["Person"])
        persons, errors = pure_functions.resolve_persons(article["Person"], article["Datum"])
        print(persons)
        # Skip als helemaal niks bruikbaars
        if not persons and not article.get("Person"):
            no_valid_names += 1
            continue

        # Check op duplicaat
        if pure_functions.check_duplicates(article["Media item title"], persons, article["Datum"]):
            duplicates += 1
            continue

        # Als er wel personen zijn, verwerken
        if persons:
            article["Person_resolved"] = persons

            if AI:
                article = ai_functions.ai_getinfo(article)
            else:
                article['article_degree'] = "national"
                article['researcher_role'] = "interviewee"
                article['media_type'] = "Contribution"
                article['typerole'] = "exportcomment"
                article['goodfit'] = "yes"
                article['keywords'] = article["Keywords"]
                article['Medium_type'] = "Web"

            processed_articles.append(article)
        else:
            no_valid_names += 1

        # if counter ==2:
        #     break
    # Log final count
    pd.DataFrame(processed_articles).to_excel(LOG_DIR / "processed_articles.xlsx", index=False)
    logging.info(f"Processed {counter} articles")
    logging.info(f"Found {no_valid_names} articles without valid Pure persons")
    logging.info(f"Found {duplicates} duplicates")
    dedup_fields = ['Media item title', 'URL']
    processed_articles = deduplicate_articles_by_fields(processed_articles, dedup_fields)

    pd.DataFrame(processed_articles).to_excel(LOG_DIR / "processed_articles2.xlsx", index=False)
    logging.info(f"Processed {len(processed_articles)} of {len(all_articles)} articles into XML")


    xml_content = xml_builder.build_xml(processed_articles)

    pure_functions.upload_processed_articles(
        processed_articles,
        api_key=APIKEY_CRUD,
        api_url_base=BASEURL_CURD
    )

    output_file = OUTPUT_DIR / f"press_clippings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
    output_file.write_text(xml_content, encoding="utf-8")
    logging.info(f"Generated XML file: {output_file}")


if __name__ == "__main__":
    nltk.download("punkt")
    nltk.download("stopwords")
    main()