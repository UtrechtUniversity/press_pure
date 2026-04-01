#!/usr/bin/env python3
"""Main pipeline: parse LexisNexis .eml files → resolve URLs → match persons → enrich → upload to Pure."""

import logging
import locale
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import configparser
import nltk
import pandas as pd

import ai_functions
import parsing_functions
import pdf_archiver
import pure_functions
import url_resolver
import xml_builder

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT_DIR / "knipsel"
OUTPUT_DIR = ROOT_DIR / "output"
LOG_DIR = ROOT_DIR / "logs"

locale.setlocale(locale.LC_TIME, "nl_NL.UTF-8")

CONFIG_PATH = ROOT_DIR / "config.cfg"
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
APIKEY_CRUD = CONFIG["CREDENTIALS"]["APIKEY_CRUD"]
BASEURL_CRUD = CONFIG["CREDENTIALS"]["BASEURL_CRUD"]
AI = CONFIG.getboolean("AI", "AI")
DOWNLOAD_PDFS = CONFIG.getboolean("PDF", "DOWNLOAD", fallback=False)

FACULTY_MAP = {
    "Beta 1": "BETA", "Beta 2": "BETA", "Beta 3": "BETA",
    "DGK": "DGK", "FSW": "FSW", "GEO": "GEO", "GW": "GW", "REBO": "REBO",
}


def setup_logging() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"press_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


def extract_faculty(filename: str) -> str | None:
    match = re.search(r"faculteit (.*?) -", filename)
    if not match:
        return None
    return FACULTY_MAP.get(match.group(1).strip(), match.group(1).strip())


def deduplicate_articles(articles: list, fields: list) -> list:
    seen = set()
    unique = []
    for article in articles:
        key = tuple(article[f] for f in fields)
        if key not in seen:
            seen.add(key)
            unique.append(article)
    return unique


def process_article(article: dict) -> tuple[dict | None, str]:
    """Resolve persons, check duplicates, and enrich a single article.

    Returns (article, status) where status is 'ok', 'duplicate', or 'no_persons'.
    """
    persons, _ = pure_functions.resolve_persons(article["Person"], article["Datum"])

    if not persons and not article.get("Person"):
        return None, "no_persons"

    if pure_functions.check_duplicates(article["Media item title"], persons, article["Datum"]):
        return None, "duplicate"

    if not persons:
        return None, "no_persons"

    article["Person_resolved"] = persons

    if AI:
        article = ai_functions.ai_getinfo(article)
    else:
        article["article_degree"] = "national"
        article["researcher_role"] = "interviewee"
        article["media_type"] = "Contribution"
        article["typerole"] = "exportcomment"
        article["goodfit"] = "yes"
        article["keywords"] = article["Keywords"]
        article["Medium_type"] = "Web"

    return article, "ok"


def main() -> None:
    setup_logging()
    OUTPUT_DIR.mkdir(exist_ok=True)
    logger.info("Press clipping processing started.")

    # --- Phase 1: Parse .eml files -------------------------------------------
    t = time.time()
    all_articles = []
    for eml_file in INPUT_DIR.rglob("*.eml"):
        logger.info(f"Parsing: {eml_file.name}")
        faculty = extract_faculty(eml_file.name)
        all_articles.extend(parsing_functions.process_html_file(eml_file, faculty))
    logger.info(f"Parsed {len(all_articles)} articles in {time.time()-t:.1f}s")

    # --- Phase 2: Resolve URLs -----------------------------------------------
    t = time.time()
    url_resolver.batch_resolve_urls(all_articles)
    logger.info(f"URL resolution done in {time.time()-t:.1f}s")

    # --- Phase 3: Person lookup, dedup, enrichment (parallel) ----------------
    t = time.time()
    processed_articles = []
    counts = {"ok": 0, "duplicate": 0, "no_persons": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_article, article): article for article in all_articles}
        for future in as_completed(futures):
            try:
                result, status = future.result()
            except Exception as e:
                logger.warning(f"Article processing error: {e}")
                counts["error"] += 1
                continue
            counts[status] += 1
            if status == "ok":
                processed_articles.append(result)

    logger.info(
        f"Processing done in {time.time()-t:.1f}s — "
        f"{counts['ok']} ok, {counts['duplicate']} duplicates, "
        f"{counts['no_persons']} no persons, {counts['error']} errors"
    )

    # Deduplicate by title + URL before export
    processed_articles = deduplicate_articles(processed_articles, ["Media item title", "URL"])
    logger.info(f"{len(processed_articles)} unique articles after deduplication")

    # Debug export
    pd.DataFrame(processed_articles).to_excel(LOG_DIR / "processed_articles.xlsx", index=False)

    # --- Phase 4: Archive as PDF (optional) ----------------------------------
    if DOWNLOAD_PDFS:
        t = time.time()
        saved = pdf_archiver.batch_save_pdfs(processed_articles, OUTPUT_DIR / "pdf")
        logger.info(f"PDF archiving done in {time.time()-t:.1f}s: {saved}/{len(processed_articles)} saved")
    else:
        logger.info("PDF archiving skipped (PDF.DOWNLOAD=false)")

    # --- Phase 5: Build XML + upload -----------------------------------------
    t = time.time()
    xml_content = xml_builder.build_xml(processed_articles)
    output_file = OUTPUT_DIR / f"press_clippings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
    output_file.write_text(xml_content, encoding="utf-8")
    logger.info(f"XML written to {output_file}")

    pure_functions.upload_processed_articles(
        processed_articles, api_key=APIKEY_CRUD, api_url_base=BASEURL_CRUD
    )
    logger.info(f"Upload phase done in {time.time()-t:.1f}s")
    logger.info("Pipeline complete.")


if __name__ == "__main__":
    nltk.download("punkt", quiet=True)
    nltk.download("stopwords", quiet=True)
    main()
