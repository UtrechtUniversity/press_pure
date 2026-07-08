"""Download article pages and archive as PDF when content is meaningful.

Dependencies:
    pip install readability-lxml weasyprint

WeasyPrint also requires system libraries on Linux:
    Fedora:  sudo dnf install pango cairo gdk-pixbuf2
    Ubuntu:  sudo apt install libpango-1.0-0 libcairo2 libgdk-pixbuf2.0-0
"""

import logging
import re
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)

# Check dependencies once at import time
try:
    from readability import Document
    import weasyprint
    _PDF_AVAILABLE = True
except ImportError as e:
    _PDF_AVAILABLE = False
    logger.warning(f"PDF archiving disabled — install readability-lxml and weasyprint: {e}")

# --- Quality thresholds ------------------------------------------------------

MIN_WORD_COUNT = 200

# Paywall / login wall patterns (checked against first 2000 chars)
_PAYWALL = re.compile(
    r"(subscribe to (read|continue)|log.?in to (read|access|continue)|"
    r"premium content|members?.only|sign up to (read|continue)|"
    r"create an? account to|access denied|content not available|"
    r"dit artikel is alleen voor abonnees|lees verder na registratie)",
    re.IGNORECASE,
)


def _is_good_content(text: str) -> tuple[bool, str]:
    """Return (True, 'ok') when content is a real article worth archiving."""
    word_count = len(text.split())
    if word_count < MIN_WORD_COUNT:
        return False, f"too short ({word_count} words, min {MIN_WORD_COUNT})"
    if _PAYWALL.search(text[:2000]):
        return False, "paywall or login wall detected"
    return True, "ok"


def _sanitize_filename(title: str) -> str:
    """Convert article title to a safe filename, max 80 chars."""
    clean = re.sub(r"[^\w\s-]", "", title)
    clean = re.sub(r"\s+", "_", clean.strip())
    return clean[:80]


# --- Core function -----------------------------------------------------------

def fetch_and_save_pdf(article: dict, output_dir: Path) -> Path | None:
    """Fetch the article URL, check content quality, and save as PDF.

    Adds 'pdf_path' key to the article dict (Path or None).
    Returns the PDF path on success, None otherwise.
    """
    url = article.get("URL")
    title = article["Media item title"]

    if not _PDF_AVAILABLE:
        article["pdf_path"] = None
        return None

    if not url:
        logger.debug(f"PDF skip '{title}': no URL")
        article["pdf_path"] = None
        return None

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code != 200:
            logger.debug(f"PDF skip '{title}': HTTP {response.status_code}")
            article["pdf_path"] = None
            return None

        if "text/html" not in response.headers.get("Content-Type", ""):
            logger.debug(f"PDF skip '{title}': not HTML")
            article["pdf_path"] = None
            return None

        # Extract main article body using Mozilla's Readability algorithm
        doc = Document(response.text)
        extracted_html = doc.summary()
        extracted_text = re.sub(r"<[^>]+>", " ", extracted_html)

        good, reason = _is_good_content(extracted_text)
        if not good:
            logger.debug(f"PDF skip '{title}': {reason}")
            article["pdf_path"] = None
            return None

        # Render clean article HTML to PDF
        html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{ font-family: Georgia, serif; max-width: 780px; margin: 40px auto;
            font-size: 14px; line-height: 1.7; color: #222; }}
    h1   {{ font-size: 20px; margin-bottom: 6px; }}
    .src {{ color: #666; font-size: 11px; margin-bottom: 28px; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div class="src">Bron: {url}</div>
  {extracted_html}
</body>
</html>"""

        output_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = output_dir / f"{_sanitize_filename(title)}.pdf"
        weasyprint.HTML(string=html, base_url=url).write_pdf(str(pdf_path))

        logger.info(f"PDF saved '{title}' → {pdf_path.name}")
        article["pdf_path"] = pdf_path
        return pdf_path

    except Exception as e:
        logger.warning(f"PDF failed for '{title}': {e}")
        article["pdf_path"] = None
        return None


# --- Batch -------------------------------------------------------------------

def batch_save_pdfs(articles: list, output_dir: Path, max_workers: int = 5) -> int:
    """Save PDFs for all articles in parallel. Returns count of successfully saved PDFs."""
    if not _PDF_AVAILABLE:
        logger.warning("PDF archiving skipped — dependencies not installed")
        return 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_and_save_pdf, a, output_dir) for a in articles]
        results = [f.result() for f in futures]

    saved = sum(1 for r in results if r is not None)
    logger.info(f"PDF archiving complete: {saved}/{len(articles)} saved")
    return saved
