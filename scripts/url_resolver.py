"""URL resolution for LexisNexis press clipping links.

Three-phase strategy:
  1. Direct HTTP follow      — fast, works for public articles
  2. DuckDuckGo search       — free fallback, limited concurrency to avoid blocks
  3. Google Custom Search    — quota-conscious last resort (100 free/day)
"""

import logging
import configparser
import threading
import time
import requests
from urllib.parse import urlparse, parse_qs, quote_plus, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
GOOGLE_API = CONFIG["CREDENTIALS"]["GOOGLE_API"]
GOOGLE_CX = CONFIG["CREDENTIALS"]["GOOGLE_CX"]

BLOCKED_DOMAINS = {"lexisnexis.com", "advance.lexis.com"}
DDG_MAX_WORKERS = 2
GOOGLE_MAX_WORKERS = 2
NETWORK_TIMEOUT = (3, 8)
PROGRESS_EVERY = 10
_NETWORK_FAILURE_MARKERS = (
    "NameResolutionError",
    "Temporary failure in name resolution",
    "Failed to resolve",
    "ConnectTimeoutError",
    "ReadTimeout",
    "Connection timed out",
)

# Dedicated session for web fetching (separate from the Pure API session)
_session = requests.Session()
# Avoid retrying DNS/connect/read failures: these create warning bursts and
# are unlikely to recover immediately.
_retry = Retry(
    total=2,
    connect=0,
    read=0,
    status=2,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)
_provider_lock = threading.Lock()
_ddg_disabled = False
_google_disabled = False

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


def _is_valid_url(url: str) -> bool:
    """Return True if the URL is a real article destination (not LexisNexis or a fragment)."""
    lower = url.lower()
    return not any(domain in lower for domain in BLOCKED_DOMAINS) and "#content" not in lower


def _is_network_failure(exc: Exception) -> bool:
    """Return True for DNS/connectivity problems where retries are unlikely to help."""
    message = str(exc)
    return any(marker in message for marker in _NETWORK_FAILURE_MARKERS)


def resolve_direct(url: str, title: str) -> str | None:
    """Follow redirects on the original LexisNexis link and return the final URL if it escapes LexisNexis."""
    try:
        response = _session.get(url, headers=_HEADERS, allow_redirects=True, timeout=NETWORK_TIMEOUT)
        if response.ok and _is_valid_url(response.url):
            logger.debug(f"Direct resolve OK '{title}': {response.url}")
            return response.url
        logger.debug(f"Direct resolve stayed on LexisNexis for '{title}': {response.url}")
    except requests.RequestException as e:
        logger.debug(f"Direct resolve failed '{title}': {e}")
    return None


def search_duckduckgo(title: str) -> str | None:
    """Search DuckDuckGo HTML and return the first non-LexisNexis result URL."""
    global _ddg_disabled
    if _ddg_disabled:
        return None
    search_url = f"https://duckduckgo.com/html/?q={quote_plus(title)}"
    ddg_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Accept-Language": "nl,en;q=0.9",
    }
    try:
        response = _session.get(search_url, headers=ddg_headers, timeout=NETWORK_TIMEOUT)
        if response.status_code != 200:
            logger.debug(f"DuckDuckGo returned {response.status_code} for '{title}'")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # DDG wraps result URLs in a redirect — extract 'uddg' param
        for link in soup.find_all("a", class_="result__a", href=True):
            qs = parse_qs(urlparse(link["href"]).query)
            url = unquote(qs.get("uddg", [""])[0])
            if url and _is_valid_url(url):
                logger.debug(f"DuckDuckGo found '{title}': {url}")
                return url

        # Fallback: direct href if DDG changes their redirect format
        for link in soup.find_all("a", class_="result__a", href=True):
            href = link["href"]
            if href.startswith("http") and _is_valid_url(href):
                logger.debug(f"DuckDuckGo direct href '{title}': {href}")
                return href

    except requests.RequestException as e:
        if _is_network_failure(e):
            with _provider_lock:
                if not _ddg_disabled:
                    _ddg_disabled = True
                    logger.warning("DuckDuckGo disabled for this run due to DNS/connectivity failure.")
        logger.debug(f"DuckDuckGo failed '{title}': {e}")
    return None


def search_google_cse(title: str) -> str | None:
    """Search using Google Custom Search API. Called as last resort to preserve daily quota."""
    global _google_disabled
    if not GOOGLE_API or not GOOGLE_CX:
        return None
    if _google_disabled:
        return None
    try:
        response = _session.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": GOOGLE_API, "cx": GOOGLE_CX, "q": title, "num": 1},
            timeout=NETWORK_TIMEOUT,
        )
        response.raise_for_status()
        items = response.json().get("items", [])
        if items:
            url = items[0]["link"]
            logger.debug(f"Google CSE found '{title}': {url}")
            return url
    except requests.RequestException as e:
        if _is_network_failure(e):
            with _provider_lock:
                if not _google_disabled:
                    _google_disabled = True
                    logger.warning("Google CSE disabled for this run due to DNS/connectivity failure.")
        logger.debug(f"Google CSE failed '{title}': {e}")
    return None


def _run_phase(articles: list, fn, max_workers: int) -> tuple[list, list]:
    """Run a resolver function over a list of articles in parallel.

    Returns (resolved_articles, unresolved_articles).
    """
    resolved, unresolved = [], []
    total = len(articles)
    if total == 0:
        return resolved, unresolved
    phase_name = getattr(fn, "__name__", "resolver")
    started = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fn, a["Media item title"]): a for a in articles}
        for idx, future in enumerate(as_completed(futures), start=1):
            article = futures[future]
            try:
                result = future.result()
            except Exception as e:
                logger.debug(f"Resolver exception for '{article['Media item title']}': {e}")
                result = None
            if result:
                article["URL"] = result
                resolved.append(article)
            else:
                unresolved.append(article)
            if idx % PROGRESS_EVERY == 0 or idx == total:
                elapsed = time.time() - started
                logger.info(
                    f"URL {phase_name} progress: {idx}/{total} processed, "
                    f"{len(resolved)} resolved, {len(unresolved)} remaining, {elapsed:.1f}s elapsed"
                )
    return resolved, unresolved


def batch_resolve_urls(articles: list) -> None:
    """Resolve URLs in-place on the articles list using a three-phase fallback strategy."""
    # Phase 1: direct HTTP follow (fast, high concurrency)
    needs_url = [a for a in articles if a.get("URL", "").strip()]
    no_url = [a for a in articles if not a.get("URL", "").strip()]

    phase1_resolved, after_phase1 = [], []
    phase1_total = len(needs_url)
    phase1_started = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(resolve_direct, a.get("URL", ""), a["Media item title"]): a
            for a in needs_url
        }
        for idx, future in enumerate(as_completed(futures), start=1):
            article = futures[future]
            try:
                result = future.result()
            except Exception as e:
                logger.debug(f"Phase 1 exception '{article['Media item title']}': {e}")
                result = None
            if result:
                article["URL"] = result
                phase1_resolved.append(article)
            else:
                article["URL"] = None
                after_phase1.append(article)
            if phase1_total and (idx % PROGRESS_EVERY == 0 or idx == phase1_total):
                elapsed = time.time() - phase1_started
                logger.info(
                    f"URL phase 1 progress: {idx}/{phase1_total} processed, "
                    f"{len(phase1_resolved)} resolved, {len(after_phase1)} remaining, {elapsed:.1f}s elapsed"
                )

    logger.info(f"URL phase 1 (direct): {len(phase1_resolved)} resolved, {len(after_phase1)} remaining")

    # Phase 2: DuckDuckGo
    phase2_resolved, after_phase2 = _run_phase(
        after_phase1,
        search_duckduckgo,
        max_workers=DDG_MAX_WORKERS,
    )
    logger.info(f"URL phase 2 (DuckDuckGo): {len(phase2_resolved)} resolved, {len(after_phase2)} remaining")

    # Phase 3: Google CSE — only for the hard cases that DDG couldn't find
    phase3_resolved, after_phase3 = _run_phase(
        after_phase2,
        search_google_cse,
        max_workers=GOOGLE_MAX_WORKERS,
    )
    logger.info(f"URL phase 3 (Google CSE): {len(phase3_resolved)} resolved, {len(after_phase3)} unresolvable")

    # Articles with no original URL stay None
    for a in no_url:
        a["URL"] = None

    total_resolved = len(phase1_resolved) + len(phase2_resolved) + len(phase3_resolved)
    logger.info(f"URL resolution complete: {total_resolved}/{len(articles)} resolved")
