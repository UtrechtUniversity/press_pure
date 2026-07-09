"""Helpers for deriving Pure media type from Nexis metadata."""

import re


RADIO_PATTERN = re.compile(r"\b(radio|nieuwsradio)\b", re.IGNORECASE)
TV_PATTERN = re.compile(
    r"\b(tv|television)\b|"
    r"\b(npo\s*[123]|rtl\s*(4|5|7|8|z)|sbs6|net5|veronica|canvas|vrt\s*1|wlky-tv)\b",
    re.IGNORECASE,
)


def infer_medium_type(title: str, source: str, feed: str | None) -> str | None:
    """Infer Pure medium type from the Nexis section header and item metadata.

    Nexis' TVRADIO feed identifies broadcast items, but it does not distinguish
    radio from television. Use source/title patterns for that last step.
    """
    feed_text = (feed or "").upper()
    text = f"{title or ''} {source or ''}"

    if "ARTICLE" in feed_text:
        return "Web"

    if "TVRADIO" not in feed_text:
        return None

    if RADIO_PATTERN.search(text):
        return "Radio"
    if TV_PATTERN.search(text):
        return "TV"

    return None
