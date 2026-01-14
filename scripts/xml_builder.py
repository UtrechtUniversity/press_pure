#!/usr/bin/env python3
"""Utilities for building Pure-compatible XML structures for press clippings."""
import xml.etree.ElementTree as ET
import configparser

from xml.dom import minidom
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

NAMESPACE = "v1.unified.clipping.pure.atira.dk"
ET.register_namespace("v1", NAMESPACE)
FALLBACK_ORG_UUID = CONFIG.get("DEFAULTS", "FALLBACK_ORG_UUID", fallback="UNKNOWN")

def make_header() -> ET.Element:
    """Create the root XML element for Pure clippings."""
    return ET.Element(f"{{{NAMESPACE}}}clippings")

def make_single_clipping(root: ET.Element, article: Dict[str, Any], press_id: str) -> None:
    """Add a single clipping element to the XML root."""
    clipping = ET.SubElement(root, f"{{{NAMESPACE}}}clipping", {
        "id": press_id,
        "type": article['typerole'] ,
        "managedInPure": "true"
    })

    man_org = FALLBACK_ORG_UUID
    for person_id, uuid, name, orgs in article.get("Person_resolved", []):
        for org in orgs:

            if org['orgtype'] == 'Organization':
                man_org = org['organization-uuid']
                break


    ET.SubElement(clipping, f"{{{NAMESPACE}}}title").text = article["Media item title"]
    ET.SubElement(clipping, f"{{{NAMESPACE}}}description").text = ' '
    ET.SubElement(clipping, f"{{{NAMESPACE}}}startDate").text = article["Datum"].strftime("%Y-%m-%d")
    ET.SubElement(clipping, f"{{{NAMESPACE}}}managedBy",
                  {'lookupHint': 'orgSync', 'lookupId': man_org})
    # Keywords
    if article["keywords"]:
        keywords_elem = ET.SubElement(clipping, f"{{{NAMESPACE}}}keywords")
        for keyword in article["keywords"]:
            ET.SubElement(keywords_elem, f"{{{NAMESPACE}}}keyword").text = keyword

    ET.SubElement(clipping, f"{{{NAMESPACE}}}visibility").text = "Public"

    if article['goodfit'] == "no":
        ET.SubElement(clipping, f"{{{NAMESPACE}}}workflow").text = "entryInProgress"
    else:
        ET.SubElement(clipping, f"{{{NAMESPACE}}}workflow").text = "entryInProgress"

    # Media Reference
    ref_id = f"{press_id}_ref"
    media_refs = ET.SubElement(clipping, f"{{{NAMESPACE}}}mediaReferences")

    media_ref = ET.SubElement(media_refs, f"{{{NAMESPACE}}}mediaReference", {
        "type": article['media_type'] ,  # Adjust as needed
        "id": ref_id
    })
    ET.SubElement(media_ref, f"{{{NAMESPACE}}}title").text = article["Media item title"]
    ET.SubElement(media_ref, f"{{{NAMESPACE}}}date").text = article["Datum"].strftime("%Y-%m-%d")

    # Persons
    persons_elem = ET.SubElement(media_ref, f"{{{NAMESPACE}}}persons")
    for person_id, uuid, name, orgs in article.get("Person_resolved", []):
        person = ET.SubElement(persons_elem, f"{{{NAMESPACE}}}person", {"id": person_id})
        ET.SubElement(person, f"{{{NAMESPACE}}}person", {
            "lookupId": person_id,
            "lookupHint": "personSync",
            "origin": "internal"
        })
        ET.SubElement(person, f"{{{NAMESPACE}}}role").text = article["researcher_role"] # Adjust as needed

        orgs_elem = ET.SubElement(person, f"{{{NAMESPACE}}}organisations")
        for org in orgs:

            ET.SubElement(orgs_elem, f"{{{NAMESPACE}}}organisation", {
                "lookupId": org["organization-uuid"],
                "lookupHint": "orgSync",
                "origin": "internal"
            })

    ET.SubElement(media_ref, f"{{{NAMESPACE}}}medium").text = article["Media name"]
    if article["URL"]:
        ET.SubElement(media_ref, f"{{{NAMESPACE}}}url").text = article["URL"]
    ET.SubElement(media_ref, f"{{{NAMESPACE}}}mediaType").text = article["Medium_type"]
    ET.SubElement(media_ref, f"{{{NAMESPACE}}}degreeOfRecognition").text = str(article['article_degree'])

def remove_duplicates(root: ET.Element) -> ET.Element:
    """Remove duplicate clippings based on title and person IDs."""
    seen = set()
    for clipping in list(root):
        title = clipping.find(f"{{{NAMESPACE}}}mediaReferences/{{{NAMESPACE}}}mediaReference/{{{NAMESPACE}}}title").text
        person_ids = tuple(
            p.get("id") for p in clipping.findall(
                f"{{{NAMESPACE}}}mediaReferences/{{{NAMESPACE}}}mediaReference/{{{NAMESPACE}}}persons/{{{NAMESPACE}}}person")
        )
        key = (title.lower(), person_ids)
        if key in seen:
            root.remove(clipping)
        else:
            seen.add(key)
    return root


def build_xml(articles: List[Dict[str, Any]]) -> str:
    """Build a complete XML string from a list of articles."""
    root = make_header()
    for i, article in enumerate(articles):
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        make_single_clipping(root, article, f"Knipselkrant-{i}-{timestamp}")
    root = remove_duplicates(root)
    return minidom.parseString(ET.tostring(root, "utf-8")).toprettyxml(indent="   ")