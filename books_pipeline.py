#!/usr/bin/env python3
# (contents unchanged from previous attempt)
# See above cell for full script; rewriting here completely.

#!/usr/bin/env python3
"""
Open Library Data Pipeline
--------------------------
Fetches books from the Open Library Search API, normalizes into a relational
CSV model (authors, books, book_subjects), and is idempotent across runs.

Usage:
    python books_pipeline.py --search "subject:horses" --limit 20 --log-level INFO
"""

import argparse
import csv
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

# Constants
API_URL = "https://openlibrary.org/search.json"
OUTPUT_DIR = Path(".")
AUTHORS_CSV = OUTPUT_DIR / "authors.csv"
BOOKS_CSV = OUTPUT_DIR / "books.csv"
BOOK_SUBJECTS_CSV = OUTPUT_DIR / "book_subjects.csv"
VENDOR = "Open Library"


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def slugify_handle(title: str) -> str:
    """Basic slugify: lowercase, alnum+hyphen, collapse spaces -> hyphens."""
    import re

    s = title.strip().lower()
    s = re.sub(r"[']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "untitled"


def ensure_headers(path: Path, headers: List[str]):
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def read_csv_to_dicts(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_dicts_to_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def get_next_id(existing_rows: List[Dict[str, str]], id_field: str) -> int:
    max_id = 0
    for r in existing_rows:
        try:
            max_id = max(max_id, int(r[id_field]))
        except Exception:
            continue
    return max_id + 1


def compute_price(first_publish_year: Optional[int], edition_count: int) -> float:
    base = 10.0
    year = datetime.utcnow().year
    decades_old = 0
    if first_publish_year:
        decades_old = max((year - int(first_publish_year)) // 10, 0)
    price = base + (2.0 * decades_old) + (1.0 * edition_count)
    return float(f"{min(price, 50.0):.2f}")


def fetch_open_library(search: str, limit: int) -> Dict:
    params = {"q": search, "limit": limit}
    url = f"{API_URL}?{urlencode(params)}"
    headers = {"User-Agent": "open-library-pipeline/1.0 (contact: you@example.com)"}
    logging.info("GET %s", url)
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logging.error("API request failed: %s", e)
        return {"docs": []}


def select_work_key(doc: Dict) -> Optional[str]:
    key = doc.get("key")
    if isinstance(key, str) and key.startswith("/works/"):
        return key
    wk = doc.get("work_key")
    if isinstance(wk, list) and wk:
        for k in wk:
            if isinstance(k, str) and k.startswith("/works/"):
                return k
    return None


def extract_subjects(doc: Dict) -> List[str]:
    subs = doc.get("subject") or []
    if not isinstance(subs, list):
        return []
    return [str(s).strip() for s in subs if str(s).strip()]


def get_primary_author(doc: Dict) -> Tuple[Optional[str], Optional[str]]:
    akeys = doc.get("author_key") or []
    anames = doc.get("author_name") or []
    akey = akeys[0] if isinstance(akeys, list) and akeys else None
    aname = anames[0] if isinstance(anames, list) and anames else None
    return akey, aname


def load_state():
    authors = read_csv_to_dicts(AUTHORS_CSV)
    books = read_csv_to_dicts(BOOKS_CSV)
    book_subjects = read_csv_to_dicts(BOOK_SUBJECTS_CSV)

    author_by_key = {a["author_key"]: a for a in authors if a.get("author_key")}
    book_by_work = {b.get("url", "").replace("https://openlibrary.org", ""): b for b in books if b.get("url")}
    fallback_index = {(b.get("title"), b.get("author_key")): b for b in books if b.get("title") and b.get("author_key")}

    return authors, books, book_subjects, author_by_key, book_by_work, fallback_index


def save_state(authors, books, book_subjects):
    write_dicts_to_csv(
        AUTHORS_CSV, ["author_id", "author_key", "name", "book_count"], authors
    )
    write_dicts_to_csv(
        BOOKS_CSV,
        [
            "book_id",
            "handle",
            "title",
            "author_id",
            "author_key",
            "vendor",
            "price",
            "first_publish_year",
            "edition_count",
            "url",
        ],
        books,
    )
    write_dicts_to_csv(BOOK_SUBJECTS_CSV, ["book_id", "subject"], book_subjects)


def main():
    parser = argparse.ArgumentParser(description="Open Library Data Pipeline")
    parser.add_argument("--search", default="subject:horses", help="Search term, e.g., 'subject:horses'")
    parser.add_argument("--limit", type=int, default=20, help="Number of books to fetch")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    setup_logging(args.log_level)

    ensure_headers(AUTHORS_CSV, ["author_id", "author_key", "name", "book_count"])
    ensure_headers(
        BOOKS_CSV,
        [
            "book_id",
            "handle",
            "title",
            "author_id",
            "author_key",
            "vendor",
            "price",
            "first_publish_year",
            "edition_count",
            "url",
        ],
    )
    ensure_headers(BOOK_SUBJECTS_CSV, ["book_id", "subject"])

    authors, books, book_subjects, author_by_key, book_by_work, fallback_index = load_state()

    fetched = 0
    new_authors = 0
    existing_authors = 0
    new_books = 0
    duplicate_books = 0
    skipped_records = 0
    new_subject_links = 0

    data = fetch_open_library(args.search, args.limit)
    docs = data.get("docs", [])
    fetched = len(docs)
    logging.info("Fetched %d docs", fetched)

    next_author_id = get_next_id(authors, "author_id")
    next_book_id = get_next_id(books, "book_id")

    existing_subject_rows = {(int(row["book_id"]), row["subject"]) for row in book_subjects if row.get("book_id") and row.get("subject")}

    for doc in docs:
        title = doc.get("title")
        work_key = select_work_key(doc)
        author_key, author_name = get_primary_author(doc)
        edition_count = int(doc.get("edition_count", 0) or 0)
        first_publish_year = doc.get("first_publish_year")
        subjects = extract_subjects(doc)

        if not title or not author_key or not work_key:
            skipped_records += 1
            logging.warning("Skipping doc due to missing critical fields (title=%r, author_key=%r, work_key=%r)", title, author_key, work_key)
            continue

        if author_key not in author_by_key:
            author_row = {
                "author_id": str(next_author_id),
                "author_key": author_key,
                "name": author_name or "",
                "book_count": "0",
            }
            authors.append(author_row)
            author_by_key[author_key] = author_row
            next_author_id += 1
            new_authors += 1
        else:
            existing_authors += 1

        author_id = int(author_by_key[author_key]["author_id"])

        existing_book = book_by_work.get(work_key) or fallback_index.get((title, author_key))
        if existing_book:
            duplicate_books += 1
            book_id = int(existing_book["book_id"])
        else:
            handle = slugify_handle(title)
            price = compute_price(first_publish_year, edition_count)
            url = f"https://openlibrary.org{work_key}"
            book_row = {
                "book_id": str(next_book_id),
                "handle": handle,
                "title": title,
                "author_id": str(author_id),
                "author_key": author_key,
                "vendor": VENDOR,
                "price": f"{price:.2f}",
                "first_publish_year": str(first_publish_year or ""),
                "edition_count": str(edition_count),
                "url": url,
            }
            books.append(book_row)
            book_by_work[work_key] = book_row
            fallback_index[(title, author_key)] = book_row
            cnt = int(author_by_key[author_key]["book_count"] or 0) + 1
            author_by_key[author_key]["book_count"] = str(cnt)
            book_id = next_book_id
            next_book_id += 1
            new_books += 1

        for s in subjects:
            if (book_id, s) not in existing_subject_rows:
                book_subjects.append({"book_id": str(book_id), "subject": s})
                existing_subject_rows.add((book_id, s))
                new_subject_links += 1

    save_state(authors, books, book_subjects)

    print("Pipeline Summary:")
    print(f"- Fetched: {fetched} books from API")
    print(f"- Authors: {new_authors} new, {existing_authors} existing (deduplicated)")
    print(f"- Books: {new_books} new, {duplicate_books} duplicates skipped")
    print(f"- Skipped: {skipped_records} records with missing critical fields")
    print(f"- Subjects: {new_subject_links} subject associations created")


if __name__ == "__main__":
    main()
