# Open Library Data Pipeline

This project builds a **Python-based data pipeline** which queries the **Open Library Search API** and exports a **normalized relational CSV model** (`authors.csv`, `books.csv`, `book_subjects.csv`). It is **idempotent**: running it multiple times (with the same or different search terms) **won’t create duplicates**, and it preserves referential integrity.

---

## Quick Start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# First run
python books_pipeline.py --search "subject:horses" --limit 20 --log-level INFO

# Second run (overlapping results expected)
python books_pipeline.py --search "subject:equine" --limit 20 --log-level INFO
```

This will create/update three CSVs in the working directory:
- `authors.csv`
- `books.csv`
- `book_subjects.csv`

> **Note:** The sample CSVs in this repository were generated from a small curated set to demonstrate idempotency; your live runs will pull from the Open Library API.

---

## Design & Data Model

### Tables / CSVs

- **authors.csv**
  - `author_id` (PK, auto-increment, stable across runs)
  - `author_key` (Open Library author key, e.g., `OL25123A`) — **unique constraint for dedup**
  - `name`
  - `book_count` (maintained/updated as books are added)

- **books.csv**
  - `book_id` (PK, auto-increment, stable across runs)
  - `handle` (slug from title, lowercase + hyphens)
  - `title`
  - `author_id` (FK → authors.author_id)
  - `author_key` (OL author key, reference value only)
  - `vendor` (constant `Open Library`)
  - `price` (see pricing rule below)
  - `first_publish_year`
  - `edition_count`
  - `url` (Open Library work page, e.g. `https://openlibrary.org{{work_key}}`)

- **book_subjects.csv**
  - `book_id` (FK → books.book_id)
  - `subject` (1 row per subject)

### Pricing Rule

- **Price = $10 + $2 × (decades old) + $1 × (edition_count)**, **capped at $50**.
  - *Decades old* is computed from `first_publish_year` to the current UTC year.
  - Missing `first_publish_year` is treated as **0 decades** (i.e., no age premium).

---

## Idempotency & Deduplication Strategy

1. **Load existing CSVs** at startup (if present).
2. **Authors**: Use **`author_key`** as the canonical unique identifier. Before inserting, check the in-memory index (`author_by_key`); insert only if absent.
3. **Books**:
   - Primary key: **Open Library Work `key`** (e.g., `/works/OL23197W`). We store it in `books.url` as `https://openlibrary.org{{work_key}}`.
   - Fallback: **(`title`, `author_key`)** if Work key is missing.
   - Maintain an in-memory index:
     - `book_by_work` (maps `/works/...` → book)
     - `fallback_index` maps (`title`, `author_key`) → book
4. **IDs**: `author_id` and `book_id` are **auto-increment**. We compute `next_id = max(existing_id) + 1` to keep IDs consistent across runs.
5. **Referential Integrity**: Every `books.author_id` references an existing `authors.author_id`. We set `author_id` after author upsert and update `authors.book_count` when a new book is saved.
6. **Subjects**: For each book, add unique `(book_id, subject)` pairs (avoid duplicates using a set).

**Why Work Keys?** The Search API returns **Works** by default (collections of editions). Work keys are the **stable, canonical identifiers** for the intellectual work and dedupe reliably across searches.  
If a Work key is unavailable, falling back to (`title`, `author_key`) is a practical compromise.

---

## Error Handling & Logging

- Uses Python’s `logging` with `--log-level` controlling verbosity.
- Logs API calls, file I/O, dedup decisions, and skips due to missing critical fields.
- Gracefully handles network failures (returns empty result set and continues).

---

## CLI

```bash
python books_pipeline.py --search "subject:horses" --limit 20 --log-level INFO
```

- `--search` (default: `subject:horses`)
- `--limit`  (default: `20`)
- `--log-level` (`DEBUG` | `INFO` | `WARNING` | `ERROR`; default `INFO`)

---

## Assumptions & Ambiguities (and Rationale)

- **Critical fields**: A record **must** have `title`, `author_key` (first), and a **Work key** (from `key` or `work_key[0]`). Otherwise, it’s logged and **skipped**.  
  *Rationale*: Ensures we can maintain dedup, referential integrity, and a valid `url`.
- **Multiple Authors**: We use **the first author** (`author_key[0]` / `author_name[0]`) for `books.author_id`.  
  *Rationale*: Keeps schema simple (1 author per book) while still deterministic.
- **Slug generation**: Minimal slugify is implemented inline to avoid extra deps; it lowercases, removes apostrophes, replaces non-alnum with hyphens, and collapses repeats.
- **Price capping**: Hard cap at **$50.00** as specified; formatted to 2 decimals.
- **Existing malformed rows**: We don’t rewrite old malformed data; we log and continue.
- **Current Year**: For pricing, we use **UTC year** from the runtime environment.

---

## Example Output

See the included sample CSVs which demonstrate overlapping results across two runs (`subject:horses` then `subject:equine`) without duplicates. The `authors.book_count` rolls up counts as new books come in, and `book_subjects.csv` has one row per subject per book.

---

## Dependencies

- Python 3.10+
- `requests`

Install with:
```bash
pip install -r requirements.txt
```

---

## Testing Idempotency

Run the pipeline twice with overlapping queries (e.g., `"subject:horses"` then `"subject:equine"`). You should see a summary similar to:

```
Pipeline Summary:
- Fetched: 20 books from API
- Authors: 5 new, 10 existing (deduplicated)
- Books: 18 new, 2 duplicates skipped
- Skipped: 0 records with missing critical fields
- Subjects: 45 subject associations created
```

---

## Notes on Open Library API

- Search endpoint: `https://openlibrary.org/search.json?q=...&limit=...`
- Work-level fields commonly used: `key`, `title`, `author_key`, `author_name`, `first_publish_year`, `edition_count`, `subject`

Please be a good API citizen and include a custom `User-Agent` header with contact info when making requests.

---

## Project Structure

```
.
├── books_pipeline.py
├── requirements.txt
├── README.md
├── authors.csv
├── books.csv
└── book_subjects.csv
```

---

## License

MIT
