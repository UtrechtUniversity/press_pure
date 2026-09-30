"""Generate Nexus proximity search queries from a Pure persons export."""

import configparser
import argparse
import re
import warnings
from pathlib import Path

import pandas as pd

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

ROOT_DIR = Path(__file__).resolve().parent.parent
SUPPORTED_INPUT_SUFFIXES = ('.csv', '.xlsx', '.xls')
MORE_NAMES_PATTERN = re.compile(r"\s*\(\+\d+\s+more\)\s*", re.IGNORECASE)
LAST_NAME_PREFIXES = {
    "aan", "af", "al", "ap", "auf", "ben", "bin", "da", "dal", "de", "del",
    "della", "den", "der", "di", "dos", "du", "el", "la", "le", "ten", "ter",
    "van", "vanden", "von",
}


def _clean_name(name: str) -> str:
    """Remove Pure UI suffixes such as '(+2 more)' from a person name."""
    if pd.isna(name):
        return name
    return " ".join(MORE_NAMES_PATTERN.sub(" ", str(name)).split()).strip()


def _format_name_for_newsdesk(name: str) -> str:
    """Return names in Newsdesk's preferred first-name last-name order."""
    if pd.isna(name):
        return name

    name = _clean_name(name)
    if "," not in name:
        return name

    last_name, first_names = [part.strip() for part in name.split(",", 1)]
    if not last_name or not first_names:
        return name

    return f"{first_names} {last_name}".strip()


def _last_name_sort_key(name: str) -> tuple[str, str]:
    """Sort a display name by surname, keeping prefixes with the surname."""
    cleaned = _clean_name(name).casefold()
    if "," in cleaned:
        last_name = cleaned.split(",", 1)[0].strip()
        return last_name, cleaned

    parts = cleaned.split()
    if not parts:
        return "", cleaned

    start = len(parts) - 1
    while start > 0 and parts[start - 1] in LAST_NAME_PREFIXES:
        start -= 1
    return " ".join(parts[start:]), cleaned


def _load_query_dataframe(input_file: Path) -> pd.DataFrame:
    if input_file.suffix.lower() not in SUPPORTED_INPUT_SUFFIXES:
        raise ValueError('Supported input formats: .csv, .xlsx, .xls')
    if input_file.suffix.lower() == ".csv":
        df = pd.read_csv(input_file, sep=";", encoding="utf-8-sig")
        if len(df.columns) == 1:
            df = pd.read_csv(input_file, sep=",", encoding="utf-8-sig")
    else:
        df = pd.read_excel(input_file, sheet_name=0)

    org_col_candidates = [
        "Organisations > Organisational unit-0",
        "Organisational unit name",
        "Alle organisational units",
    ]
    name_col_candidates = [
        "Name variant > Known as name-1",
        "Known as name",
        "Name",
    ]

    org_col = next((col for col in org_col_candidates if col in df.columns), None)
    name_col = next((col for col in name_col_candidates if col in df.columns), None)

    if org_col is None or name_col is None:
        raise ValueError(
            "Onbekende query-indeling. Gevonden kolommen: "
            f"{list(df.columns)}"
        )

    result_df = pd.DataFrame()
    result_df["org_unit"] = df[org_col]
    if "Alle organisational units" in df.columns:
        result_df["org_unit"] = result_df["org_unit"].fillna(df["Alle organisational units"])
    result_df["name_variant"] = df[name_col]
    result_df["org_unit"] = result_df["org_unit"].astype("string").str.strip()
    result_df["name_variant"] = (
        result_df["name_variant"]
        .astype("string")
        .str.strip()
        .apply(_format_name_for_newsdesk)
    )

    result_df = result_df.dropna(subset=['name_variant'])
    result_df = result_df[result_df['name_variant'].str.strip().ne('')].copy()
    if result_df.empty:
        raise ValueError('No usable person names found; existing query output has been preserved.')
    terms = [term.strip() for term in CONFIG.get('QUERY', 'FACULTY_TERMS', fallback='faculteit,faculty,school').split(',') if term.strip()]
    prefixes = [term.strip() for term in CONFIG.get('QUERY', 'FACULTY_PREFIXES', fallback='').split(',') if term.strip()]
    aliases = {key.strip().casefold(): value.strip() for key, value in CONFIG.items('FACULTY_ALIASES')} if CONFIG.has_section('FACULTY_ALIASES') else {}
    if any(not value for value in aliases.values()):
        raise ValueError('Faculty alias targets must not be empty')
    unmatched = []
    matched_rows = 0

    def extract_faculties(org_value: str) -> list[str]:
        nonlocal matched_rows
        parts = [] if pd.isna(org_value) else [part.strip() for part in str(org_value).split('//') if part.strip()]
        faculties = {}
        for part in parts:
            canonical = aliases.get(part.casefold())
            if canonical is None and (
                any(re.search(r'(?<!\w)' + re.escape(term) + r'(?!\w)', part, re.IGNORECASE) for term in terms)
                or any(re.match(re.escape(prefix) + r'(?!\w)', part, re.IGNORECASE) for prefix in prefixes)
            ):
                canonical = part
            if canonical:
                faculties.setdefault(canonical.casefold(), canonical)
        if faculties:
            matched_rows += 1
            return list(faculties.values())
        unmatched.append(str(org_value).strip() if parts else '(missing organisation)')
        return ['Unmatched organisations']

    result_df['org_unit'] = result_df['org_unit'].apply(extract_faculties)
    if unmatched:
        warnings.warn(f'{len(unmatched)} person row(s) have unmatched organisations; retained in "Unmatched organisations". Values: {sorted(set(unmatched))}', stacklevel=2)
    if not matched_rows:
        warnings.warn('No faculties recognized. Configure QUERY faculty terms/prefixes or FACULTY_ALIASES.', stacklevel=2)
    result_df = result_df.explode("org_unit")
    return result_df.drop_duplicates(subset=['org_unit', 'name_variant'])


def build_queries(input_file: Path, output_file: Path, limit: int = 1300) -> None:
    if limit < 1:
        raise ValueError('Query chunk limit must be at least 1')
    name_nl = CONFIG["NAME"]["DUTCH"]
    name_en = CONFIG["NAME"]["ENGLISH"]
    org_part = f'("{name_en.title()}" OR "{name_nl.title()}")'

    df = _load_query_dataframe(input_file)
    groups = df.groupby("org_unit")

    with open(output_file, "w", encoding="utf-8") as f_out:
        for org_unit, group_df in groups:
            name_variants = sorted(
                group_df["name_variant"].dropna().unique().tolist(),
                key=_last_name_sort_key,
            )
            chunks = [name_variants[i:i + limit] for i in range(0, len(name_variants), limit)]

            print(f"{org_unit}: {len(chunks)} chunk(s)")
            for chunk in chunks:
                query = (
                    f"{org_part} NEAR/50 ("
                    + " OR ".join(f'"{name}"' for name in chunk)
                    + ")"
                )
                f_out.write(f"faculty: {org_unit}\n{query}\n\n")

    print(f"Queries written to {output_file}")


def select_input_file(directory: Path, explicit: Path | None = None) -> Path:
    if explicit is not None:
        explicit = Path(explicit)
        if explicit.suffix.lower() not in SUPPORTED_INPUT_SUFFIXES:
            raise ValueError('Unsupported input format; use .csv, .xlsx or .xls')
        if not explicit.is_file():
            raise ValueError(f'Input file does not exist: {explicit}')
        return explicit
    candidates = [directory / f'query{suffix}' for suffix in SUPPORTED_INPUT_SUFFIXES]
    existing = [path for path in candidates if path.is_file()]
    if not existing:
        raise ValueError(f'No input found in {directory}; expected query.csv, query.xlsx or query.xls. Use --input PATH.')
    if len(existing) > 1:
        warnings.warn(f'Multiple query files found; using {existing[0]}. Use --input PATH to select another.', stacklevel=2)
    return existing[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, help='CSV, XLSX or XLS export (first worksheet)')
    parser.add_argument('--output', type=Path, default=ROOT_DIR / 'output' / 'queries_per_faculty.txt')
    args = parser.parse_args(argv)
    try:
        input_file = select_input_file(ROOT_DIR / 'files', args.input)
        print(f'Using input: {input_file}')
        build_queries(input_file, args.output)
    except (ValueError, OSError, ImportError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
