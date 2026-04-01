"""Generate Nexus proximity search queries from a Pure persons export."""

import configparser
import pandas as pd
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.cfg'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

ROOT_DIR = Path(__file__).resolve().parent.parent


def _load_query_dataframe(input_file: Path) -> pd.DataFrame:
    if input_file.suffix.lower() == ".csv":
        df = pd.read_csv(input_file)
    else:
        df = pd.read_excel(input_file, sheet_name=0)

    org_col_candidates = [
        "Organisations > Organisational unit-0",
        "Organisational unit name",
        "Alle organisational units",
    ]
    name_col_candidates = [
        "Name variant > Known as name-1",
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
    result_df["name_variant"] = result_df["name_variant"].astype("string").str.strip()

    def extract_faculties(org_value: str) -> list[str]:
        if pd.isna(org_value):
            return []
        parts = [part.strip() for part in str(org_value).split("//")]
        faculties = [part for part in parts if part.startswith("Faculteit ")]
        if not faculties and str(org_value).startswith("Faculteit "):
            faculties = [str(org_value).strip()]
        return faculties

    result_df["org_unit"] = result_df["org_unit"].apply(extract_faculties)
    result_df = result_df.explode("org_unit")
    return result_df.dropna(subset=["org_unit", "name_variant"])


def build_queries(input_file: Path, output_file: Path, limit: int = 1300) -> None:
    name_nl = CONFIG["NAME"]["DUTCH"]
    name_en = CONFIG["NAME"]["ENGLISH"]
    org_part = f'("{name_en.title()}" OR "{name_nl.title()}")'

    df = _load_query_dataframe(input_file)
    groups = df.groupby("org_unit")

    with open(output_file, "w", encoding="utf-8") as f_out:
        for org_unit, group_df in groups:
            name_variants = group_df["name_variant"].dropna().unique().tolist()
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


if __name__ == "__main__":
    default_input = ROOT_DIR / "files" / "query.csv"
    if not default_input.exists():
        default_input = ROOT_DIR / "files" / "query.xls"
    build_queries(
        input_file=default_input,
        output_file=ROOT_DIR / "output" / "queries_per_faculty.txt",
    )
