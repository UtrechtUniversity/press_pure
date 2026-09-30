# Pure Press Clipping Processor

Generate Nexis Newsdesk searches from Pure person exports, then process newsletter
clippings, match researchers, optionally enrich metadata with AI, and upload the
results to Pure. Each import also produces an XML export and logs.

This is the central guide for installation, configuration, and the complete
newsletter import workflow, including screenshots. Run all commands from the
repository root. Screenshots illustrate the existing Nexis Newsdesk and Outlook
workflow; interface labels may differ in your version.

## Contents

- [1. Install the application](#1-install-the-application)
- [2. Configure the application](#2-configure-the-application)
- [Required Pure API permissions](#required-pure-api-permissions)
- [Configure Pure roles and keywords](#configure-pure-roles-and-keywords)
- [3. Configure source and title exclusions](#3-configure-source-and-title-exclusions)
- [4. Export a person report from Pure](#4-export-a-person-report-from-pure)
- [5. Generate search queries](#5-generate-search-queries)
- [6. Create searches in Nexis Newsdesk](#6-create-searches-in-nexis-newsdesk)
- [7. Set up newsletters](#7-set-up-newsletters)
- [8. Save newsletters](#8-save-newsletters)
- [9. Import press clippings](#9-import-press-clippings)
- [Directory structure](#directory-structure)
- [Tests and contributions](#tests-and-contributions)

## 1. Install the application

Clone the repository and open its root directory:

```bash
git clone https://github.com/UtrechtUniversity/press_pure.git
cd press_pure
```

Use Python 3.12 on Linux (the environment used for local verification). Run all
commands from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
mkdir -p files knipsel output logs
```

The importer requires the `nl_NL.UTF-8` system locale for date parsing. Check it
before importing:

```bash
python -c 'import locale; print(locale.setlocale(locale.LC_TIME, "nl_NL.UTF-8"))'
```

If this reports an unsupported locale, install/enable the Dutch UTF-8 locale
using your Linux distribution's locale tools and rerun the check. A virtual
environment does not install system locales. At startup the importer also asks
NLTK to download `punkt` and `stopwords`; the first run needs access to those
resources.

Optional PDF archiving uses WeasyPrint and its system libraries. The platform
package examples are listed in `scripts/pdf_archiver.py`. PDF archiving defaults
to off; enable it by adding `[PDF]` with `DOWNLOAD = true` to `config.cfg`.

## 2. Configure the application

For a new installation, copy `configdummy.cfg` to `config.cfg`, then edit the
copy. For an existing installation, merge the required settings into your
current config without replacing its credentials. `config.cfg` is ignored by Git.

- Under `[CREDENTIALS]`, set `APIKEY_CRUD` and `BASEURL_CRUD` for your Pure
  installation, with a trailing slash on the URL. Keep the legacy `APIKEY` and
  `BASEURL` entries: the code still reads them at startup, although the current
  import pipeline uses the CRUD key and URL for its Pure requests. The template's
  Utrecht URLs are examples. Configure the API access described below.
- Set `[NAME]` to your institution's Dutch and English names. Review `[FILTERS]`,
  `[SOURCE_MAP]`, and `[WORKFLOW STATUS]` for your institution. The filename-to-faculty
  map in `scripts/knipselkrant.py` also contains Utrecht-specific defaults.
- Under `[AI]`, set `AI = false` to disable enrichment. To enable it, set `AI = true`,
  choose `PROVIDER = openai` or `mistral`, configure `MODEL`, and supply the matching
  `OPENAI_API` or `MISTRAL_API` credential. The template enables AI by default.
- `GOOGLE_API` and `GOOGLE_CX` are used by the optional Google URL-search fallback.
- Configure `[PURE_CLIPPING]` and at least one `[PURE_ROLE:<key>]` for your
  installation, as described below. These are required even with AI disabled.

### Required Pure API permissions

Ask your Pure administrator to configure `APIKEY_CRUD` for the target environment.
Endpoint access, the associated user's privileges, and the access definition's
content and field permissions all matter. Enabling an endpoint alone does not
grant access to every record or permission to write it. See Pure's documentation
on [API keys](https://butler.elsevierpure.com/ws/api/documentation/user-guide/api-keys.html)
and [authorization](https://tourolaw.elsevierpure.com/ws/api/documentation/user-guide/authorization.html).

The following requirements come from the current import pipeline. Paths are
relative to `BASEURL_CRUD`; requests authenticate with the `api-key` header.

| Method and endpoint | Required access and purpose |
| --- | --- |
| `POST /persons/search` | Read/search persons for researcher matching. This POST is a search, not a person write. |
| `GET /organizations/{uuid}` | Read organisations for affiliations and the managing organisation. |
| `POST /pressmedia/search` | Read/search existing press/media records for duplicate detection. This POST is also read-only. |
| `GET /pressmedia/allowed-media-coverages-persons-roles` | Read allowed person roles; required before each nonempty upload batch. |
| `GET /pressmedia/allowed-keyword-group-configurations` | Read keyword groups when free keywords or the imported marker are used. |
| `GET /pressmedia/allowed-keyword-group-configurations/{pureId}/classifications` | Read classifications when the imported marker is enabled. |
| `PUT /pressmedia` | Create press/media records with the configured content and workflow status. |

Configure **read access for persons and organisations**, and **read and write
access for press/media**, including the fields used by the importer. The Pure
user attached to the key must be able to create press/media for the relevant
organisations and use the configured workflow statuses. Have the administrator
check the role names in your installation; this project does not prescribe a
universal Pure administrator role. Person/organisation writes and deletion are
not used by this pipeline.

Field and content access must include:

- Persons: UUID, name and name variants, identifiers (including `Employee ID`),
  and staff organisation associations with their periods and organisation UUIDs.
- Organisations: UUID, type, and name, including the `en_GB` values read by the code.
- Existing press/media: titles and media coverages, including linked person
  identifiers/UUIDs and dates, for duplicate detection. Content filters must
  include the existing records against which imports should be checked.
- New press/media: title, type, visibility, descriptions, managing organisation,
  workflow, and media coverages with their metadata, person roles and organisation
  links; keyword groups and country when included in the payload.

The optional `get_media_item()` helper additionally uses
`GET /pressmedia/{uuid}`; it is not called by the normal import pipeline.
Generating queries from a saved person export makes no Pure API requests.
Creating/exporting the person report in Pure requires the operator's own reporting
access separately from the import API key.

Validate read/search access and vocabulary responses in the target environment,
then check write access with a small staging import. The application's vocabulary
preflight does not verify every user privilege or writable field, so a successful
preflight alone does not establish that uploads will succeed.

<a id="installation-specific-pure-roles-and-keywords"></a>

### Configure Pure roles and keywords

Existing installations must add `[PURE_CLIPPING]` and `[PURE_ROLE:<key>]`
sections to `config.cfg`; see `configdummy.cfg` for a single-role example.
There is deliberately no assumed universal default role. Each role requires
an exact `URI`, display `LABEL`, and `COVERAGE_TYPE` (`Coverage` or `Contribution`).
`DEFAULT_RESEARCHER_ROLE` must reference a configured key. The same default
applies to missing/invalid AI choices, AI failure, and imports with AI disabled.
The existing researchcited role uses `Coverage`; author, interviewee, and
participant use `Contribution`. JSON and XML share the same normalization.

Discover the target installation's settings using these GET endpoints relative
to `BASEURL_CRUD`, authenticated with the configured Pure API key:

- `pressmedia/allowed-media-coverages-persons-roles`
- `pressmedia/allowed-keyword-group-configurations`
- `pressmedia/allowed-keyword-group-configurations/{pureId}/classifications`

The import marker is disabled by default. To preserve it on an installation
that supports it, set `IMPORTED_KEYWORD_GROUP_ENABLED = true` and configure
`IMPORTED_KEYWORD_GROUP_LOGICAL_NAME` and `IMPORTED_KEYWORD_CLASSIFICATION_URI`.
The previous values were `/dk/atira/pure/clippings/keywords/imported` and
`/dk/atira/pure/clippings/keywords/imported/true`; use them only if discovered
in that installation. Free keywords remain independent, using
`FREE_KEYWORDS_GROUP_LOGICAL_NAME` (default `keywordContainers`). Display labels
such as `Keywords` are not substitutes for logical names.

Before each nonempty upload batch, the application checks roles and the keyword
groups it uses against the server. A mismatch or failed validation stops the
batch before writes. Payload generation and tests can run offline.

## 3. Configure source and title exclusions

Edit `files/Filter_media.xlsx` to exclude sources and titles. Its `Media name`
sheet lists exact source names in the first column; its `Media title` sheet lists
title substrings in the first column. Matching is case-sensitive. If the workbook
is missing, the importer warns and continues without these exclusions.

## 4. Export a person report from Pure

Use `files/UU leerstoel report Persons CURRENT (1).json` as the basis for a
person report in Pure. Adjust the Academic, Type, and Media Type filters as
appropriate for your report, choose *Apply* and *Save*, run it, and export the results to `files/query.csv`, `files/query.xlsx`, or
`files/query.xls`. The report definition itself is not query input.

Excel input uses the first worksheet; there is no intermediate CSV conversion.
CSV input must be UTF-8 (a BOM is supported), comma- or semicolon-separated.
Supported name columns are `Name variant > Known as name-1`, `Known as name`,
or `Name`, in that preference order. Organisation columns are
`Organisations > Organisational unit-0`, `Organisational unit name`, or
`Alle organisational units`. Names such as `van Dijk, Jan` are automatically
converted to `Jan van Dijk`; manual Excel name editing is unnecessary.

## 5. Generate search queries

```bash
python scripts/build_nexus_query.py
```

The default output is `output/queries_per_faculty.txt`.

The script looks for `files/query.csv`, `files/query.xlsx`, then `files/query.xls`
in that order. It prints the selected file and warns if several exist. Select a
specific file and a separate output explicitly when testing a subset:

```bash
python scripts/build_nexus_query.py --input files/query.xlsx --output output/query_subset.txt
```

Faculty matching splits `//` hierarchies and matches `faculteit`, `faculty`,
and `school` case-insensitively. Configure literal `FACULTY_TERMS` and
`FACULTY_PREFIXES` under `[QUERY]` in `config.cfg`; for example,
`FACULTY_PREFIXES = TS`. Use `[FACULTY_ALIASES]` for exact case-insensitive
mappings, such as `TS Economics and Management = Tilburg School of Economics and Management`.
Aliases take precedence for their segment. Researchers without a recognized
organisation are retained in `Unmatched organisations`, with warnings listing
the unmatched values. This group needs review; it is not a faculty classification.
An input with no usable names fails before overwriting existing query output.

## 6. Create searches in Nexis Newsdesk

Open Nexis Newsdesk and use the queries from `output/queries_per_faculty.txt`
to create searches.

<img src="docs/press_pure_README/media/image1.png" width="601" alt="Entering a search query in Nexis Newsdesk" />

Set the content types and dates according to your institution's preferences.
Click the magnifying glass to search, then *Save changes* to save the search.
Repeat for each faculty or institute, for example:

<img src="docs/press_pure_README/media/image2.png" width="147" alt="Saved searches grouped by faculty" />

Set the filters on the left according to your preferences. Choose *Additional
filters* at the bottom to access *Source category*. The Utrecht University
example uses the settings shown below:

<img src="docs/press_pure_README/media/image3.png" width="107" alt="Deduplication settings" /> <img src="docs/press_pure_README/media/image4.png" width="138" alt="Media type selection" /> <img src="docs/press_pure_README/media/image5.png" width="107" alt="Noise filters" /> <img src="docs/press_pure_README/media/image6.png" width="93" alt="Source category selection" />

## 7. Set up newsletters

After creating the searches, choose *Share* and *New newsletter*. Add the
appropriate searches and create a separate newsletter for each faculty or institute.

<img src="docs/press_pure_README/media/image7.png" width="601" alt="Creating a newsletter and adding searches" />

Click *Customize*, select green under *Highlight keywords*, and use the other
settings shown below. The parser uses the green highlighting to identify person
names.

<img src="docs/press_pure_README/media/image8.png" width="377" alt="Newsletter layout with green keyword highlighting" />

To test the newsletter, click *Edit & send now* and then *Send test*. Set a
*schedule* and add yourself as a *recipient*. You will receive newsletters in
your mailbox at the scheduled times.

## 8. Save newsletters

Export the received newsletters from Outlook 365 as `.eml` files:

<img src="docs/press_pure_README/media/image9.png" width="377" alt="Downloading a newsletter as EML in Outlook" />

Place the files under `knipsel/`. The current importer also accepts `.html` and
searches all subdirectories. Move files outside `knipsel/` when they should no
longer be scanned; a subfolder named `archive` is still included.

## 9. Import press clippings

Check the configured Pure target before running this command. It processes all
matching input files, generates XML, and uploads eligible clippings through the
Pure API. There is no command-line dry-run mode. For an initial live check, use
staging credentials and keep only a small input sample under `knipsel/`.

```bash
python scripts/knipselkrant.py
```

The importer parses clippings, matches persons, checks duplicates, optionally
enriches metadata with AI, and uploads eligible records. Review the results:

| File or directory | Contents |
| --- | --- |
| `logs/press_import_*.log` | Parsing, filtering, duplicate detection, and upload results |
| `output/press_clippings_*.xml` | XML export of processed clippings |
| `logs/processed_articles.xlsx` | Debug export, overwritten each run |
| `output/pdf/` | Optional local PDF archive |

XML generation alone does not confirm a successful upload. Check the upload
results in the log file.

## Directory structure

```text
press_pure/
├── README.md                # Central illustrated guide
├── scripts/                 # Python entry points and helper modules
├── files/
│   ├── Filter_media.xlsx
│   └── query.csv             # Or query.xlsx / query.xls
├── knipsel/                  # Newsletter .eml / .html files; searched recursively
├── output/                   # Queries, XML exports, optional PDF archive
├── logs/                     # Run logs and processed_articles.xlsx
├── tests/                    # Offline regression tests
├── docs/press_pure_README/media/  # The nine screenshots in this guide
├── configdummy.cfg           # Configuration template
└── config.cfg                # Local configuration and credentials
```

## Tests and contributions

Run the offline regression suite before submitting changes:

```bash
python -m unittest discover -s tests -v
```

Update this README and `configdummy.cfg` when the workflow changes. Commit the
implementation, tests, configuration template, and matching documentation
together. Keep credentials, person exports, newsletter inputs, and generated
logs/output out of the change. Please submit pull requests or open issues.
