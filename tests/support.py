"""Public dummy configuration; tests never need an institution's credentials."""

import configparser


def app_config():
    config = configparser.ConfigParser(interpolation=None)
    config.read_dict({
        'CREDENTIALS': {
            'APIKEY': 'test', 'APIKEY_CRUD': 'test',
            'BASEURL': 'https://example.test/ws/api/524/',
            'BASEURL_CRUD': 'https://example.test/ws/api/',
            'GOOGLE_API': '', 'GOOGLE_CX': '',
        },
        'WORKFLOW STATUS': {'TEST': 'entryInProgress'},
        'NAME': {'DUTCH': 'test universiteit', 'ENGLISH': 'test university'},
        'AI': {'AI': 'false'},
        'PURE_CLIPPING': {'DEFAULT_RESEARCHER_ROLE': 'researchcited'},
        'PURE_ROLE:researchcited': {
            'URI': '/dk/atira/pure/clipping/roles/clipping/researchcited',
            'LABEL': 'Research cited', 'COVERAGE_TYPE': 'Coverage',
        },
    })
    return config


def read_dummy_config(parser, filenames, encoding=None):
    parser.read_dict({section: dict(app_config()[section]) for section in app_config().sections()})
    return []
