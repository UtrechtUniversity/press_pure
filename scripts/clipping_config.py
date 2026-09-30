"""Institution-specific clipping roles, shared by AI, XML and JSON output."""

import configparser
from dataclasses import dataclass
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.cfg'


@dataclass(frozen=True)
class PersonRole:
    key: str
    uri: str
    label: str
    coverage_type: str


@dataclass
class ClippingSettings:
    roles: dict[str, PersonRole]
    default_role: str
    imported_group: str = ''
    imported_uri: str = ''
    free_keywords_group: str = 'keywordContainers'

    @classmethod
    def from_config(cls, config):
        roles = {}
        for section in config.sections():
            if not section.startswith('PURE_ROLE:'):
                continue
            key = section.split(':', 1)[1].strip().casefold()
            uri = config.get(section, 'URI', fallback='').strip()
            label = config.get(section, 'LABEL', fallback='').strip()
            coverage = config.get(section, 'COVERAGE_TYPE', fallback='').strip()
            if (not key or key in roles or not uri.startswith('/')
                    or any(c.isspace() for c in uri) or not label
                    or coverage not in ('Coverage', 'Contribution')):
                raise ValueError(f'Invalid role configuration [{section}]: require unique key, exact URI, LABEL and COVERAGE_TYPE (Coverage/Contribution).')
            roles[key] = PersonRole(key, uri, label, coverage)
        default = config.get('PURE_CLIPPING', 'DEFAULT_RESEARCHER_ROLE', fallback='').strip().casefold()
        if not roles or default not in roles:
            raise ValueError('Configure [PURE_ROLE:<key>] sections and [PURE_CLIPPING] DEFAULT_RESEARCHER_ROLE; see configdummy.cfg.')
        enabled = config.getboolean('PURE_CLIPPING', 'IMPORTED_KEYWORD_GROUP_ENABLED', fallback=False)
        group = config.get('PURE_CLIPPING', 'IMPORTED_KEYWORD_GROUP_LOGICAL_NAME', fallback='').strip() if enabled else ''
        uri = config.get('PURE_CLIPPING', 'IMPORTED_KEYWORD_CLASSIFICATION_URI', fallback='').strip() if enabled else ''
        if enabled and (not group or not uri.startswith('/') or any(c.isspace() for c in uri)):
            raise ValueError('Enabled IMPORTED keyword group requires IMPORTED_KEYWORD_GROUP_LOGICAL_NAME and a valid IMPORTED_KEYWORD_CLASSIFICATION_URI')
        free_group = config.get('PURE_CLIPPING', 'FREE_KEYWORDS_GROUP_LOGICAL_NAME', fallback='keywordContainers').strip()
        if not free_group:
            raise ValueError('FREE_KEYWORDS_GROUP_LOGICAL_NAME must not be empty')
        return cls(roles, default, group, uri, free_group)

    def marker_group(self):
        if not self.imported_group:
            return None
        return {
            'typeDiscriminator': 'ClassificationsKeywordGroup',
            'logicalName': self.imported_group,
            'classifications': [{'uri': self.imported_uri}],
        }

    def resolve_role(self, value):
        normalized = value.strip().casefold() if isinstance(value, str) else ''
        if normalized in self.roles:
            return self.roles[normalized]
        # Support display labels and legacy research-cited spelling without inventing URIs.
        for role in self.roles.values():
            if normalized == role.label.casefold() or (
                normalized == 'research cited' and role.key == 'researchcited'
            ):
                return role
        if value is not None:
            logger.warning('Unconfigured researcher role %r; using %s', value, self.default_role)
        return self.roles[self.default_role]

    def normalize_article(self, article):
        role = self.resolve_role(article.get('researcher_role'))
        article['researcher_role'] = role.key
        article['media_type'] = role.coverage_type
        return role


def load_settings():
    config = configparser.ConfigParser(interpolation=None)
    config.read(CONFIG_PATH)
    return ClippingSettings.from_config(config)


def vocabulary_get(session, base_url, api_key, endpoint, field):
    """Read a vocabulary with bounded requests; fail closed before uploads."""
    import requests
    try:
        response = session.get(
            f'{base_url.rstrip("/")}/pressmedia/{endpoint}',
            headers={'Accept': 'application/json', 'api-key': api_key},
            timeout=(5, 30),
        )
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as error:
        raise ValueError(f'Pure vocabulary validation failed for {endpoint}: {error}') from error
    if not isinstance(data, dict) or not isinstance(data.get(field), list):
        raise ValueError(f'Unexpected Pure vocabulary response for {endpoint}: expected {field}')
    return data[field]


def validate_vocabularies(settings, session, base_url, api_key, *, uses_free_keywords=False):
    allowed = vocabulary_get(session, base_url, api_key,
                             'allowed-media-coverages-persons-roles', 'classifications')
    uris = {item.get('uri') for item in allowed}
    invalid = [role.uri for role in settings.roles.values() if role.uri not in uris]
    if invalid:
        raise ValueError(f'Configured person role URIs are not allowed by Pure: {invalid}')
    if not settings.imported_group and not uses_free_keywords:
        return
    configurations = vocabulary_get(session, base_url, api_key,
                                    'allowed-keyword-group-configurations', 'configurations')
    groups = {item.get('logicalName'): item for item in configurations}

    def require_group(name, group_type):
        group = groups.get(name)
        if not group or group.get('keywordGroupType') != group_type:
            raise ValueError(f'Pure keyword group {name!r} must exist with type {group_type}')
        return group

    if uses_free_keywords:
        require_group(settings.free_keywords_group, 'FreeKeywordsKeywordGroup')
    if settings.imported_group:
        group = require_group(settings.imported_group, 'ClassificationsKeywordGroup')
        group_id = group.get('pureId')
        if not isinstance(group_id, int):
            raise ValueError('Pure keyword group configuration has no integer pureId')
        allowed = vocabulary_get(session, base_url, api_key,
                                 f'allowed-keyword-group-configurations/{group_id}/classifications',
                                 'classifications')
        if settings.imported_uri not in {item.get('uri') for item in allowed}:
            raise ValueError(f'Pure keyword classification is not allowed: {settings.imported_uri}')
