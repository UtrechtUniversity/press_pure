import configparser
import importlib
import json
from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from support import read_dummy_config

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))

ROLE = '/dk/atira/pure/clipping/roles/clipping/researchcited'


def configuration(multiple=False, marker=False):
    c = configparser.ConfigParser(interpolation=None)
    c.read_dict({
        'PURE_CLIPPING': {'DEFAULT_RESEARCHER_ROLE': 'researchcited',
                          'IMPORTED_KEYWORD_GROUP_ENABLED': str(marker)},
        'PURE_ROLE:researchcited': {'URI': ROLE, 'LABEL': 'Research cited',
                                  'COVERAGE_TYPE': 'Coverage'},
    })
    if multiple:
        c.read_dict({'PURE_ROLE:author': {
            'URI': '/dk/atira/pure/clipping/roles/clipping/author',
            'LABEL': 'Author', 'COVERAGE_TYPE': 'Contribution'}})
    if marker:
        c['PURE_CLIPPING'].update({
            'IMPORTED_KEYWORD_GROUP_LOGICAL_NAME': '/example/imported',
            'IMPORTED_KEYWORD_CLASSIFICATION_URI': '/example/imported/true'})
    return c


def article():
    return {'Media item title': 'Synthetic subset article', 'URL': '',
            'Datum': datetime(2026, 9, 1), 'Person_resolved': [
                ('1', 'person-1', 'Jan van Dijk', []),
                ('2', 'person-2', 'Anne Example', [])],
            'Medium_type': 'Web', 'media_type': 'Contribution',
            'researcher_role': 'participant', 'typerole': 'research',
            'article_degree': 'national', 'Media name': 'Test',
            'Faculty': 'TEST', 'keywords': [], 'goodfit': 'yes'}


class ClippingTests(unittest.TestCase):
    def setUp(self):
        reader = patch.object(configparser.ConfigParser, 'read', read_dummy_config)
        reader.start(); self.addCleanup(reader.stop)
        network = patch('requests.sessions.Session.request', side_effect=AssertionError('Unexpected live HTTP request in unit test'))
        network.start(); self.addCleanup(network.stop)

    def settings(self, **kwargs):
        self.assertIsNotNone(importlib.util.find_spec('clipping_config'),
                             'Shared clipping configuration is missing')
        module = importlib.import_module('clipping_config')
        return module.ClippingSettings.from_config(configuration(**kwargs))

    def test_single_role_enforced_at_json_boundary(self):
        settings = self.settings()
        import pure_functions
        for value in ('participant', 'research cited', None, [], {'bad': 1}):
            row = article(); row['researcher_role'] = value
            payload = pure_functions.build_payload_from_row(row, settings=settings)
            coverage = payload['mediaCoverages'][0]
            self.assertEqual('COVERAGE', coverage['coverageType'])
            self.assertEqual([ROLE, ROLE], [p['role']['uri'] for p in coverage['persons']])
            self.assertEqual('Research cited', coverage['persons'][0]['role']['term']['en_GB'])
            self.assertEqual('Contribution', row['media_type'])  # builder does not mutate input

    def test_multiple_roles_and_xml_use_same_resolution(self):
        settings = self.settings(multiple=True)
        row = article(); row['researcher_role'] = ' AUTHOR '
        settings.normalize_article(row)
        self.assertEqual(('author', 'Contribution'), (row['researcher_role'], row['media_type']))
        import xml_builder
        root = xml_builder.make_header()
        row['researcher_role'] = 'nonsense'
        xml_builder.make_single_clipping(root, row, 'test', settings=settings)
        ns = {'p': xml_builder.NAMESPACE}
        self.assertEqual('Coverage', root.find('.//p:mediaReference', ns).get('type'))
        self.assertEqual(['researchcited'] * 2, [e.text for e in root.findall('.//p:role', ns)])

    def test_complete_xml_uses_supplied_settings_snapshot(self):
        import inspect
        import xml_builder
        self.assertIn('settings', inspect.signature(xml_builder.build_xml).parameters)
        settings = self.settings(multiple=True)
        row = article(); row['researcher_role'] = 'author'
        with patch.object(xml_builder, 'load_settings', side_effect=AssertionError('Unexpected settings reload')):
            xml = xml_builder.build_xml([row], settings=settings)
        self.assertIn('>author<', xml)
        self.assertIn('type="Contribution"', xml)

    def test_invalid_configuration_rejected(self):
        self.settings()
        from clipping_config import ClippingSettings
        for c in (configparser.ConfigParser(), configuration()):
            if c.sections():
                c['PURE_CLIPPING']['DEFAULT_RESEARCHER_ROLE'] = 'absent'
            with self.assertRaises(ValueError):
                ClippingSettings.from_config(c)

    def test_ai_failure_and_invalid_output_use_default(self):
        settings = self.settings()
        import ai_functions
        for content in ('{"researcher_role":"participant"}', '{"researcher_role":null}', '[]', '{}'):
            from types import SimpleNamespace as NS
            response = NS(choices=[NS(message=NS(content=content))])
            with patch.object(ai_functions, 'get_ai_client') as client:
                client.return_value.chat.completions.create.return_value = response
                row = ai_functions.ai_getinfo(article(), settings=settings)
                self.assertEqual(('researchcited', 'Coverage'), (row['researcher_role'], row['media_type']))
        with patch.object(ai_functions, 'get_ai_client', side_effect=RuntimeError('offline')):
            row = ai_functions.ai_getinfo(article(), settings=settings)
            self.assertEqual('researchcited', row['researcher_role'])

    def test_role_preflight_blocks_writes(self):
        settings = self.settings()
        import pure_functions
        from requests import Response
        response = Response(); response.status_code = 200
        response._content = b'{"classifications":[]}'
        with patch.object(pure_functions.SESSION, 'get', return_value=response), \
             patch.object(pure_functions.SESSION, 'put') as put:
            with self.assertRaisesRegex(ValueError, 'role'):
                pure_functions.upload_processed_articles([article()], 'test', 'https://example.test/ws/api/', settings=settings)
            put.assert_not_called()

    def test_disabled_ai_uses_configured_role(self):
        settings = self.settings()
        # Import-time locale selection is unrelated to role behavior and varies by host.
        with patch('locale.setlocale'):
            import knipselkrant
        row = article(); row['Person'] = ['Jan van Dijk']; row['Keywords'] = []
        with patch.object(knipselkrant, 'AI', False), \
             patch.object(knipselkrant.pure_functions, 'resolve_persons', return_value=(row['Person_resolved'], [])), \
             patch.object(knipselkrant.pure_functions, 'check_duplicates', return_value=False):
            result, status = knipselkrant.process_article(row, settings=settings)
        self.assertEqual('ok', status)
        self.assertEqual(('researchcited', 'Coverage'), (result['researcher_role'], result['media_type']))

    def test_upload_subset_validates_once_then_sends_normalized_payloads(self):
        settings = self.settings()
        import pure_functions
        from requests import Response
        response = Response(); response.status_code = 200
        response._content = json.dumps({'classifications': [{'uri': ROLE}]}).encode()
        sent = []
        def send(url, **kwargs):
            self.assertEqual('https://example.test/ws/api/pressmedia', url)
            sent.append(json.loads(kwargs['data']))
            return response
        with patch.object(pure_functions.SESSION, 'get', return_value=response) as get, \
             patch.object(pure_functions.SESSION, 'put', side_effect=send):
            pure_functions.upload_processed_articles([article(), article(), article()], 'test', 'https://example.test/ws/api/', settings=settings)
        self.assertEqual(1, get.call_count)
        self.assertEqual(3, len(sent))
        for payload in sent:
            self.assertNotIn('keywordGroups', payload)
            self.assertEqual('COVERAGE', payload['mediaCoverages'][0]['coverageType'])
            self.assertEqual([ROLE, ROLE], [p['role']['uri'] for p in payload['mediaCoverages'][0]['persons']])

    def test_marker_and_free_keyword_combinations(self):
        import pure_functions
        for enabled in (False, True):
            settings = self.settings(marker=enabled)
            for keywords in ([], ['climate']):
                row = article(); row['keywords'] = keywords
                payload = pure_functions.build_payload_from_row(row, settings=settings)
                groups = payload.get('keywordGroups', [])
                self.assertEqual(int(enabled) + bool(keywords), len(groups))
                if enabled:
                    self.assertEqual('/example/imported', groups[0]['logicalName'])
                    self.assertEqual('/example/imported/true', groups[0]['classifications'][0]['uri'])
                if keywords:
                    self.assertEqual(['climate'], groups[-1]['keywords'][0]['freeKeywords'])
                if not enabled and not keywords:
                    self.assertNotIn('keywordGroups', payload)

    def test_incomplete_marker_configuration_rejected(self):
        self.settings()
        from clipping_config import ClippingSettings
        c = configuration(); c['PURE_CLIPPING']['IMPORTED_KEYWORD_GROUP_ENABLED'] = 'true'
        with self.assertRaisesRegex(ValueError, 'IMPORTED'):
            ClippingSettings.from_config(c)

    def test_keyword_vocabulary_validation(self):
        settings = self.settings(marker=True)
        from clipping_config import validate_vocabularies
        from requests import Response
        from unittest.mock import Mock
        data = {
            'allowed-media-coverages-persons-roles': {'classifications': [{'uri': ROLE}]},
            'allowed-keyword-group-configurations': {'configurations': [
                {'pureId': 12, 'logicalName': '/example/imported', 'keywordGroupType': 'ClassificationsKeywordGroup'},
                {'pureId': 13, 'logicalName': 'keywordContainers', 'keywordGroupType': 'FreeKeywordsKeywordGroup'}]},
            'allowed-keyword-group-configurations/12/classifications': {'classifications': [{'uri': '/example/imported/true'}]},
        }
        def get(url, **kwargs):
            response = Response(); response.status_code = 200
            response._content = json.dumps(data[url.split('/pressmedia/')[1]]).encode()
            return response
        session = Mock(); session.get.side_effect = get
        validate_vocabularies(settings, session, 'https://example.test', 'test', uses_free_keywords=True)
        for mutation in ('type', 'uri', 'missing'):
            with self.subTest(mutation=mutation):
                import copy
                original = copy.deepcopy(data)
                if mutation == 'type':
                    data['allowed-keyword-group-configurations']['configurations'][0]['keywordGroupType'] = 'FreeKeywordsKeywordGroup'
                elif mutation == 'uri':
                    data['allowed-keyword-group-configurations/12/classifications']['classifications'] = []
                else:
                    data['allowed-keyword-group-configurations']['configurations'] = []
                with self.assertRaises(ValueError):
                    validate_vocabularies(settings, session, 'https://example.test', 'test', uses_free_keywords=True)
                data = original


if __name__ == '__main__':
    unittest.main()
