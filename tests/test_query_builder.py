from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
import warnings
import configparser
from unittest.mock import patch
from support import app_config, read_dummy_config

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
with patch.object(configparser.ConfigParser, 'read', read_dummy_config):
    import build_nexus_query as query


class QueryTests(unittest.TestCase):
    def setUp(self):
        settings = patch.object(query, 'CONFIG', app_config())
        settings.start(); self.addCleanup(settings.stop)

    def test_xlsx_discovery_and_explicit_selection(self):
        self.assertTrue(hasattr(query, 'select_input_file'), 'Input discovery is missing')
        with TemporaryDirectory() as temp:
            directory = Path(temp)
            xlsx = directory / 'query.xlsx'; xlsx.touch()
            self.assertEqual(xlsx, query.select_input_file(directory))
            csv = directory / 'query.csv'; csv.touch()
            with self.assertWarnsRegex(UserWarning, '--input'):
                self.assertEqual(csv, query.select_input_file(directory))
            self.assertEqual(xlsx, query.select_input_file(directory, xlsx))
            xls = directory / 'query.xls'; xls.touch()
            self.assertEqual(xls, query.select_input_file(directory, xls))

    def test_missing_and_unsupported_input_fail_clearly(self):
        self.assertTrue(hasattr(query, 'select_input_file'), 'Input discovery is missing')
        with TemporaryDirectory() as temp:
            directory = Path(temp)
            with self.assertRaisesRegex(ValueError, 'xlsx'):
                query.select_input_file(directory)
            with self.assertRaisesRegex(ValueError, 'exist'):
                query.select_input_file(directory, directory / 'missing.xlsx')
            other = directory / 'query.txt'; other.touch()
            with self.assertRaisesRegex(ValueError, 'supported'):
                query.select_input_file(directory, other)

    def test_csv_and_xlsx_generate_equivalent_queries(self):
        frame = pd.DataFrame({'Name': ['van Dijk, Jan', 'Éxample, Anne'],
                              'Organisational unit name': ['Faculteit Science'] * 2})
        with TemporaryDirectory() as temp:
            directory = Path(temp)
            outputs = []
            for suffix, delimiter in (('csv', ','), ('csv', ';'), ('xlsx', None)):
                path = directory / ('query.' + suffix)
                if delimiter:
                    frame.to_csv(path, sep=delimiter, index=False, encoding='utf-8-sig')
                else:
                    frame.to_excel(path, index=False)
                output = directory / 'out.txt'
                with redirect_stdout(StringIO()):
                    query.build_queries(path, output)
                outputs.append(output.read_text())
            self.assertEqual(outputs[0], outputs[1]); self.assertEqual(outputs[0], outputs[2])
            self.assertIn('"Jan van Dijk"', outputs[0])
            self.assertIn('"Anne Éxample"', outputs[0])

    def test_faculty_matching_preserves_unmatched_people(self):
        frame = pd.DataFrame({'Name': ['Example, A', 'Example, B', 'Example, C', 'Example, D', 'Example, E'],
                              'Organisational unit name': [
                                  'school of Economics', 'Faculty of Science',
                                  'TS Economics and Management', 'Institute Something', None]})
        with TemporaryDirectory() as temp:
            path = Path(temp) / 'query.csv'; frame.to_csv(path, index=False)
            with warnings.catch_warnings(record=True) as reported:
                warnings.simplefilter('always')
                result = query._load_query_dataframe(path)
            self.assertEqual(5, len(result))
            self.assertEqual(['school of Economics', 'Faculty of Science'] + ['Unmatched organisations'] * 3,
                             result['org_unit'].tolist())
            self.assertTrue(any('3' in str(w.message) and 'unmatched' in str(w.message).lower() for w in reported))

    def test_configured_prefix_alias_and_hierarchy_deduplication(self):
        frame = pd.DataFrame({'Name': ['Example, A', 'Example, B'],
                              'Organisational unit name': [
                                  'University // TS Economics and Management // faculty of Science // TS Economics and Management',
                                  'ARTS Institute']})
        c = configparser.ConfigParser()
        c.read_dict({'QUERY': {'FACULTY_PREFIXES': 'TS'},
                     'FACULTY_ALIASES': {'TS Economics and Management': 'Economics'}})
        with TemporaryDirectory() as temp:
            path = Path(temp) / 'query.xlsx'; frame.to_excel(path, index=False)
            with patch.object(query, 'CONFIG', c), warnings.catch_warnings(record=True):
                result = query._load_query_dataframe(path)
            self.assertEqual(['Economics', 'faculty of Science', 'Unmatched organisations'], result['org_unit'].tolist())
            c.remove_section('FACULTY_ALIASES')
            with patch.object(query, 'CONFIG', c), warnings.catch_warnings(record=True):
                result = query._load_query_dataframe(path)
            self.assertEqual('TS Economics and Management', result.iloc[0]['org_unit'])

    def test_empty_names_fail_without_overwriting_output(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / 'query.csv'
            pd.DataFrame({'Name': ['', '  ', None], 'Organisational unit name': ['Faculteit A'] * 3}).to_csv(path, index=False)
            output = Path(temp) / 'out.txt'; output.write_text('existing queries')
            with self.assertRaisesRegex(ValueError, 'usable'):
                query.build_queries(path, output)
            self.assertEqual('existing queries', output.read_text())


if __name__ == '__main__':
    unittest.main()
