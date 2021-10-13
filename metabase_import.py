import argparse
from importer.importer import MetabaseImporter
from config import FILENAMES_MAP

parser = argparse.ArgumentParser()
parser.add_argument('--url', dest='url', required=True)
parser.add_argument('--username', dest='username', required=True)
parser.add_argument('--password', dest='password', required=True)
parser.add_argument('--db', dest='db', required=True)
parser.add_argument('--collection', dest='collection', required=True)
parser.add_argument('--files_path', dest='path', required=True)
options = vars(parser.parse_args())

metabase_importer = MetabaseImporter(
    apiurl=options['url'],
    username=options['username'],
    password=options['password'],
)

metabase_importer.import_fields_from_csv(options['db'], f"{options['path']}/{FILENAMES_MAP['fields']}")
metabase_importer.import_metrics_from_json(options['db'], f"{options['path']}/{FILENAMES_MAP['metrics']}")
metabase_importer.import_cards_from_json(options['db'], f"{options['path']}/{FILENAMES_MAP['cards']}", options['collection'])
metabase_importer.import_dashboards_from_json(options['db'], f"{options['path']}/{FILENAMES_MAP['dashboards']}")
