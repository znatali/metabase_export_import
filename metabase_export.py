import argparse
from exporter.exporter import MetabaseExporter
from config import FILENAMES_MAP

parser = argparse.ArgumentParser()
parser.add_argument('--url', dest='url', required=True)
parser.add_argument('--username', dest='username', required=True)
parser.add_argument('--password', dest='password', required=True)
parser.add_argument('--db', dest='db', required=True)
parser.add_argument('--collection', dest='collection', required=True)
options = vars(parser.parse_args())

metabase_exporter = MetabaseExporter(
    apiurl=options['url'],
    username=options['username'],
    password=options['password'],
)

metabase_exporter.export_fields_to_csv(options['db'], FILENAMES_MAP['fields'])
metabase_exporter.export_cards_to_json(options['db'], FILENAMES_MAP['cards'], options['collection'])
metabase_exporter.export_dashboards_to_json(options['db'], FILENAMES_MAP['dashboards'], options['collection'])
metabase_exporter.export_metrics_to_json(options['db'], FILENAMES_MAP['metrics'])
