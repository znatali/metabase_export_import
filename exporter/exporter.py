import json
import csv

from metabase_api_interface import MetabaseApiInterface


class MetabaseExporter(MetabaseApiInterface):

    def export_fields_to_csv(self, database_name, filename):
        export = self.export_fields(database_name)
        if not export:
            return
        with open(filename, 'w', newline='') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',')
            need_header = True
            for row in export:
                if need_header:
                    my_writer.writerow(row.keys())
                    need_header = False
                my_writer.writerow(row.values())

    def export_cards_to_json(self, database_name, filename, collection_name):
        export = self.get_cards(database_name)
        sorted_export = []
        for card_row in export:
            if card_row['collection']['name'] == collection_name:
                sorted_export.append(card_row)

        with open(filename, 'w', newline='') as jsonfile:
            jsonfile.write(json.dumps(self.convert_ids2names(database_name, sorted_export, None)))

    def export_metrics_to_json(self, database_name, filename):
        export = self.get_metrics(database_name)
        with open(filename, 'w', newline = '') as jsonfile:
            jsonfile.write(json.dumps(self.convert_ids2names(database_name, export, None)))

    def export_dashboards_to_json(self, database_name, filename):
        export = self.get_dashboards(database_name)
        with open(filename, 'w', newline = '') as jsonfile:
            jsonfile.write(json.dumps(self.convert_ids2names(database_name, export, None)))

    def export_fields(self, database_name):
        self.database_export = self.get_database(database_name, True)
        result = []
        if not self.database_export.get('tables'):
            return None
        for table in self.database_export['tables']:
            table_name = table['name']
            for field in table['fields']:
                field_id = field['fk_target_field_id']
                [fk_table, fk_field] = self.field_id2tablenameandfieldname(database_name, field_id)
                if not field['semantic_type']:
                    field['semantic_type'] = ''
                if not field['custom_position']:
                    field['custom_position'] = ''
                result.append({
                                'table_name': table_name, 'field_name': field['name'], 'description': field['description'],
                                'semantic_type': field['semantic_type'],
                                'foreign_table': fk_table, 'foreign_field': fk_field,
                                'visibility_type': field['visibility_type'], 'has_field_values': field['has_field_values'],
                                'custom_position': field['custom_position'], 'effective_type': field['effective_type'],
                                'base_type': field['base_type'], 'database_type': field['database_type'], 'field_id': field['id']
                              })
        return result


