import json
import csv
import datetime

from metabase_api_interface import MetabaseApiInterface


class MetabaseImporter(MetabaseApiInterface):
    def create_update_collection(self, database_name, collection_name):
        self.create_update_root_collection(collection_name)

    def import_fields_from_csv(self, database_name, filename):
        fields = []
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                fields.append(row)
        return self.update_fields(database_name, fields)

    def import_metrics_from_json(self, database_name, filename, collection_name=None):
        res = []
        with open(filename, 'r', newline='') as jsonfile:
            jsondata = json.load(jsonfile)
            errors = None
            for metric in jsondata:
                try:
                    res.append(self.metric_import(database_name, self.map_names_ids(database_name, None, metric)))
                except ValueError as e:
                    if not errors:
                        errors = e
                    else:
                        errors = ValueError(str(errors) + " / " + str(e))
            if errors:
                raise errors
        return res

    def import_cards_from_json(self, database_name, filename, collection_name=None):
        res = []
        res_ids = []
        card_names = []
        with open(filename, 'r', newline='') as jsonfile:
            jsondata = json.load(jsonfile)
            errors = None
            for card in jsondata:
                try:
                    name = self.map_names_ids(database_name, collection_name, card)
                    card_names.append(name['name'])
                    import_result = self.card_import(database_name, name)
                    res.append(import_result)
                    res_ids.append(import_result['id'])
                except ValueError as e:
                    if not errors:
                        errors = e
                    else:
                        errors = ValueError(str(errors) + " / " + str(e))
            if errors:
                raise errors
        if collection_name:
            params = {'card_ids': res_ids, 'collection_id': self.collection_id}
            if self.collection_id:
                res = self.query('POST', 'card/collections', params)
                all_cards = self.get_cards(database_name)
                for cur_card in all_cards:
                    if cur_card['name'] not in card_names:
                        self.query('DELETE', f"card/{cur_card['id']}")
        return res

    def import_dashboards_from_json(self, database_name, filename, collection_name=None):
        res = [[], [], []]
        with open(filename, 'r', newline='') as jsonfile:
            jsondata = self.map_names_ids(database_name, collection_name, json.load(jsonfile))
            dash_names = []

            for dash in jsondata:
                self.map_old_id_dash_names.update({dash['id']: dash['name']})

            for dash in jsondata:
                dash_names.append(dash['name'])
                res[0].append(self.dashboard_import(dash))
                self.dashboard_delete_all_cards(dash['name'])
                for ocard in dash['ordered_cards']:
                    res[1].append(self.dashboard_import_card(dash['name'], ocard))
            dashboards = self.query('GET', 'dashboard')
            for dashboard_row in dashboards:
                if dashboard_row['name'] not in dash_names:
                    self.query('DELETE', f"dashboard/{dashboard_row['id']}")
        return res

    def dashboard_import(self, dash_from_json):
        dashid = self.dashboard_name2id(dash_from_json['name'])
        dash_from_json['collection_id'] = self.collection_id
        for order_card in dash_from_json['ordered_cards']:
            if 'visualization_settings' in order_card.keys():
                if 'column_settings' in order_card['visualization_settings']:
                    for key, setting in order_card['visualization_settings']['column_settings'].items():
                        if isinstance(setting, dict):
                            if 'click_behavior' in setting.keys():
                                if setting['click_behavior']['linkType'] == 'dashboard':
                                    dash_new_id = self._find_id_dashboard_by_old_id(setting['click_behavior']['targetId'])
                                    if dash_new_id:
                                        setting['click_behavior']['targetId'] = dash_new_id

        if dashid:
            return self.query('PUT', 'dashboard/'+str(dashid), dash_from_json)
        self.dashboards_name2id = None
        return self.query('POST', 'dashboard', dash_from_json)

    def _find_id_dashboard_by_old_id(self, old_id):
        if old_id in self.map_old_id_dash_names:
            name = self.map_old_id_dash_names[old_id]
            if name:
                return self.dashboard_name2id(name)
        return None

    def card_import(self, database_name, card_from_json):
        if not card_from_json.get('description'):
            card_from_json['description'] = None
        cardid = self.card_name2id(database_name, card_from_json['name'])
        if cardid:
            result = self.query('PUT', 'card/'+str(cardid), card_from_json)
            return result
        self.cards_name2id = {}
        result = self.query('POST', 'card', card_from_json)
        return result

    def metric_import(self, database_name, metric_from_json):
        metricid = self.metric_name2id(database_name, metric_from_json['name'])
        metric_from_json['revision_message'] = "Import du "+datetime.datetime.now().isoformat()
        if metricid:
            return self.query('PUT', 'metric/'+str(metricid), metric_from_json)
        self.metrics_name2id = {}
        return self.query('POST', 'metric', metric_from_json)


