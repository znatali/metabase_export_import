import requests
import json


class MetabaseApiInterface:
    def __init__(self, apiurl, username, password, debug=False):
        self.apiurl = apiurl
        self.username = username
        self.password = password
        self.debug = debug
        
        self.metabase_session = None
        self.database_export = None
        self.cards_export = None
        self.metrics_export = None
        self.dashboards_name2id = None
        self.cards_name2id = {}
        self.collections_name2id = {}
        self.metrics_name2id = {}

        self.collection_id = None

        self.map_old_id_dash_names = {}
        
    def query(self, method, query_name, json_data = None):
        json_str = None
        if json_data is not None:
            json_str = json.dumps(json_data)
        
        headers = { "Content-Type": "application/json;charset=utf-8" }
        
        if self.metabase_session is not None:
            headers["X-Metabase-Session"] = self.metabase_session
        
        query_url = self.apiurl+query_name
        
        if (self.debug):
            print(method+' '+query_url)
            print(headers)
            print(json_str)
        
        if method == 'POST':
            r = requests.post(
                                query_url ,
                                data = json_str,
                                headers= headers
                             )
        elif method == 'GET':
            r = requests.get(
                                query_url,
                                data = json_str,
                                headers= headers
                            )
        elif method == 'PUT':
            r = requests.put(
                                query_url,
                                data = json_str,
                                headers= headers
                            )
        elif method == 'DELETE':
            r = requests.delete(
                                query_url,
                                data = json_str,
                                headers= headers
                                )
        else:
            raise ConnectionError('unkown method: '+method+' (GET,POST,DELETE allowed)')
        
        if self.debug:
            print(r.text)
        
        try:
            query_response = r.json()
            if query_response.get('errors'):
                raise ConnectionError(query_response)
            if query_response.get('_status') == 500:
                raise ConnectionError(query_response)
            if query_response.get('via'):
                raise ConnectionError(query_response)
        except AttributeError:
            if r.text.find('endpoint') > -1:
                raise ConnectionError(query_url+" ("+method+"): "+r.text)
            return query_response
        except ValueError:
            if (r.text):
                raise ConnectionError(r.text)
            return {}
        
        return query_response

    def create_session(self):
        json_response = self.query('POST', 'session', {"username": self.username, "password": self.password})
        try:
            self.metabase_session = json_response["id"]
        except KeyError:
            if json_response.get('errors'):
                raise ConnectionError(json_response['errors'])
            raise ConnectionError("ERROR: enable to connect: " + str(json_response))

    def create_session_if_needed(self):
        if self.metabase_session:
            return;
        self.create_session()

    def get_databases(self, full_info=False):
        self.create_session_if_needed()
        url = 'database'
        if (full_info):
            url += '?include=tables'
        databases = self.query('GET', url)
        if isinstance(databases, list):
            return databases
        return databases['data']

    def create_database(self, name, engine, details, is_full_sync=True, is_on_demand=False, auto_run_queries=True):
        self.create_session_if_needed()
        data = self.get_database(name)
        if data:
            return data
        return self.query('POST', 'database', {"name": name, 'engine': engine, "details": details, "is_full_sync": is_full_sync, "is_on_demand": is_on_demand, "auto_run_queries": auto_run_queries})

    def get_database(self, name, full_info=False):
        name2database = {}
        for database in self.get_databases():
            name2database[database['name']] = database
        data = name2database.get(name)
        if not data:
            return {}
        if not full_info:
            return data
        return self.query('GET', 'database/'+str(data['id'])+'?include=tables.fields')

    def delete_database(self, name):
        self.create_session_if_needed()
        data = self.get_database(name)
        if not data:
            return
        return self.query('DELETE', 'database/'+str(data['id']), {'id': data['id']})

    def get_all_tables(self):
        self.create_session_if_needed()
        return self.query('GET', 'table')

    def get_tables_of_database(self, database_name):
        self.create_session_if_needed()
        result = self.get_database(database_name, True)
        try:
            return result['tables']
        except KeyError:
            return {}

    def get_table(self, database_name, table_name):
        for t in self.get_tables_of_database(database_name):
            if t['name'] == table_name:
                return t
        table = {}

    def get_field(self, database_name, table_name, field_name):
        table = self.get_table(database_name, table_name)
        try:
            for f in table['fields']:
                if f['name'] == field_name:
                    return f
        except TypeError:
            return {}
        return {}

    def delete_session(self):
        self.query('DELETE', 'session', {'metabase-session-id': self.metabase_session})
        self.metabase_session = None

    def field_id2tablenameandfieldname(self, database_name, field_id):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not field_id:
            return ['', '']
        for table in self.database_export['tables']:
            for field in table['fields']:
                if field['id'] == field_id:
                    return [table['name'], field['name']]
        return ['', '']

    def table_id2name(self, database_name, table_id):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_id:
            return ['', '']
        for table in self.database_export['tables']:
            if table['id'] == table_id:
                return table['name']
        return ''

    def card_id2name(self, database_name, card_id):
        if self.cards_export is None:
            self.cards_export = self.get_cards(database_name)
        for card in self.cards_export:
            if card['id'] == card_id:
                return card['name']
        return ''

    def metric_id2name(self, database_name, metric_id):
        if self.metrics_export is None:
            self.metrics_export = self.get_metrics(database_name)
        for metric in self.metrics_export:
            if metric['id'] == metric_id:
                return metric['name']
        return ''

    def get_api_field(self, database_name, table_name, field_name):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_name or not field_name:
            return None
        if not self.database_export.get('tables'):
            return None
        for table in self.database_export['tables']:
            if table['name'] == table_name:
                for field in table['fields']:
                    if field['name'] == field_name:
                        return field
        return None

    def table_name2id(self, database_name, table_name):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_name:
            return None
        if not self.database_export.get('tables'):
            return None
        for table in self.database_export['tables']:
            if table['name'] == table_name:
                return table['id']
        return None

    def update_fields(self, database_name, fields):
        output = []
        for f in fields:
            output.append(output.append(self.update_field(database_name, f)))
        return output

    def update_field(self, database_name, field):
        field_from_api = self.get_api_field(database_name, field['table_name'], field['field_name'])
        if not field_from_api:
            return None
        fk = self.get_api_field(database_name, field['foreign_table'], field['foreign_field'])
        field.pop('foreign_table')
        field.pop('foreign_field')
        data = {'id': str(field_from_api['id'])}
        for k in field.keys():
            if field[k]:
                data[k] = field[k]
            else:
                data[k] = None
        if fk:
            data['fk_target_field_id'] = fk['id']
        return self.query('PUT', 'field/'+data['id'], data)

    def database_name2id(self, database_name):
        self.create_session_if_needed()
        data = self.query('GET', 'database')
        if isinstance(data, list):
            newdata = {}
            newdata['data'] = data
            data = newdata
        for d in data['data']:
            if d['name'] == database_name:
                return d['id']
        return None

    def get_cards(self, database_name):
        database_id = self.database_name2id(database_name)
        return self.query('GET', 'card?f=database&model_id='+str(database_id))

    def get_collections(self):
        self.create_session_if_needed()
        return self.query('GET', 'collection')

    def get_dashboard(self, dashboard_name):
        dashboard_id = self.dashboard_name2id(dashboard_name)
        return self.query('GET', 'dashboard/'+str(dashboard_id))

    def get_dashboards(self, database_name, filter_collection_id=None):
        database_id = self.database_name2id(database_name)
        dashbords_light = self.query('GET', 'dashboard')
        dashboards = []
        for d in dashbords_light:
            res = self.query('GET', 'dashboard/'+str(d['id']))
            good_db = True
            good_col = True
            for c in res['ordered_cards']:
                if c['card'].get('database_id') and c['card'].get('database_id') != database_id:
                    good_db = False
                    continue
                if filter_collection_id:
                    if c['card'].get('collection_id') and c['card'].get('collection_id') != filter_collection_id or not c['card'].get('collection_id'):
                        good_col = False
                        continue
            if not good_db:
                continue
            if not good_col:
                continue
            dashboards.append(res)
        return dashboards

    def get_metrics(self, database_name):
        database_id = self.database_name2id(database_name)
        res = self.query('GET', 'metric')
        metrics = []
        for m in res:
            if m['database_id'] == database_id:
                metrics.append(m)
        return metrics

    def dashboard_name2id(self, dashboard_name):
        if not self.dashboards_name2id:
            self.dashboards_name2id = {}
            for d in self.query('GET', 'dashboard'):
                self.dashboards_name2id[d['name']] = d['id']
        return self.dashboards_name2id.get(dashboard_name)

    def dashboard_id2name(self, database_name, dashboard_id):
        self.dashboard_name2id(database_name)
        for dname in self.dashboards_name2id.keys():
            if self.dashboards_name2id[dname] == dashboard_id:
                return dname
        return None

    def card_name2id(self, database_name, card_name):
        if not self.cards_name2id:
            for c in self.get_cards(database_name):
                self.cards_name2id[c['name']] = c['id']
        return self.cards_name2id.get(card_name)

    def collection_name2id(self, collection_name):
        if not self.collections_name2id:
            for c in self.get_collections():
                self.collections_name2id[c['name']] = c['id']
        return self.collections_name2id.get(collection_name)

    def metric_name2id(self, database_name, metric_name):
        if not self.metrics_name2id:
            for c in self.get_metrics(database_name):
                self.metrics_name2id[c['name']] = c['id']
        return self.metrics_name2id.get(metric_name)

    def collection_name2id_or_create_it(self, collection_name):
        cid = self.collection_name2id(collection_name)
        if cid:
            return cid
        result = self.create_collection(collection_name)
        if result:
            return self.collection_name2id(collection_name)

    def create_collection(self, collection_name, parent_collection_name = None, param_args = {}):  # TODO for Natali it's update collection now
        self.create_session_if_needed()
        param = param_args.copy()
        param['name'] = collection_name
        parent_id = None
        if parent_collection_name:
            parent_id = self.collection_name2id_or_create_it(parent_collection_name)
            if parent_id:
                param['parent_id'] = parent_id
        if not param.get('color'):
            param['color'] = '#509ee3'
        cid = self.collection_name2id(collection_name)
        self.collections_name2id = {}
        if cid:
            return self.query('PUT', 'collection/'+str(cid), param)

    def create_update_root_collection(self, collection_name, param_args={}):
        self.create_session_if_needed()
        all_collection = self.query('GET', 'collection/')
        for row_collection in all_collection:
            if row_collection['name'] == collection_name:
                self.collection_id = row_collection['id']
        if not self.collection_id:
            param = param_args.copy()
            param['name'] = collection_name
            param['parent_id'] = None
            param['color'] = '#509ee3'
            param['description'] = None
            param['namespace'] = None
            result = self.query('POST', 'collection/', param)
            self.collection_id = result['id']

    def convert_pcnames2id(self, database_name, collection_name, fieldname, pcnames):
        if pcnames[0] != '%':
            raise ValueError('Not a convertible value')
        sep = pcnames.find('%', 1)
        if sep == -1:
            raise ValueError('Not a convertible value')
        [new_k, names] = pcnames[1:sep], pcnames[sep+1:]
        if new_k == 'JSONCONV':
            data = self.map_names_ids(database_name, collection_name, json.loads(names))
            return [json.dumps(data), None]
        if fieldname == 'database_name':
            return [new_k, self.database_name2id(database_name)]
        if fieldname == 'collection_name':
            return [new_k, self.collection_name2id_or_create_it(collection_name)]   # TODO for Natali if None
        if fieldname == 'card_name':
            return [new_k, self.card_name2id(database_name, names)]
        if fieldname == 'dashboard_name':
            return [new_k, self.dashboard_name2id(names)]
        if fieldname == 'pseudo_table_card_name':
            card_id = self.card_name2id(database_name, names)
            if not card_id:
                raise ValueError('card_name '+names+' not found')
            return [new_k, 'card__'+str(card_id)]
        resplit = names.split('|')
        if len(resplit) == 3:
            metricid = self.metric_name2id(database_name, resplit[2])
            if metricid:
                return[new_k, metricid]
            raise ValueError('metric not found: '+resplit[2])
        if len(resplit) == 2:
            field = self.get_api_field(database_name, resplit[0], resplit[1])
            if field:
                return[new_k, field['id']]
            raise ValueError('field not found: '+resplit[0]+'/'+resplit[1])
        if len(resplit) == 1:
            table_id = self.table_name2id(database_name, resplit[0])
            return [new_k, table_id]
        raise ValueError('Unknown '+str(fieldname)+' %'+str(new_k)+'% type')

    def map_names_ids(self, database_name, collection_name, obj):
        obj_res = obj
        if isinstance(obj, list):
            if len(obj) and obj[0] in ['field', 'metric']:
                if obj[1][0] == '%':
                    [k_name, value] = self.convert_pcnames2id(database_name, collection_name, None, obj[1])
                    obj_res[1] = value
            else:
                for i in range(len(obj)):
                    obj_res[i] = self.map_names_ids(database_name, collection_name, obj[i])
        elif isinstance(obj, dict):
            obj_res = obj.copy()
            for k in obj.keys():
                if k[0] == '%':
                    try:
                        [new_k, value] = self.convert_pcnames2id(database_name, collection_name, None, k)
                        obj_res.pop(k)
                        obj_res[new_k] = self.map_names_ids(database_name, collection_name, obj[k])
                    except ValueError:
                        obj_res[k] = obj[k]
                elif k in ['field_name', 'table_name', 'database_name', 'card_name', 'pseudo_table_card_name', 'dashboard_name', 'collection_name'] and obj[k][0] == '%':
                    [new_k, value] = self.convert_pcnames2id(database_name, collection_name, k, obj[k])
                    obj_res.pop(k)
                    obj_res[new_k] = value
                else:
                    if isinstance(obj[k], dict) or isinstance(obj[k], list):
                        obj_res[k] = self.map_names_ids(database_name, collection_name, obj[k])
        return obj_res

    def convert_ids2names(self, database_name, obj, previous_key):
        obj_res = obj
        if isinstance(obj, list):
            if len(obj):
                try:
                    if obj[0] == 'field':
                        [t, f] = self.field_id2tablenameandfieldname(database_name, int(obj_res[1]))
                        obj_res[1] = '%%'+t+'|'+f
                    elif obj[0] == 'metric':
                        m = self.metric_id2name(database_name, int(obj_res[1]))
                        obj_res[1] = '%%||'+m
                    else:
                        for i in range(len(obj)):
                            obj_res[i] = self.convert_ids2names(database_name, obj[i], previous_key)
                except ValueError:
                    obj_res[1] = obj[1]
        elif isinstance(obj, dict):
            obj_res = obj.copy()
            for k in obj.keys():
                if k == 'collection':
                    obj_res.pop(k)
                    continue
                if isinstance(obj[k], dict) or isinstance(obj[k], list):
                    k_previous = previous_key
                    k2int = None
                    #Cas de clé d'un dictionnaire qui sont les id de fields
                    try:
                        k2int = int(k)
                        k_name = k
                        if k2int:
                            [t, f] = self.field_id2tablenameandfieldname(database_name, k2int)
                            k_name = '%%'+t+'|'+f
                    except ValueError:
                        k_name = k
                        k_previous = k
                    #Cas de clé du dictionnaire qui sont du json encodé
                    if not k2int:
                        try:
                            k_data = json.loads(k)
                            if k_data[0] == 'ref' and k_data[1][0] == 'field':
                                [t, f] = self.field_id2tablenameandfieldname(database_name, int(k_data[1][1]))
                                k_data[1][1] = '%%'+t+'|'+f
                                k_name = '%JSONCONV%'+json.dumps(k_data)
                            else:
                                k_name = k
                        except json.decoder.JSONDecodeError:
                            k_name = k
                            k_previous = k
                    obj_res.pop(k)
                    obj_res[k_name] = self.convert_ids2names(database_name, obj[k], k_previous)
                else:
                    if k in ['field_id'] or (k == 'id' and previous_key in ['result_metadata', 'param_fields']):
                        id = obj_res.pop(k)
                        if id:
                            [t, f] = self.field_id2tablenameandfieldname(database_name, int(id))
                            obj_res['field_name'] = '%'+k+'%'+t+'|'+f
                    elif k in ['table_id', 'source-table']:
                        id = obj_res.pop(k)
                        if id:
                            try:
                                t = self.table_id2name(database_name, int(id))
                                obj_res['table_name'] = '%'+k+'%'+t
                            except ValueError:
                                if id[0:6] == 'card__':
                                    c = self.card_id2name(database_name, int(id[6:]))
                                    obj_res['pseudo_table_card_name'] = '%'+k+'%'+c
                    elif k == 'card_id':
                        id = obj_res.pop(k)
                        if id:
                            n = self.card_id2name(database_name, int(id))
                            obj_res['card_name'] = '%'+k+'%'+n
                    elif k in ['database_id', 'database']:
                        if obj.get(k):
                            obj_res.pop(k)
                            obj_res['database_name'] = '%'+k+'%'
                    elif k == 'collection_id':
                        obj_res.pop(k)
                        obj_res['collection_name'] = '%'+k+'%'
                    elif k == 'dashboard_id':
                        id = obj_res.pop(k)
                        dashboard_id2name = self.dashboard_id2name(database_name, id)
                        if dashboard_id2name:
                            obj_res['dashboard_name'] = '%' + k + '%' + dashboard_id2name
        return obj_res

    def dashboard_delete_all_cards(self, dashboard_name):
        dash = self.get_dashboard(dashboard_name)
        res = []
        for c in dash['ordered_cards']:
            res.append(self.query('DELETE', 'dashboard/'+str(dash['id'])+'/cards?dashcardId='+str(c['id'])))
        return res

    def dashboard_import_card(self, dashboard_name, ordered_card_from_json):
        dashid = self.dashboard_name2id(dashboard_name)
        cardid = ordered_card_from_json.get('card_id')
        if cardid:
            ordered_card_from_json['cardId'] = cardid
            ordered_card_from_json.pop('card')
        return self.query('POST', 'dashboard/'+str(dashid)+'/cards', ordered_card_from_json)

    def get_users(self):
        self.create_session_if_needed()
        users = self.query('GET', 'user?status=all')
        try:
            return users['data']
        except:
            return None

    def user_email2id(self, user_email):
        for u in self.get_users():
            if u['email'] == user_email:
                return u['id']
        return None

    def create_user(self, email, password, extra = {}):
        self.create_session_if_needed()
        extra['email'] = email
        extra['password'] = password
        user_id = self.user_email2id(email)
        if (user_id):
            return self.query('PUT', 'user/'+str(user_id), extra)
        return self.query('POST', 'user', extra)

    def user_password(self, email, password):
        self.create_session_if_needed()
        data = {}
        data['email'] = email
        data['password'] = password
        user_id = self.user_email2id(email)
        if not user_id:
            raise ValueError('known user '+email)
        return self.query('PUT', 'user/'+str(user_id)+'/password', data)

    def create_group(self, group_name):
        self.create_session_if_needed()
        return self.query('POST', 'permissions/group', {'name': group_name})

    def get_groups(self):
        self.create_session_if_needed()
        groups = self.query('GET', 'permissions/group')
        try:
            return groups
        except:
            return None

    def group_name2id(self, group_name):
        for g in self.get_groups():
            if g['name'] == group_name:
                return g['id']
        return None

    def get_memberships(self):
        self.create_session_if_needed()
        return self.query('GET', 'permissions/membership')

    def membership_add(self, user_email, group_name):
        self.create_session_if_needed()
        user_id = self.user_email2id(user_email)
        group_id = self.group_name2id(group_name)
        if (not group_id):
            group = self.create_group(group_name)
            group_id = group['id']
        memberships = self.get_memberships()
        for m in memberships[str(user_id)]:
            if m['group_id'] == group_id:
                return m
        return self.query('POST', 'permissions/membership', {'group_id': group_id, 'user_id': user_id})

    def permission_get_database(self):
        self.create_session_if_needed()
        return self.query('GET', 'permissions/graph')

    def permission_set_database(self, group_name, database_name, schema_data, native_sql):
        if group_name == 'all':
            group_id = '1'
        else:
            group_id = self.group_name2id(group_name)
        if not group_id:
            raise ValueError("group "+group_name+" not found")
        database_id = self.database_name2id(database_name)
        if not database_id:
            raise ValueError("database "+database_name+" not found")
        data = self.permission_get_database()
        if not data['groups'].get(group_id):
            data['groups'][group_id] = {}
        data['groups'][group_id][database_id] = {}
        if native_sql:
            data['groups'][group_id][database_id]['native'] = 'write'
            if schema_data:
                data['groups'][group_id][database_id]['schemas'] = 'all'
            else:
                data['groups'][group_id][database_id].pop('schemas', None)
        else:
            data['groups'][group_id][database_id]['native'] = 'none'
            data['groups'][group_id][database_id]['schemas'] = 'none'
        return self.query('PUT', 'permissions/graph', data)

    def permission_get_collection(self):
        self.create_session_if_needed()
        return self.query('GET', 'collection/graph')

    def permission_set_collection(self, group_name, collection_name, right):
        if not right in ['read', 'write', 'none']:
            raise ValueError('right not read/write/none')
        if group_name == 'all':
            group_id = '1'
        else:
            group_id = self.group_name2id(group_name)
        if not group_id:
            raise ValueError("group "+group_name+" not found")
        if collection_name == 'root':
            collection_id = 'root'
        else:
            collection_id = self.collection_name2id(collection_name)
        if not collection_id:
            raise ValueError("collection "+collection_name+" not found")
        data = self.permission_get_collection()
        if not data['groups'].get(group_id):
            data['groups'][group_id] = {}
        data['groups'][group_id][collection_id] = right
        return self.query('PUT', 'collection/graph', data)
