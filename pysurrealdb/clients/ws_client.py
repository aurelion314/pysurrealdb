import json
import websocket
import time
import random

from ..config import config
from ..err import QueryError, SurrealDBError
from ..utils import verify_table_and_id

class WSClient:
    """
    Representation of a socket connection with a surrealdb server.

    You should not need to use this class directly. Instead, use the Connection class via the connect() method.
    """
    sock = None

    def __init__(self, host=None, port=None, user=None, password=None, database=None, namespace=None, **kwargs):
        self.host = host
        self.port = port
        self._user = user
        self._password = password
        self._database = database
        self._namespace = namespace
        self._write_timeout = kwargs.get('write_timeout', 5)

        if not self.host:
            self.host = 'ws://localhost'
            if config.warnings: print('SurrealDB: No host specified. Using "ws://localhost"')
        if not self.port:
            self.port = '8000'
            if config.warnings: print('SurrealDB: No port specified. Using "8000"')
        if not self._database:
            self._database = 'main'
            if config.warnings: print('SurrealDB: No database specified. Using "main"')
        if not self._namespace:
            self._namespace = 'main'
            if config.warnings: print('SurrealDB: No namespace specified. Using "main"')
        
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        #Include headers and auth
        url = self._get_url()
        self.sock = websocket.create_connection(url)

        if self._namespace and self._database:
            self.use(self._namespace, self._database)

        if self._user and self._password:
            self.login(self._user, self._password)
        else:
            if config.warnings: print('SurrealDB: No user or password specified. Use .login(user, pass) to login.')
        print('SurrealDB Connected')

    def close(self):
        self.sock.close()

    def _get_url(self):
        # convert to ws or wss
        if self.host.startswith('http'):
            self.host = self.host.replace('http', 'ws')
        elif self.host.startswith('https'):
            self.host = self.host.replace('https', 'wss')
        if not self.host.startswith('ws'):
            self.host = f'ws://{self.host}'
        # clear final slash
        if self.host.endswith('/'):
            self.host = self.host[:-1]
        # add port and rpc
        return f'{self.host}:{self.port}/rpc'


    def _send_receive(self, method, *params):
        self._send(method, *params)
        r = self._recv()
        return r

    def _send(self, method, *params):
        if not self.sock:
            self.connect()
        self.sock.settimeout(self._write_timeout)
        data = {
            'id': self._generate_id(),
            'method': method,
            'params': params,
        }
        # print('sending', data)
        self.sock.send(json.dumps(data, ensure_ascii=False, default=str))

    def _generate_id(self):
        # generate a unique id string
        return f'{time.time()}-{random.randint(0, 1000000)}'

    def _recv(self):
        # receive any data from the socket
        r = self.sock.recv()
        r = json.loads(r)
        if 'error' in r:
            raise Exception(r['error'])
        return r['result']


    def _split_table(self, table, id=None):
        id = id or table.split(':')[-1] if ':' in table else None
        table = table.split(':')[0]
        return table, id
        

    def use(self, namespace, database):
        self._send('use', namespace, database)
        self._namespace = namespace
        self._database = database
        return self._recv()

    def login(self, user, password):
        return self._send_receive('signin', {'user': user, 'pass': password})

    def ping(self):
        return self._send_receive('ping')

    def query(self, query):
        """Run a query on the current database"""
        r = self._send_receive('query', query)
        if len(r) > 1:
            results = []
            for row in r:
                if row['status'] != 'OK':
                    raise QueryError("Query failed.", row['result'])
                results.append(row['result'])
            return results
                
        if r[0]['status'] != 'OK':
            raise QueryError("Query failed.", r[0])
        return r[0]['result']

    def create(self, table, data=None):
        """Create one or many records in a SurrealDB table."""
        if isinstance(data, list):
            return self.create_many(table, data)
        return self.create_one(table, data)

    def create_many(self, table, data):
        """Create many records in a SurrealDB table."""
        results = []
        for row in data:
            r = self.create_one(table, row)
            results.append(r[0])
        return results

    def create_one(self, table, data):
        """Create a new record in the specified table"""
        table, id = verify_table_and_id(table, data.get('id'))
        if id:
            result = self._send_receive('create', f'{table}:{id}', data)
        else:
            result = self._send_receive('create', table, data)
        if not result:
            raise Exception('Problem creating record - DB returned no response.', f'{table}:{id}')
        return result


    def insert(self, table, data):
        """Create a new record in the specified table"""
        return self.create(table, data)

    def update(self, table, data=None):
        """Update a record in the specified table"""
        if not data:
            data = table
            table = table['id']
        table, id = verify_table_and_id(table, data.get('id'))
        if not id:
            raise Exception('Cannot update a record without an id')
        return self._send_receive('update', f'{table}:{id}', data)

    def get(self, table, id=None):
        """Get a record from the specified table"""
        table, id = verify_table_and_id(table, id)
        if not id:
            return self._send_receive('select', table)
        result = self._send_receive('select', table + ':' + id)
        if not result:
            return None
        return result[0]

    def delete(self, table, id=None):
        """Delete a record from the specified table"""
        table, id = verify_table_and_id(table, id)
        if not id:
            raise Exception('Cannot delete a record without an id')
        return self._send_receive('delete', table + ':' + id)

    def drop(self, table):
        """Drop a table"""
        return self._send_receive('delete', table)
