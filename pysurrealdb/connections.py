from .clients.http_client import HttpClient
from .cursor import Cursor
from .query_builder import QueryBuilder


class Connection(HttpClient):
    """
    This class is used to emulate a pymysql connection object. This allows the use of libraries like pandas. 
    """
    connections = {}
    client = None
    _cursor = None
    def __init__(self, client=None, **kwargs):
        """
            Cache connections by name. If a connection is already cached, return that connection.
        """
        if isinstance(client, str):
            if client in self.connections:
                return Connection.connections[client]
            else:
                connection_info = get_config(client)
                Connection.connections[client] = Connection(**connection_info)
                self = Connection.connections[client]
        if not client and not kwargs:
            # try to get a default connection
            if 'default' in Connection.connections:
                return Connection.connections['default']
            default_connection = get_config('default')
            self.client = HttpClient(**default_connection)
            Connection.connections['default'] = self
        elif not client:
            self.client = HttpClient(**kwargs)
        else:
            self.client = client

    def cursor(self) -> Cursor:
        if not self._cursor:
            self._cursor = Cursor(self.client)
        return self._cursor

    def commit(self):
        pass

    # allow entry into query builder when they call specific methods. Includes table(), select(), where().
    def table(self, table) -> QueryBuilder:
        return QueryBuilder(self.client).table(table)

    def select(self, *args) -> QueryBuilder:
        return QueryBuilder(self.client).select(*args)

    def where(self, *args) -> QueryBuilder:
        return QueryBuilder(self.client).where(*args)
    # any other methods should just be passed to the client
    def __getattr__(self, name):
        return getattr(self.client, name)

    
    
def get_config(name) -> dict:
    # check config file for connection details. It should be called 'pysurrealdb.json'
    import json
    import os
    from pathlib import Path
    # first check if an env variable is set for the config file
    config_file = os.getenv('PYSURREALDB_CONFIG', 'code/mypysurreal/pysurrealdb.json')
    # use the working directory to find the config file
    if not config_file:
        config_file = os.getcwd() + '/pysurrealdb.json'
    config_file = Path(config_file)
    if not config_file.exists():
        raise Exception('Attempt to insantiate a connection by name, but no config file found.', name)
    with open(config_file, 'r') as f:
        config = json.load(f)
    if 'connections' not in config or name not in config['connections']:
        raise Exception('Attempt to insantiate a connection by name, but no config found with that name.', name)
    return config['connections'][name]

    

    
    
