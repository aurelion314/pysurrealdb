from .clients.http_client import HttpClient
from .cursor import Cursor
from .query_builder import QueryBuilder


class Connection(HttpClient):
    """
    This is the main connection object used to create and cache connections.
    
    It includes methods to emulate a pymysql connection object. This allows the use of libraries like pandas. 
    """
    connections = {}
    client = None
    _cursor = None
    def __init__(self, client=None, **kwargs):
        """
            Attempt to build a connection. If a client is passed, use that. Otherwise, try to get a connection from the config file. If no config file is found, use localhost defaults.
            Cache connections by name. If a connection is already cached, return that connection.
        """
        if isinstance(client, str):
            if client in self.connections:
                return Connection.connections[client]
            else:
                connection_info = get_connection_config(client)
                self.client = HttpClient(**connection_info)
                Connection.connections[client] = self
        if not client and not kwargs:
            # try to get a default connection
            if 'default' in Connection.connections:
                return Connection.connections['default']
            try: 
                default_connection = get_connection_config('default')
            except:
                default_connection = None
            if default_connection:
                self.client = HttpClient(**default_connection)
                Connection.connections['default'] = self
            else:
                # Last attempt, try localhost default info.
                self.client = HttpClient()
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

    # allow entry into query builder when they call specific methods.
    def table(self, table) -> QueryBuilder:
        return QueryBuilder(self.client).table(table)

    # any other methods should just be passed to the client
    def __getattr__(self, name):
        return getattr(self.client, name)

    
    
def get_config() -> dict:
    # check config file for connection details. It should be called 'pysurrealdb.json'
    import json
    import os
    from pathlib import Path
    # first check if an env variable is set for the config file
    config_file = os.getenv('PYSURREALDB_CONFIG')
    # otherwise use the working directory to find the config file
    if not config_file:
        config_file = os.getcwd() + '/pysurrealdb.json'
    config_file = Path(config_file)
    if not config_file.exists():
        raise Exception('No pysurrealdb config file found.')
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config


def get_connection_config(name) -> dict:
    config = get_config()
    if 'connections' not in config or name not in config['connections']:
        raise Exception('Attempt to insantiate a connection by name, but no config found with that name.', name)
    return config['connections'][name]

    

    
    
