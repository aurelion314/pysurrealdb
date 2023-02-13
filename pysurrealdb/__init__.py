import requests

from .clients.http_client import HttpClient
from .connections import Connection
from .model import Model
from .query_builder import QueryBuilder
from .config import config

Client = Instance = Connection # I've seen some people prefer to use the name Client/Instance instead of Connection. This is just an alias. Not to be confused with the HttpClient class, which is the interface to the surreal api.

def connect(host=None, port=None, user=None, password=None, database=None, namespace=None, client=None) -> Connection:
    """ 
    Connect to the SurrealDB server. 

    Args:
        client: Specify client or client type. Client type can be 'http' or 'websocket'. If passing a client object, it must be an instance of HttpClient or WSClient.
        host: The host of the server. defaults to localhost.
        port: The port of the server. defaults to 8000.
        user: The user to login with.
        password: The password to login with.
        database: The database to use. defaults to 'main'.
        namespace: The namespace to use. defaults to 'main'.
    
    This function caches the current connection. 
    """

    if isinstance(host, dict):
        # if host is a dict, assume it's a config object
        Connection.connections['current'] = Connection(**host)
    else:
        Connection.connections['current'] = Connection(host=host, port=port, user=user, password=password, database=database, namespace=namespace, client=client)
    return Connection.connections['current']

def connection(name:str = None) -> Connection:
    """
    Connect to the SurrealDB server. Similar to connect(), but uses a connection name from the config file.

    Args:
        name: The name of the connection. This is used to look up the connection in the config file. If not specified, the most recent connection is returned. If no current connection exists, a default connection is attempted.
    """
    conn = None
    # If no name is specified, return the most recent connection
    if name is None and 'current' in Connection.connections:
        conn = Connection.connections['current']

    elif name:
        # have we already created a connection with this name?
        if name in Connection.connections:
            conn = Connection.connections[name]
        elif name in config.connections: 
            # if not, create a new connection
            conn = Connection(**config.connections[name])
            Connection.connections[name] = conn
        else:
            raise Exception(f'SurrealDB: Connection "{name}" not found.')

    # check for a default config
    elif config.connections.get('default'):
        conn = Connection(**config.connections['default'])
        Connection.connections['default'] = conn

    if not conn:
        # try to connect to localhost
        conn = Connection()

    # cache the connection
    Connection.connections['current'] = conn
    
    return conn


## These are utility functions to allow the use of query methods without having to create a connection object. Note: They only work if a connection has already been created, or a default connection is available.
def table(name: str) -> QueryBuilder:
    # Return a query builder for the specified table.
    return QueryBuilder(connection()).table(name)

def select(*args, **kwargs):
    return connection().select(*args, **kwargs)

def query(*args, **kwargs):
    return connection().query(*args, **kwargs)

def get(*args, **kwargs):
    return connection().get(*args, **kwargs)

def insert(*args, **kwargs):
    return connection().insert(*args, **kwargs)

def create(*args, **kwargs):
    return connection().create(*args, **kwargs)

def update(*args, **kwargs):
    return connection().update(*args, **kwargs)

def upsert(*args, **kwargs):
    return connection().upsert(*args, **kwargs)

def delete(*args, **kwargs):
    return connection().delete(*args, **kwargs)

def drop(*args, **kwargs):
    return connection().drop(*args, **kwargs)

def relate(*args, **kwargs):
    return connection().relate(*args, **kwargs)