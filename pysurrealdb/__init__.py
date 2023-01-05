import requests

from .clients.http_client import HttpClient
from .connections import Connection, get_config
from .model import Model
from .query_builder import QueryBuilder

Client = Instance = Connection # I've seen some people prefer to use the name Client/Instance instead of Connection. This is just an alias. Not to be confused with the HttpClient class, which is the interface to the surreal api.

def connect(host=None, port=None, user=None, password=None, database=None, namespace=None) -> Connection:
    # Connect to the SurrealDB server. If no host is specified, the default connection will be used. This function also sets the current connection. Unlike the connection() method, which only returns the specified connection.
    if isinstance(host, dict):
        # if host is a dict, assume it's a config object
        Connection.connections['current'] = Connection(**host)
    else:
        Connection.connections['current'] = Connection(host=host, port=port, user=user, password=password, database=database, namespace=namespace)
    return Connection.connections['current']

def connection(name=None) -> Connection:
    # Connect to the SurrealDB server. Similar to connect(), but uses a connection name from the config file.
    # If no name is specified, this returns the current active connection. If no current connection exists, return a default connection.
    if name is None and 'current' in Connection.connections:
        return Connection.connections['current']

    return Connection(name)


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